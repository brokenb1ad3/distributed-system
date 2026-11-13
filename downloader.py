"""Tải song song từ nhiều peer: rarest-first, timeout, retry có giới hạn,
tự chuyển nguồn khi peer chết, loại chunk sai hash.

Xem docs/PROTOCOL.md mục 5.

Mô hình: MỘT event loop, nhiều coroutine. Bộ lập lịch (run) chọn cặp (chunk, nguồn),
mỗi cặp là một task `_attempt`. Task thất bại thì chunk quay lại hàng chờ và
được giao cho NGUỒN KHÁC ở vòng lập lịch kế tiếp.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import random
import time
from collections import Counter, defaultdict
from pathlib import Path

from common import protocol as P
from common.log import event
from peer.storage import IntegrityError
from peer.tracker_client import TrackerError


class FetchError(Exception):
    """Một lần xin chunk thất bại. code: TIMEOUT | CONNECT | PROTOCOL | BUSY | NOT_FOUND | BAD_HASH"""

    def __init__(self, code: str, detail: str = ""):
        super().__init__(f"{code} {detail}".strip())
        self.code, self.detail = code, detail


class DownloadStalled(Exception):
    """Quá lâu không tải thêm được chunk nào (không còn nguồn)."""


def pick_rarest(wanted: set[int], usable: list[dict], free: list[dict]) -> int | None:
    """Rarest-first: trong các chunk còn thiếu mà một peer đang rảnh có thể cung cấp,
    chọn chunk có ÍT peer sở hữu nhất (tính trên mọi peer dùng được). Hòa thì chọn ngẫu nhiên."""
    counts = Counter(i for p in usable for i in p["chunks"] if i in wanted)
    candidates = {i for p in free for i in p["chunks"] if i in wanted}
    if not candidates:
        return None
    rarest = min(counts[i] for i in candidates)
    return random.choice(sorted(i for i in candidates if counts[i] == rarest))


class Downloader:
    def __init__(self, peer_id: str, store, tracker, log: logging.Logger, *,
                 max_parallel: int = 4, per_peer_limit: int = 2, chunk_timeout: float = 5.0,
                 max_retries: int = 3, suspect_seconds: float = 30.0, dead_cooldown: float = 5.0,
                 chunk_backoff: float = 2.0, refresh_interval: float = 1.0, stall_timeout: float = 60.0):
        self.peer_id = peer_id
        self.store = store                    # ChunkStore
        self.tracker = tracker                # TrackerClient
        self.log = log
        self.meta = store.meta
        self.file_id = store.file_id
        self.max_parallel = max_parallel      # giới hạn tổng số chunk tải cùng lúc
        self.per_peer_limit = per_peer_limit  # giới hạn số request đồng thời tới MỘT peer
        self.chunk_timeout = chunk_timeout
        self.max_retries = max_retries
        self.suspect_seconds = suspect_seconds
        self.dead_cooldown = dead_cooldown
        self.chunk_backoff = chunk_backoff
        self.refresh_interval = refresh_interval
        self.stall_timeout = stall_timeout

        self.peers: dict[str, dict] = {}                      # peer_id -> {host, port, chunks:set}
        self.active: Counter[str] = Counter()                 # số request đang chạy tới mỗi peer
        self.cooldown: dict[str, float] = {}                  # peer_id -> thời điểm được dùng lại
        self.in_flight: set[int] = set()                      # chunk đang tải (không tải trùng)
        self.attempts: Counter[int] = Counter()               # số lần thử của mỗi chunk
        self.failed_from: dict[int, set[str]] = defaultdict(set)
        self.chunk_hold: dict[int, float] = {}                # chunk tạm hoãn sau khi hết lượt retry
        self.stats = {"by_source": Counter(), "retries": 0, "rejected": 0, "timeouts": 0,
                      "source_failures": 0, "bytes": 0, "elapsed": 0.0}
        self._last_wait_log = 0.0
        self._tracker_down = False

    # ------------------------------------------------------------------ mạng
    async def fetch_chunk(self, peer: dict, index: int) -> bytes:
        """Mở TCP tới peer, gửi GET_CHUNK, đợi CHUNK có cùng msg_id. Toàn bộ nằm trong CHUNK_TIMEOUT."""
        mid = P.new_msg_id()
        writer: asyncio.StreamWriter | None = None

        async def exchange() -> tuple[dict, bytes]:
            nonlocal writer
            reader, writer = await asyncio.open_connection(peer["host"], peer["port"])
            await P.send_frame(writer, {"type": P.GET_CHUNK, "msg_id": mid, "from": self.peer_id,
                                        "file_id": self.file_id, "index": index})
            return await P.read_frame(reader)

        try:
            header, payload = await asyncio.wait_for(exchange(), self.chunk_timeout)
        except asyncio.TimeoutError:
            raise FetchError("TIMEOUT", f"quá {self.chunk_timeout}s") from None
        except P.ProtocolError as e:
            raise FetchError("PROTOCOL", str(e)) from None
        except (asyncio.IncompleteReadError, OSError) as e:
            raise FetchError("CONNECT", type(e).__name__) from None
        finally:
            if writer is not None:
                writer.close()
                with contextlib.suppress(Exception):
                    await writer.wait_closed()

        if header["type"] == P.ERROR:
            raise FetchError(str(header.get("code", "ERROR")), str(header.get("reason", "")))
        # Chống message lạc/trùng: response phải khớp đúng request vừa gửi.
        if header["type"] != P.CHUNK or header.get("msg_id") != mid \
                or header.get("index") != index or header.get("file_id") != self.file_id:
            raise FetchError("PROTOCOL", "response không khớp request")
        return payload

    async def _refresh_peers(self) -> None:
        try:
            fresh = await self.tracker.get_peers(self.file_id)
        except TrackerError as e:
            if not self._tracker_down:
                event(self.log, "TRACKER_UNREACHABLE", f"{e} | tiếp tục với {len(self.peers)} nguồn đã biết",
                      logging.WARNING)
            self._tracker_down = True
            return
        self._tracker_down = False
        new = {p["peer_id"]: {"peer_id": p["peer_id"], "host": p["host"], "port": p["port"],
                              "chunks": set(p["chunks"])} for p in fresh}
        for pid in new.keys() - self.peers.keys():
            event(self.log, "SOURCE_DISCOVERED", f"peer={pid} addr={new[pid]['host']}:{new[pid]['port']} "
                                                 f"have={len(new[pid]['chunks'])}/{self.meta['num_chunks']}")
        for pid in self.peers.keys() - new.keys():
            event(self.log, "SOURCE_GONE", f"peer={pid} (tracker đã xóa khỏi danh sách)")
        self.peers = new

    # ------------------------------------------------------------ lập lịch
    def _pick(self) -> tuple[int, dict] | None:
        now = time.monotonic()
        usable = [p for pid, p in self.peers.items() if self.cooldown.get(pid, 0) <= now]
        free = [p for p in usable if self.active[p["peer_id"]] < self.per_peer_limit]
        wanted = {i for i in self.store.missing() - self.in_flight if self.chunk_hold.get(i, 0) <= now}
        index = pick_rarest(wanted, usable, free)
        if index is None:
            return None
        sources = [p for p in free if index in p["chunks"]]
        # Ưu tiên nguồn CHƯA từng lỗi với chunk này, rồi tới nguồn đang ít tải nhất.
        fresh = [p for p in sources if p["peer_id"] not in self.failed_from[index]] or sources
        least = min(self.active[p["peer_id"]] for p in fresh)
        return index, random.choice([p for p in fresh if self.active[p["peer_id"]] == least])

    async def _attempt(self, index: int, peer: dict) -> bool:
        pid = peer["peer_id"]
        total = self.meta["num_chunks"]
        try:
            data = await self.fetch_chunk(peer, index)
            try:
                stored = await self.store.put(index, data)
            except IntegrityError:
                raise FetchError("BAD_HASH", "sha256 không khớp metadata") from None
            if stored:
                self.stats["by_source"][pid] += 1
                self.stats["bytes"] += len(data)
                event(self.log, "CHUNK_RECEIVED", f"file={self.file_id} idx={index} from={pid} bytes={len(data)} "
                                                  f"hash=ok progress={len(self.store.have)}/{total}")
            else:
                event(self.log, "CHUNK_DUPLICATE", f"file={self.file_id} idx={index} from={pid} -> bỏ qua")
            return True
        except FetchError as e:
            now = time.monotonic()
            if e.code == "BUSY":                       # nguồn quá tải: nghỉ ngắn, không tính là lỗi
                self.cooldown[pid] = now + 0.3
                return False
            if e.code == "BAD_HASH":
                self.stats["rejected"] += 1
                self.cooldown[pid] = now + self.suspect_seconds
                event(self.log, "CHUNK_REJECTED", f"file={self.file_id} idx={index} from={pid} hash=SAI -> loại chunk, "
                                                  f"nghi ngờ {pid} trong {self.suspect_seconds:.0f}s", logging.WARNING)
            elif e.code == "NOT_FOUND":
                peer["chunks"].discard(index)
            else:                                      # TIMEOUT / CONNECT / PROTOCOL: coi như nguồn hỏng
                self.stats["source_failures"] += 1
                if e.code == "TIMEOUT":
                    self.stats["timeouts"] += 1
                self.cooldown[pid] = now + self.dead_cooldown
                event(self.log, "SOURCE_FAILED", f"peer={pid} idx={index} lỗi={e.code} {e.detail} -> "
                                                 f"tạm ngưng {pid} {self.dead_cooldown:.0f}s, chuyển nguồn khác",
                      logging.WARNING)
            self.attempts[index] += 1
            self.failed_from[index].add(pid)
            self.stats["retries"] += 1
            if self.attempts[index] >= self.max_retries:
                self.chunk_hold[index] = now + self.chunk_backoff
                self.attempts[index] = 0
                self.failed_from[index].clear()
                event(self.log, "CHUNK_FAILED", f"file={self.file_id} idx={index} hết {self.max_retries} lần thử "
                                                f"-> hoãn {self.chunk_backoff:.0f}s", logging.WARNING)
            else:
                event(self.log, "CHUNK_RETRY", f"file={self.file_id} idx={index} lần={self.attempts[index]}/"
                                               f"{self.max_retries} -> xin lại từ nguồn khác")
            return False
        finally:
            self.active[pid] -= 1

    async def run(self) -> Path:
        """Vòng lặp chính. Trả về đường dẫn file đã ghép và kiểm tra hash."""
        t0 = time.monotonic()
        total = self.meta["num_chunks"]
        event(self.log, "DOWNLOAD_START", f"file={self.file_id} name={self.meta['name']} have={len(self.store.have)}/"
                                          f"{total} max_parallel={self.max_parallel} per_peer={self.per_peer_limit}")
        tasks: dict[asyncio.Task, int] = {}
        last_refresh = 0.0
        last_progress = time.monotonic()
        try:
            while not self.store.is_complete():
                now = time.monotonic()
                if now - last_refresh >= self.refresh_interval:
                    await self._refresh_peers()
                    last_refresh = now
                while len(tasks) < self.max_parallel:
                    choice = self._pick()
                    if choice is None:
                        break
                    index, peer = choice
                    self.in_flight.add(index)
                    self.active[peer["peer_id"]] += 1
                    tasks[asyncio.create_task(self._attempt(index, peer))] = index
                if not tasks:
                    if now - last_progress > self.stall_timeout:
                        raise DownloadStalled(f"không tải thêm được chunk nào trong {self.stall_timeout:.0f}s")
                    if now - self._last_wait_log > 3:
                        self._last_wait_log = now
                        event(self.log, "WAITING_FOR_SOURCES", f"file={self.file_id} thiếu={len(self.store.missing())} "
                                                               f"nguồn={len(self.peers)}")
                    await asyncio.sleep(0.2)
                    continue
                done, _ = await asyncio.wait(tasks, timeout=self.refresh_interval,
                                             return_when=asyncio.FIRST_COMPLETED)
                for t in done:
                    self.in_flight.discard(tasks.pop(t))
                    if t.result():
                        last_progress = time.monotonic()
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

        out = self.store.assemble()        # ghép đúng thứ tự + kiểm tra hash toàn file
        self.stats["elapsed"] = round(time.monotonic() - t0, 3)
        sources = " ".join(f"{pid}={n}" for pid, n in sorted(self.stats["by_source"].items()))
        event(self.log, "FILE_VERIFIED", f"file={self.file_id} sha256={self.meta['file_hash'][:16]}… khớp metadata")
        event(self.log, "DOWNLOAD_COMPLETE", f"file={self.file_id} -> {out} time={self.stats['elapsed']}s "
                                             f"sources[{sources}] retries={self.stats['retries']} "
                                             f"rejected={self.stats['rejected']}")
        self._save_stats()
        return out

    def _save_stats(self) -> None:
        stats = dict(self.stats, by_source=dict(self.stats["by_source"]), peer_id=self.peer_id, file_id=self.file_id)
        with contextlib.suppress(OSError):
            (self.store.root / "stats.json").write_text(json.dumps(stats, indent=2), encoding="utf-8")

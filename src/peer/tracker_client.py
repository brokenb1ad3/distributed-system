"""Client gọi API tracker (thư viện chuẩn urllib, chạy trong thread để không chặn event loop)."""
from __future__ import annotations

import asyncio
import json
import logging
import urllib.error
import urllib.request

from common.log import event

# Bỏ qua proxy hệ thống: tracker luôn được gọi trực tiếp.
_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


class TrackerError(Exception):
    def __init__(self, status: int | None, message: str):
        super().__init__(f"[{status}] {message}")
        self.status = status


class TrackerClient:
    def __init__(self, base_url: str, token: str, peer_id: str, host: str, port: int, timeout: float = 5.0):
        self.base = base_url.rstrip("/")
        self.token = token
        self.peer_id, self.host, self.port = peer_id, host, port
        self.timeout = timeout

    # ---------- HTTP ----------
    def _request(self, method: str, path: str, body: object = None) -> object:
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(self.base + path, data=data, method=method,
                                     headers={"X-Peer-Token": self.token, "Content-Type": "application/json"})
        try:
            with _OPENER.open(req, timeout=self.timeout) as resp:
                return json.loads(resp.read() or b"null")
        except urllib.error.HTTPError as e:
            try:
                msg = json.loads(e.read()).get("error", e.reason)
            except Exception:
                msg = str(e.reason)
            raise TrackerError(e.code, msg) from None
        except (urllib.error.URLError, OSError, TimeoutError) as e:
            raise TrackerError(None, f"không kết nối được tracker: {e}") from None

    async def _call(self, method: str, path: str, body: object = None) -> object:
        return await asyncio.to_thread(self._request, method, path, body)

    # ---------- API ----------
    async def register_file(self, meta: dict) -> dict:
        return await self._call("POST", "/files", meta)  # type: ignore[return-value]

    async def get_metadata(self, file_id: str) -> dict:
        return await self._call("GET", f"/files/{file_id}")  # type: ignore[return-value]

    async def list_files(self) -> list[dict]:
        return await self._call("GET", "/files")  # type: ignore[return-value]

    async def announce(self, file_id: str, chunks: list[int], event_name: str = "update") -> dict:
        return await self._call("POST", "/announce", {
            "peer_id": self.peer_id, "host": self.host, "port": self.port,
            "file_id": file_id, "chunks": sorted(chunks), "event": event_name})  # type: ignore[return-value]

    async def get_peers(self, file_id: str) -> list[dict]:
        peers = await self._call("GET", f"/files/{file_id}/peers")
        return [p for p in peers if p["peer_id"] != self.peer_id]  # type: ignore[union-attr]

    # ---------- heartbeat ----------
    async def announce_loop(self, store, log: logging.Logger, default_interval: float = 2.0) -> None:
        """Announce định kỳ (heartbeat + cập nhật danh sách chunk).
        Tracker không trả lời -> ghi log, thử lại với backoff (tối đa 10s), KHÔNG làm sập peer.
        Tracker khởi động lại và mất metadata (404) -> tự đăng ký lại metadata."""
        first = True
        was_complete = store.is_complete()
        backoff = default_interval
        while True:
            try:
                complete = store.is_complete()
                if first:
                    ev = "started"
                elif complete and not was_complete:
                    ev = "completed"
                else:
                    ev = "update"
                resp = await self.announce(store.file_id, list(store.have), ev)
                if first or backoff != default_interval:
                    event(log, "ANNOUNCE_OK", f"file={store.file_id} have={len(store.have)}/{store.meta['num_chunks']}")
                first = False
                was_complete = complete
                backoff = default_interval
                interval = float(resp.get("interval") or default_interval)
            except TrackerError as e:
                if e.status == 404:
                    event(log, "TRACKER_LOST_FILE", f"file={store.file_id} -> đăng ký lại metadata", logging.WARNING)
                    try:
                        await self.register_file(store.meta)
                        continue
                    except TrackerError:
                        pass
                event(log, "TRACKER_UNREACHABLE", f"{e} | thử lại sau {backoff:.0f}s", logging.WARNING)
                interval = backoff
                backoff = min(backoff * 2, 10.0)
            await asyncio.sleep(interval)

"""TCP server phục vụ GET_CHUNK cho peer khác.

Chạy song song với downloader trong cùng event loop, nên một peer phục vụ được
các chunk đã có NGAY CẢ KHI chưa tải xong file (yêu cầu demo bắt buộc).
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
import random

from common import protocol as P
from common.log import event
from common.metadata import sha256_hex


class Uploader:
    def __init__(self, peer_id: str, stores: dict, log: logging.Logger, max_uploads: int = 8,
                 upload_delay: float = 0.0, corrupt_rate: float = 0.0, idle_timeout: float = 30.0):
        self.peer_id = peer_id
        self.stores = stores              # file_id -> ChunkStore
        self.log = log
        self.max_uploads = max_uploads
        self.upload_delay = upload_delay  # giả lập băng thông upload hạn chế (giây/chunk)
        self.corrupt_rate = corrupt_rate  # CHỈ DÙNG ĐỂ DEMO: tỉ lệ cố tình gửi chunk hỏng
        self.idle_timeout = idle_timeout
        self.active = 0                   # số kết nối đang phục vụ (chỉ 1 event loop -> không cần lock)
        self.sent_chunks = 0

    async def start(self, host: str, port: int) -> asyncio.base_events.Server:
        server = await asyncio.start_server(self.handle, host, port)
        extra = []
        if self.upload_delay:
            extra.append(f"upload_delay={self.upload_delay}s")
        if self.corrupt_rate:
            extra.append(f"CORRUPT_RATE={self.corrupt_rate} (peer độc hại để demo)")
        event(self.log, "UPLOAD_LISTEN", f"{host}:{port} max_uploads={self.max_uploads} {' '.join(extra)}".rstrip())
        return server

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        addr = writer.get_extra_info("peername")
        if self.active >= self.max_uploads:
            event(self.log, "UPLOAD_BUSY", f"từ chối {addr}: đang phục vụ {self.active}/{self.max_uploads}")
            with contextlib.suppress(Exception):
                await P.send_frame(writer, {"type": P.ERROR, "msg_id": None, "code": "BUSY",
                                            "reason": "too many uploads"})
            await self._close(writer)
            return

        self.active += 1
        try:
            while True:
                try:
                    header, _ = await asyncio.wait_for(P.read_frame(reader), self.idle_timeout)
                except (asyncio.IncompleteReadError, asyncio.TimeoutError, ConnectionError):
                    break                                    # peer đóng kết nối hoặc im lặng quá lâu
                except P.ProtocolError as e:
                    event(self.log, "PROTOCOL_VIOLATION", f"from={addr} {e} -> đóng kết nối", logging.WARNING)
                    break
                await self._serve(header, writer)
        except ConnectionError:
            pass
        finally:
            self.active -= 1
            await self._close(writer)

    async def _serve(self, h: dict, writer: asyncio.StreamWriter) -> None:
        mid, fid, idx, who = h.get("msg_id"), h.get("file_id"), h.get("index"), h.get("from", "?")

        async def error(code: str, reason: str) -> None:
            await P.send_frame(writer, {"type": P.ERROR, "msg_id": mid, "code": code, "reason": reason})

        if h["type"] != P.GET_CHUNK:
            return await error("BAD_REQUEST", "chỉ hỗ trợ GET_CHUNK")
        if not isinstance(idx, int) or isinstance(idx, bool):
            return await error("BAD_REQUEST", "index không hợp lệ")
        store = self.stores.get(fid)
        data = store.get(idx) if store else None
        if data is None:
            event(self.log, "CHUNK_NOT_FOUND", f"file={fid} idx={idx} to={who}")
            return await error("NOT_FOUND", "không có chunk này")

        if self.upload_delay:
            await asyncio.sleep(self.upload_delay)
        real_hash = sha256_hex(data)
        if self.corrupt_rate and random.random() < self.corrupt_rate:
            data = bytes([data[0] ^ 0xFF]) + data[1:]
            event(self.log, "CHUNK_CORRUPTED_ON_PURPOSE", f"file={fid} idx={idx} to={who}", logging.WARNING)

        await P.send_frame(writer, {"type": P.CHUNK, "msg_id": mid, "from": self.peer_id,
                                    "file_id": fid, "index": idx, "sha256": real_hash}, data)
        self.sent_chunks += 1
        complete = "seeder" if store.is_complete() else f"partial {len(store.have)}/{store.meta['num_chunks']}"
        event(self.log, "CHUNK_SENT", f"file={fid} idx={idx} to={who} bytes={len(data)} (mình đang {complete})")

    @staticmethod
    async def _close(writer: asyncio.StreamWriter) -> None:
        writer.close()
        with contextlib.suppress(Exception):
            await writer.wait_closed()

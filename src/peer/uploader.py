"""[NGƯỜI B] TCP server phục vụ GET_CHUNK cho peer khác.

Phải chạy song song với downloader. Đây là điểm chứng minh yêu cầu
"upload trong khi chưa tải xong" (demo bắt buộc).
"""
from __future__ import annotations

import asyncio

from common import protocol as P
from common.log import event

MAX_UPLOADS = 8


class Uploader:
    def __init__(self, peer_id: str, stores: dict, log, max_uploads: int = MAX_UPLOADS):
        self.peer_id = peer_id
        self.stores = stores  # file_id -> ChunkStore
        self.log = log
        self.sem = asyncio.Semaphore(max_uploads)

    async def start(self, host: str, port: int):
        server = await asyncio.start_server(self.handle, host, port)
        event(self.log, "UPLOAD_LISTEN", f"{host}:{port}")
        return server

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """TODO(B):
          - nếu sem đã đầy -> trả ERROR code=BUSY
          - vòng lặp read_frame: GET_CHUNK -> kiểm tra file_id, index hợp lệ
              có chunk -> send_frame CHUNK (kèm sha256), không có -> ERROR NOT_FOUND
          - bắt ProtocolError -> ghi log cảnh báo + đóng kết nối
          - ghi log CHUNK_SENT cho mỗi chunk gửi đi
        """
        raise NotImplementedError

"""[NGƯỜI B] Lưu chunk xuống đĩa, quản lý trạng thái để resume, ghép file.

Thư mục dữ liệu của mỗi peer (DATA_DIR):
    <DATA_DIR>/<file_id>/chunks/<index>.chunk
    <DATA_DIR>/<file_id>.state.json      -> {"have": [0, 1, 5]}
    <DATA_DIR>/downloads/<name>          -> file sau khi ghép
"""
from __future__ import annotations

import asyncio
from pathlib import Path


class ChunkStore:
    def __init__(self, data_dir: Path, meta: dict):
        self.data_dir = data_dir
        self.meta = meta
        self.have: set[int] = set()
        self._lock = asyncio.Lock()  # nhiều luồng tải cùng ghi -> phải khóa

    @classmethod
    def from_seed_file(cls, data_dir: Path, meta: dict, src: Path) -> "ChunkStore":
        """Seeder: tách file gốc thành chunk, đánh dấu có đủ toàn bộ chunk. TODO(B)"""
        raise NotImplementedError

    def load_state(self) -> None:
        """Resume: đọc state.json, KIỂM TRA LẠI hash từng chunk, chunk nào sai thì bỏ. TODO(B)"""
        raise NotImplementedError

    async def put(self, index: int, data: bytes) -> bool:
        """Kiểm tra hash, ghi chunk, cập nhật have và state.json (ghi file tạm rồi rename).
        Chunk đã có thì trả False (chống trùng). TODO(B)"""
        raise NotImplementedError

    def get(self, index: int) -> bytes | None:
        """TODO(B)"""
        raise NotImplementedError

    def is_complete(self) -> bool:
        return len(self.have) == self.meta["num_chunks"]

    def assemble(self) -> Path:
        """Ghép chunk theo đúng thứ tự index, kiểm tra file_hash, trả về đường dẫn file. TODO(B)"""
        raise NotImplementedError

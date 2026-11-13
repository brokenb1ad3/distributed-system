"""Lưu chunk xuống đĩa, quản lý trạng thái để resume, ghép file.

Cấu trúc thư mục dữ liệu của mỗi peer (DATA_DIR):
    <DATA_DIR>/<file_id>/meta.json          metadata đã kiểm tra
    <DATA_DIR>/<file_id>/chunks/<i>.chunk   từng chunk đã xác minh hash
    <DATA_DIR>/<file_id>/state.json         {"have": [...]}  -> dùng để resume
    <DATA_DIR>/downloads/<name>             file sau khi ghép
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
from pathlib import Path

from common.metadata import load_metadata, save_metadata, verify_chunk


class IntegrityError(Exception):
    """Hash của chunk hoặc của cả file không khớp metadata."""


def atomic_write(path: Path, data: bytes) -> None:
    """Ghi ra file tạm rồi rename: nếu chương trình bị kill giữa chừng, file cũ vẫn nguyên vẹn."""
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


class ChunkStore:
    def __init__(self, data_dir: Path | str, meta: dict):
        self.meta = meta
        self.file_id: str = meta["file_id"]
        self.data_dir = Path(data_dir)
        self.root = self.data_dir / self.file_id
        self.chunk_dir = self.root / "chunks"
        self.chunk_dir.mkdir(parents=True, exist_ok=True)
        self.state_path = self.root / "state.json"
        self.have: set[int] = set()
        # Nhiều coroutine tải song song cùng ghi vào store -> cần khóa để
        # kiểm tra "đã có chưa" và "ghi + cập nhật state" là một thao tác nguyên tử.
        self._lock = asyncio.Lock()
        meta_path = self.root / "meta.json"
        if not meta_path.exists():
            save_metadata(meta, meta_path)

    # ---------- khởi tạo ----------
    @classmethod
    def open_existing(cls, data_dir: Path | str, file_id: str) -> "ChunkStore":
        meta = load_metadata(Path(data_dir) / file_id / "meta.json")
        store = cls(data_dir, meta)
        store.load_state()
        return store

    @classmethod
    def from_seed_file(cls, data_dir: Path | str, meta: dict, src: Path | str) -> "ChunkStore":
        """Seeder: tách file gốc thành các chunk, kiểm tra hash, đánh dấu có đủ."""
        store = cls(data_dir, meta)
        with open(src, "rb") as f:
            for i in range(meta["num_chunks"]):
                data = f.read(meta["chunk_size"])
                if not verify_chunk(meta, i, data):
                    raise IntegrityError(f"file nguồn đã thay đổi so với metadata (chunk {i})")
                atomic_write(store._chunk_path(i), data)
                store.have.add(i)
        store._save_state()
        return store

    # ---------- resume ----------
    def load_state(self) -> int:
        """Đọc state.json và KIỂM TRA LẠI hash từng chunk trên đĩa.
        Chunk bị hỏng/thiếu sẽ bị loại và tải lại. Trả về số chunk khôi phục được."""
        self.have.clear()
        claimed: list[int] = []
        if self.state_path.exists():
            try:
                claimed = json.loads(self.state_path.read_text(encoding="utf-8")).get("have", [])
            except (json.JSONDecodeError, OSError):
                claimed = []
        for i in claimed:
            p = self._chunk_path(i) if isinstance(i, int) else None
            if p and p.exists() and verify_chunk(self.meta, i, p.read_bytes()):
                self.have.add(i)
        self._save_state()
        return len(self.have)

    # ---------- đọc / ghi chunk ----------
    def _chunk_path(self, index: int) -> Path:
        return self.chunk_dir / f"{index}.chunk"

    def _save_state(self) -> None:
        state = {"file_id": self.file_id, "have": sorted(self.have)}
        atomic_write(self.state_path, json.dumps(state).encode())

    async def put(self, index: int, data: bytes) -> bool:
        """Lưu chunk. Trả về False nếu đã có (chống trùng). Ném IntegrityError nếu hash sai."""
        async with self._lock:
            if index in self.have:
                return False
            if not verify_chunk(self.meta, index, data):
                raise IntegrityError(f"chunk {index} sai hash")
            atomic_write(self._chunk_path(index), data)
            self.have.add(index)
            self._save_state()
            return True

    def get(self, index: int) -> bytes | None:
        if index not in self.have:
            return None
        try:
            return self._chunk_path(index).read_bytes()
        except OSError:
            return None

    def missing(self) -> set[int]:
        return set(range(self.meta["num_chunks"])) - self.have

    def is_complete(self) -> bool:
        return len(self.have) == self.meta["num_chunks"]

    # ---------- ghép file ----------
    @property
    def output_path(self) -> Path:
        return self.data_dir / "downloads" / self.meta["name"]

    def assemble(self) -> Path:
        """Ghép chunk theo đúng thứ tự index, kiểm tra file_hash, trả về đường dẫn file."""
        if not self.is_complete():
            raise IntegrityError(f"chưa đủ chunk: còn thiếu {len(self.missing())}")
        out = self.output_path
        out.parent.mkdir(parents=True, exist_ok=True)
        tmp = out.with_suffix(out.suffix + ".part")
        whole = hashlib.sha256()
        with open(tmp, "wb") as f:
            for i in range(self.meta["num_chunks"]):
                data = self._chunk_path(i).read_bytes()
                whole.update(data)
                f.write(data)
        if whole.hexdigest() != self.meta["file_hash"]:
            tmp.unlink(missing_ok=True)
            raise IntegrityError("hash toàn file không khớp metadata")
        os.replace(tmp, out)
        return out

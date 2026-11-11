"""[NGƯỜI B] Tạo metadata, chia chunk, ghép file, kiểm tra hash.

Đọc docs/PROTOCOL.md mục 1–2 để biết định dạng metadata.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

DEFAULT_CHUNK_SIZE = 256 * 1024


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def create_metadata(path: Path, tracker_url: str, chunk_size: int = DEFAULT_CHUNK_SIZE) -> dict:
    """Đọc file theo từng chunk, tính hash từng chunk và hash cả file, trả về dict metadata.

    TODO(B):
      - đọc file theo khối chunk_size (KHÔNG đọc cả file vào RAM một lần)
      - chunk_hashes, file_hash, num_chunks, size
      - file_id = sha256(file_hash + name + str(chunk_size))[:16]
    """
    raise NotImplementedError


def save_metadata(meta: dict, out: Path) -> None:
    out.write_text(json.dumps(meta, indent=2), encoding="utf-8")


def load_metadata(path: Path) -> dict:
    """TODO(B): kiểm tra đủ trường, kiểu dữ liệu đúng và len(chunk_hashes) == num_chunks."""
    raise NotImplementedError


def verify_chunk(meta: dict, index: int, data: bytes) -> bool:
    return 0 <= index < meta["num_chunks"] and sha256_hex(data) == meta["chunk_hashes"][index]

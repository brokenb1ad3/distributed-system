"""Tạo, kiểm tra và đọc/ghi metadata (tương tự file .torrent).

Xem docs/PROTOCOL.md mục 1–2.
"""
from __future__ import annotations

import hashlib
import json
import math
import re
from datetime import datetime, timezone
from pathlib import Path

DEFAULT_CHUNK_SIZE = 256 * 1024
MAX_CHUNK_SIZE = 1024 * 1024
MAX_NAME_LEN = 255

_HEX64 = re.compile(r"^[0-9a-f]{64}$")
_FILE_ID = re.compile(r"^[0-9a-f]{16}$")


class MetadataError(ValueError):
    """Metadata thiếu trường, sai kiểu hoặc không nhất quán."""


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def compute_file_id(file_hash: str, name: str, chunk_size: int) -> str:
    return sha256_hex(f"{file_hash}:{name}:{chunk_size}".encode())[:16]


def safe_name(name: str) -> bool:
    """Chặn path traversal: không cho '/', '\\', '..', ký tự điều khiển."""
    return (
        0 < len(name) <= MAX_NAME_LEN
        and "/" not in name and "\\" not in name
        and name not in (".", "..") and ".." not in name
        and all(ord(c) >= 32 for c in name)
    )


def create_metadata(path: Path | str, tracker_url: str, chunk_size: int = DEFAULT_CHUNK_SIZE) -> dict:
    """Đọc file theo từng khối chunk_size (không nạp cả file vào RAM),
    tính hash từng chunk và hash toàn file."""
    path = Path(path)
    if not 0 < chunk_size <= MAX_CHUNK_SIZE:
        raise MetadataError(f"chunk_size phải trong (0, {MAX_CHUNK_SIZE}]")
    if not safe_name(path.name):
        raise MetadataError(f"tên file không hợp lệ: {path.name!r}")

    chunk_hashes: list[str] = []
    whole = hashlib.sha256()
    size = 0
    with open(path, "rb") as f:
        while block := f.read(chunk_size):
            chunk_hashes.append(sha256_hex(block))
            whole.update(block)
            size += len(block)
    if size == 0:
        raise MetadataError("không chia sẻ file rỗng")

    file_hash = whole.hexdigest()
    return {
        "file_id": compute_file_id(file_hash, path.name, chunk_size),
        "name": path.name,
        "size": size,
        "chunk_size": chunk_size,
        "num_chunks": len(chunk_hashes),
        "chunk_hashes": chunk_hashes,
        "file_hash": file_hash,
        "tracker": tracker_url,
        "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }


def validate_metadata(meta: object) -> dict:
    """Kiểm tra toàn bộ metadata nhận từ mạng/đĩa. Ném MetadataError nếu sai."""
    if not isinstance(meta, dict):
        raise MetadataError("metadata phải là JSON object")
    types = {"file_id": str, "name": str, "size": int, "chunk_size": int,
             "num_chunks": int, "chunk_hashes": list, "file_hash": str, "tracker": str}
    for key, typ in types.items():
        if key not in meta:
            raise MetadataError(f"thiếu trường {key}")
        if not isinstance(meta[key], typ) or (typ is int and isinstance(meta[key], bool)):
            raise MetadataError(f"trường {key} sai kiểu")
    if not _FILE_ID.match(meta["file_id"]):
        raise MetadataError("file_id phải là 16 ký tự hex")
    if not safe_name(meta["name"]):
        raise MetadataError("tên file không hợp lệ")
    if not 0 < meta["chunk_size"] <= MAX_CHUNK_SIZE:
        raise MetadataError("chunk_size ngoài giới hạn")
    if meta["size"] <= 0:
        raise MetadataError("size phải > 0")
    if meta["num_chunks"] != math.ceil(meta["size"] / meta["chunk_size"]):
        raise MetadataError("num_chunks không khớp size/chunk_size")
    if len(meta["chunk_hashes"]) != meta["num_chunks"]:
        raise MetadataError("số lượng chunk_hashes không khớp num_chunks")
    if not all(isinstance(h, str) and _HEX64.match(h) for h in meta["chunk_hashes"]):
        raise MetadataError("chunk_hashes phải là sha256 hex")
    if not _HEX64.match(meta["file_hash"]):
        raise MetadataError("file_hash phải là sha256 hex")
    if meta["file_id"] != compute_file_id(meta["file_hash"], meta["name"], meta["chunk_size"]):
        raise MetadataError("file_id không khớp nội dung metadata")
    return meta


def chunk_length(meta: dict, index: int) -> int:
    """Độ dài đúng của chunk index (chunk cuối có thể ngắn hơn)."""
    if index == meta["num_chunks"] - 1:
        return meta["size"] - index * meta["chunk_size"]
    return meta["chunk_size"]


def verify_chunk(meta: dict, index: int, data: bytes) -> bool:
    return (
        isinstance(index, int)
        and 0 <= index < meta["num_chunks"]
        and len(data) == chunk_length(meta, index)
        and sha256_hex(data) == meta["chunk_hashes"][index]
    )


def save_metadata(meta: dict, out: Path | str) -> None:
    Path(out).write_text(json.dumps(meta, indent=2), encoding="utf-8")


def load_metadata(path: Path | str) -> dict:
    try:
        meta = json.loads(Path(path).read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        raise MetadataError(f"file metadata không phải JSON hợp lệ: {e}") from e
    return validate_metadata(meta)


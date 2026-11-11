"""Đóng gói và đọc frame giữa các peer (xem docs/PROTOCOL.md, mục 4).

Frame = 4 byte độ dài header (big-endian) + JSON header + payload.
Module này là "hợp đồng" chung của cả nhóm: sửa gì phải báo cả nhóm.
"""
from __future__ import annotations

import asyncio
import json
import struct
import uuid

MAX_HEADER_SIZE = 4 * 1024
MAX_PAYLOAD_SIZE = 1024 * 1024  # 1 MB

# Các loại message
GET_CHUNK = "GET_CHUNK"
CHUNK = "CHUNK"
ERROR = "ERROR"
VALID_TYPES = {GET_CHUNK, CHUNK, ERROR}


class ProtocolError(Exception):
    """Frame sai định dạng hoặc vượt giới hạn kích thước."""


def new_msg_id() -> str:
    return uuid.uuid4().hex


async def send_frame(writer: asyncio.StreamWriter, header: dict, payload: bytes = b"") -> None:
    if len(payload) > MAX_PAYLOAD_SIZE:
        raise ProtocolError(f"payload too large: {len(payload)}")
    header = dict(header, payload_len=len(payload))
    raw = json.dumps(header, separators=(",", ":")).encode()
    if len(raw) > MAX_HEADER_SIZE:
        raise ProtocolError("header too large")
    writer.write(struct.pack(">I", len(raw)) + raw + payload)
    await writer.drain()


async def read_frame(reader: asyncio.StreamReader) -> tuple[dict, bytes]:
    """Đọc 1 frame. Ném ProtocolError nếu dữ liệu không hợp lệ."""
    (hlen,) = struct.unpack(">I", await reader.readexactly(4))
    if hlen == 0 or hlen > MAX_HEADER_SIZE:
        raise ProtocolError(f"invalid header length {hlen}")
    try:
        header = json.loads(await reader.readexactly(hlen))
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        raise ProtocolError(f"bad header json: {e}") from e
    if not isinstance(header, dict) or header.get("type") not in VALID_TYPES:
        raise ProtocolError(f"unknown message type: {header!r:.80}")
    plen = header.get("payload_len", 0)
    if not isinstance(plen, int) or plen < 0 or plen > MAX_PAYLOAD_SIZE:
        raise ProtocolError(f"invalid payload_len {plen}")
    payload = await reader.readexactly(plen) if plen else b""
    return header, payload

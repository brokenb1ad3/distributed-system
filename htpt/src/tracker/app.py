"""[NGƯỜI A] Tracker: chỉ quản lý metadata và danh sách peer. KHÔNG lưu nội dung file.

Chạy: uvicorn tracker.app:app --host 0.0.0.0 --port 8000   (đứng ở thư mục src/)
"""
from __future__ import annotations

import asyncio
import os
import time

from fastapi import Depends, FastAPI, Header, HTTPException
from pydantic import BaseModel, Field

from common.log import event, get_logger

TOKEN = os.getenv("TRACKER_TOKEN", "")
PEER_TTL = float(os.getenv("PEER_TTL", "15"))
ANNOUNCE_INTERVAL = int(os.getenv("ANNOUNCE_INTERVAL", "5"))
log = get_logger("tracker", os.getenv("LOG_FILE"))

app = FastAPI(title="P2P Tracker")
files: dict[str, dict] = {}                 # file_id -> metadata
swarm: dict[str, dict[str, dict]] = {}      # file_id -> peer_id -> {host, port, chunks, last_seen}
lock = asyncio.Lock()


def auth(x_peer_token: str = Header(default="")):
    if not TOKEN or x_peer_token != TOKEN:
        raise HTTPException(401, "invalid token")


class Metadata(BaseModel):
    file_id: str = Field(pattern=r"^[0-9a-f]{16}$")
    name: str = Field(min_length=1, max_length=255)
    size: int = Field(ge=0)
    chunk_size: int = Field(gt=0, le=1024 * 1024)
    num_chunks: int = Field(ge=0)
    chunk_hashes: list[str]
    file_hash: str
    tracker: str
    created_at: str | None = None


class Announce(BaseModel):
    peer_id: str = Field(pattern=r"^[A-Za-z0-9_-]{1,32}$")
    host: str = Field(max_length=255)
    port: int = Field(ge=1, le=65535)
    file_id: str
    chunks: list[int]
    event: str = Field(pattern=r"^(started|update|completed|stopped)$")


@app.post("/files", status_code=201, dependencies=[Depends(auth)])
async def register_file(meta: Metadata):
    """TODO(A): kiểm tra len(chunk_hashes) == num_chunks, tên file không chứa '/', '..'; ghi log FILE_REGISTERED."""
    raise NotImplementedError


@app.get("/files/{file_id}", dependencies=[Depends(auth)])
async def get_file(file_id: str):
    """TODO(A): không có thì trả 404."""
    raise NotImplementedError


@app.post("/announce", dependencies=[Depends(auth)])
async def announce(a: Announce):
    """TODO(A): file phải tồn tại; chunk index phải nằm trong [0, num_chunks);
    stopped -> xóa peer; còn lại -> cập nhật last_seen. Ghi log PEER_ANNOUNCE / PEER_LEFT."""
    raise NotImplementedError


@app.get("/files/{file_id}/peers", dependencies=[Depends(auth)])
async def get_peers(file_id: str):
    """TODO(A): chỉ trả về các peer có last_seen trong PEER_TTL giây gần nhất."""
    raise NotImplementedError


async def reaper():
    """Xóa peer hết hạn lease (không announce quá PEER_TTL). TODO(A): ghi log PEER_EXPIRED."""
    while True:
        await asyncio.sleep(1)
        now = time.time()
        async with lock:
            for fid, peers in swarm.items():
                for pid in [p for p, info in peers.items() if now - info["last_seen"] > PEER_TTL]:
                    del peers[pid]
                    event(log, "PEER_EXPIRED", f"file={fid} peer={pid}")


@app.on_event("startup")
async def _start():
    asyncio.create_task(reaper())

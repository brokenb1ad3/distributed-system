"""Điểm khởi chạy peer.

  python -m peer.main seed  <đường dẫn file>   # chia sẻ file, in ra file_id
  python -m peer.main get   <file_id>          # tải file (vẫn upload cho peer khác trong lúc tải)
  python -m peer.main serve                    # chỉ phục vụ các file đã có trong DATA_DIR

Cấu hình đọc từ biến môi trường (xem .env.example).
"""
from __future__ import annotations

import argparse
import asyncio
import os
from pathlib import Path

from common.log import get_logger


def env(name: str, default: str | None = None) -> str:
    v = os.getenv(name, default)
    if v is None:
        raise SystemExit(f"missing env {name}")
    return v


async def amain(args) -> None:
    peer_id = env("PEER_ID")
    log = get_logger(peer_id, os.getenv("LOG_FILE"))
    data_dir = Path(env("DATA_DIR", f"./data/{peer_id}"))
    data_dir.mkdir(parents=True, exist_ok=True)
    # TODO(A+B+C, làm khi ghép code ngày 14/11):
    #  1. tạo TrackerClient(env TRACKER_URL, TRACKER_TOKEN, peer_id, PEER_HOST, PEER_PORT)
    #  2. seed: create_metadata -> ChunkStore.from_seed_file -> register_file
    #     get : get_metadata -> ChunkStore(...).load_state()   (resume)
    #  3. bật Uploader trên PEER_PORT + announce_loop
    #  4. get: chạy Downloader.run(); xong thì tiếp tục làm seeder
    #  5. bắt SIGTERM/Ctrl+C -> announce event=stopped
    raise NotImplementedError


def main() -> None:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("seed").add_argument("path")
    sub.add_parser("get").add_argument("file_id")
    sub.add_parser("serve")
    asyncio.run(amain(ap.parse_args()))


if __name__ == "__main__":
    main()

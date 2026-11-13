"""Điểm khởi chạy peer (đứng ở thư mục src/).

    python -m peer.main seed <đường dẫn file> [--chunk-size N]   chia sẻ file, in ra file_id
    python -m peer.main get  <file_id> [--exit]                  tải file; vừa tải vừa upload cho peer khác
    python -m peer.main serve                                    phục vụ mọi file đã có trong DATA_DIR
    python -m peer.main list                                     xem các file tracker đang quản lý

Cấu hình đọc từ biến môi trường / file .env (xem .env.example).
"""
from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import os
import re
import signal
from pathlib import Path

from common.config import ROOT, env_float, env_int, env_str, load_dotenv
from common.log import event, get_logger
from common.metadata import (DEFAULT_CHUNK_SIZE, MetadataError, create_metadata, save_metadata,
                             validate_metadata)
from peer.downloader import DownloadStalled, Downloader
from peer.storage import ChunkStore
from peer.tracker_client import TrackerClient, TrackerError
from peer.uploader import Uploader

PEER_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,32}$")
FILE_ID_RE = re.compile(r"^[0-9a-f]{16}$")


async def with_retry(coro_fn, log: logging.Logger, what: str, attempts: int = 10, delay: float = 1.0):
    """Retry có giới hạn cho các lời gọi tracker lúc khởi động (tracker có thể chưa sẵn sàng)."""
    for n in range(1, attempts + 1):
        try:
            return await coro_fn()
        except TrackerError as e:
            if e.status is not None or n == attempts:      # lỗi nghiệp vụ (4xx) thì không retry
                raise
            event(log, "TRACKER_RETRY", f"{what}: {e} | lần {n}/{attempts}", logging.WARNING)
            await asyncio.sleep(delay)


async def amain(args: argparse.Namespace) -> int:
    load_dotenv()
    peer_id = env_str("PEER_ID")
    if not PEER_ID_RE.match(peer_id):
        raise SystemExit("PEER_ID chỉ gồm chữ, số, '-', '_' (tối đa 32 ký tự)")
    log = get_logger(peer_id, os.getenv("LOG_FILE"))
    data_dir = Path(os.getenv("DATA_DIR") or ROOT / "data" / peer_id)
    data_dir.mkdir(parents=True, exist_ok=True)
    host = os.getenv("PEER_HOST", "127.0.0.1")           # địa chỉ quảng bá cho peer khác
    bind = os.getenv("BIND_HOST", "0.0.0.0")             # địa chỉ lắng nghe
    port = env_int("PEER_PORT", 9001)
    tracker_url = env_str("TRACKER_URL", "http://127.0.0.1:8000")
    tracker = TrackerClient(tracker_url, env_str("TRACKER_TOKEN"), peer_id, host, port)

    if args.cmd == "list":
        for f in await tracker.list_files():
            print(f"{f['file_id']}  {f['size']:>12} bytes  {f['num_chunks']:>5} chunks  {f['peers']} peer  {f['name']}")
        return 0

    stores: dict[str, ChunkStore] = {}
    target: ChunkStore | None = None

    if args.cmd == "seed":
        src = Path(args.path)
        if not src.is_file():
            raise SystemExit(f"không tìm thấy file {src}")
        meta = create_metadata(src, tracker_url, args.chunk_size)
        store = ChunkStore.from_seed_file(data_dir, meta, src)
        save_metadata(meta, data_dir / f"{meta['name']}.meta.json")
        event(log, "METADATA_CREATED", f"file={meta['file_id']} name={meta['name']} size={meta['size']} "
                                       f"chunk_size={meta['chunk_size']} chunks={meta['num_chunks']} "
                                       f"sha256={meta['file_hash'][:16]}…")
        resp = await with_retry(lambda: tracker.register_file(meta), log, "đăng ký file")
        event(log, "FILE_SHARED", f"file={meta['file_id']} tracker={tracker_url} status={resp['status']}")
        print(f"FILE_ID={meta['file_id']}", flush=True)
        stores[store.file_id] = store

    elif args.cmd == "get":
        if not FILE_ID_RE.match(args.file_id):
            raise SystemExit("file_id phải là 16 ký tự hex")
        if (data_dir / args.file_id / "meta.json").exists():           # RESUME sau khi khởi động lại
            target = ChunkStore.open_existing(data_dir, args.file_id)
            event(log, "RESUME", f"file={target.file_id} khôi phục {len(target.have)}/{target.meta['num_chunks']} "
                                 f"chunk từ state.json (đã kiểm tra lại hash)")
        else:
            try:
                meta = validate_metadata(await with_retry(lambda: tracker.get_metadata(args.file_id), log,
                                                          "lấy metadata"))
            except (TrackerError, MetadataError) as e:
                raise SystemExit(f"không lấy được metadata hợp lệ: {e}")
            if meta["file_id"] != args.file_id:
                raise SystemExit("tracker trả metadata của file khác")
            target = ChunkStore(data_dir, meta)
            event(log, "METADATA_FETCHED", f"file={meta['file_id']} name={meta['name']} chunks={meta['num_chunks']}")
        stores[target.file_id] = target

    else:  # serve
        for meta_path in sorted(data_dir.glob("*/meta.json")):
            try:
                store = ChunkStore.open_existing(data_dir, meta_path.parent.name)
            except (MetadataError, OSError) as e:
                event(log, "STORE_SKIPPED", f"{meta_path}: {e}", logging.WARNING)
                continue
            stores[store.file_id] = store
            event(log, "STORE_LOADED", f"file={store.file_id} name={store.meta['name']} "
                                       f"have={len(store.have)}/{store.meta['num_chunks']}")
        if not stores:
            event(log, "NOTHING_TO_SERVE", f"{data_dir} chưa có file nào", logging.WARNING)

    uploader = Uploader(peer_id, stores, log, max_uploads=env_int("MAX_UPLOADS", 8),
                        upload_delay=env_float("UPLOAD_DELAY", 0.0), corrupt_rate=env_float("CORRUPT_RATE", 0.0))
    server = await uploader.start(bind, port)
    interval = env_float("ANNOUNCE_INTERVAL", 2.0)
    background = [asyncio.create_task(tracker.announce_loop(s, log, interval)) for s in stores.values()]

    stop = asyncio.Event()
    with contextlib.suppress(NotImplementedError, ValueError):          # Windows không hỗ trợ
        asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, stop.set)

    code = 0
    try:
        if target is not None and not target.is_complete():
            dl = Downloader(peer_id, target, tracker, log,
                            max_parallel=env_int("MAX_PARALLEL", 4), per_peer_limit=env_int("PER_PEER_LIMIT", 2),
                            chunk_timeout=env_float("CHUNK_TIMEOUT", 5.0), max_retries=env_int("MAX_RETRIES", 3),
                            suspect_seconds=env_float("SUSPECT_SECONDS", 30.0),
                            stall_timeout=env_float("STALL_TIMEOUT", 60.0))
            dl_task = asyncio.create_task(dl.run())
            stop_task = asyncio.create_task(stop.wait())
            await asyncio.wait({dl_task, stop_task}, return_when=asyncio.FIRST_COMPLETED)
            stop_task.cancel()
            if dl_task.done():
                try:
                    dl_task.result()
                except DownloadStalled as e:
                    event(log, "DOWNLOAD_ABORTED", f"file={target.file_id} {e}", logging.ERROR)
                    code = 2
            else:
                dl_task.cancel()
                await asyncio.gather(dl_task, return_exceptions=True)
        elif target is not None:
            out = target.assemble()
            event(log, "ALREADY_COMPLETE", f"file={target.file_id} -> {out}")

        if args.cmd == "get" and args.exit:
            await asyncio.sleep(0)       # tải xong thì thoát (dùng cho script đo hiệu năng)
        elif code == 0:
            event(log, "SEEDING", f"đang phục vụ {len(stores)} file, Ctrl+C để dừng")
            await stop.wait()
    finally:
        for t in background:
            t.cancel()
        await asyncio.gather(*background, return_exceptions=True)
        for s in stores.values():        # báo tracker rời mạng một cách chủ động
            with contextlib.suppress(Exception):
                await asyncio.wait_for(tracker.announce(s.file_id, list(s.have), "stopped"), 2)
        server.close()
        event(log, "PEER_STOP", "đã báo tracker và đóng cổng upload")
    return code


def main() -> None:
    ap = argparse.ArgumentParser(description="Peer của hệ thống chia sẻ file P2P")
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("seed", help="chia sẻ một file")
    s.add_argument("path")
    s.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE)
    g = sub.add_parser("get", help="tải file theo file_id")
    g.add_argument("file_id")
    g.add_argument("--exit", action="store_true", help="tải xong thì thoát thay vì tiếp tục seed")
    sub.add_parser("serve", help="phục vụ các file đã có")
    sub.add_parser("list", help="liệt kê file trên tracker")
    try:
        raise SystemExit(asyncio.run(amain(ap.parse_args())))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()

"""Tiện ích dùng chung cho test: tracker thật (chạy trong thread) + các peer thật trên TCP localhost."""
import asyncio
import os
import sys
import threading
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from common.log import get_logger                      # noqa: E402
from common.metadata import create_metadata            # noqa: E402
from peer.downloader import Downloader                 # noqa: E402
from peer.storage import ChunkStore                    # noqa: E402
from peer.tracker_client import TrackerClient          # noqa: E402
from peer.uploader import Uploader                     # noqa: E402
from tracker.server import make_server                 # noqa: E402

TOKEN = "test-token"
CHUNK = 16 * 1024


def start_tracker(ttl=30.0, db_path=None, port=0):
    httpd = make_server("127.0.0.1", port, TOKEN, ttl=ttl, announce_interval=1, db_path=db_path)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    return httpd, f"http://127.0.0.1:{httpd.server_address[1]}"


@pytest.fixture
def tracker_url():
    httpd, url = start_tracker()
    yield url
    httpd.shutdown()
    httpd.server_close()


@pytest.fixture
def sample(tmp_path):
    """File ngẫu nhiên 20,5 chunk (chunk cuối ngắn hơn) + metadata."""
    src = tmp_path / "sample.bin"
    src.write_bytes(os.urandom(CHUNK * 20 + CHUNK // 2))
    return src


class Node:
    """Một peer chạy trong test: store + uploader (TCP thật) + tracker client."""

    def __init__(self, name, tmp_path, tracker_url, meta, **uploader_kw):
        self.name = name
        self.log = get_logger(name)
        self.dir = tmp_path / name
        self.meta = meta
        self.tracker_url = tracker_url
        self.store = ChunkStore(self.dir, meta)
        self.uploader = Uploader(name, {meta["file_id"]: self.store}, self.log, **uploader_kw)
        self.server = None
        self.tracker = None
        self.port = None

    async def start(self):
        self.server = await self.uploader.start("127.0.0.1", 0)
        self.port = self.server.sockets[0].getsockname()[1]
        self.tracker = TrackerClient(self.tracker_url, TOKEN, self.name, "127.0.0.1", self.port)
        return self

    async def seed_from(self, src):
        self.store = ChunkStore.from_seed_file(self.dir, self.meta, src)
        self.uploader.stores[self.meta["file_id"]] = self.store
        await self.tracker.register_file(self.meta)
        await self.announce()

    async def give(self, src_store, indexes):
        for i in indexes:
            await self.store.put(i, src_store.get(i))

    async def announce(self, ev="update"):
        return await self.tracker.announce(self.meta["file_id"], list(self.store.have), ev)

    def downloader(self, **kw):
        kw.setdefault("refresh_interval", 0.2)
        kw.setdefault("chunk_timeout", 1.0)
        kw.setdefault("dead_cooldown", 1.0)
        kw.setdefault("stall_timeout", 10.0)
        return Downloader(self.name, self.store, self.tracker, self.log, **kw)

    async def stop(self):
        """Giả lập peer chết đột ngột: đóng cổng, không báo tracker."""
        self.server.close()
        await self.server.wait_closed()


def make_meta(src, tracker_url="http://127.0.0.1:1", chunk=CHUNK):
    return create_metadata(src, tracker_url, chunk)


def run(coro):
    return asyncio.run(coro)

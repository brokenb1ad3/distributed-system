"""[NGƯỜI C] Tải song song, rarest-first, timeout/retry, chuyển nguồn khi peer chết.

Đọc docs/PROTOCOL.md mục 5 trước khi code.
"""
from __future__ import annotations

import asyncio
import random
from collections import Counter

from common import protocol as P
from common.log import event

MAX_PARALLEL = 4
CHUNK_TIMEOUT = 5.0
MAX_RETRIES = 3
SUSPECT_SECONDS = 30


def pick_rarest(missing: set[int], peers: list[dict], in_flight: set[int]) -> int | None:
    """Chọn chunk thiếu mà ít peer có nhất (rarest-first); hòa thì chọn ngẫu nhiên."""
    counts = Counter(i for p in peers for i in p["chunks"] if i in missing and i not in in_flight)
    if not counts:
        return None
    rarest = min(counts.values())
    return random.choice([i for i, c in counts.items() if c == rarest])


class Downloader:
    def __init__(self, peer_id: str, meta: dict, store, tracker, log):
        self.peer_id = peer_id
        self.meta = meta
        self.store = store        # ChunkStore
        self.tracker = tracker    # TrackerClient
        self.log = log
        self.sem = asyncio.Semaphore(MAX_PARALLEL)
        self.in_flight: set[int] = set()
        self.suspect: dict[str, float] = {}  # peer_id -> thời điểm hết nghi ngờ

    async def fetch_chunk(self, peer: dict, index: int) -> bytes:
        """Mở TCP tới peer, gửi GET_CHUNK, đợi CHUNK có cùng msg_id (asyncio.wait_for + CHUNK_TIMEOUT).
        TODO(C): nhận ERROR hoặc hết timeout thì ném exception."""
        raise NotImplementedError

    async def download_one(self, index: int) -> None:
        """TODO(C): thử tối đa MAX_RETRIES lần, mỗi lần chọn một peer có chunk, khác peer vừa lỗi,
        và không nằm trong danh sách nghi ngờ. Hash sai -> đánh dấu peer nghi ngờ.
        Thành công -> store.put(); thất bại hết số lần -> ghi log CHUNK_FAILED."""
        raise NotImplementedError

    async def run(self) -> None:
        """TODO(C): vòng lặp chính
          - lấy danh sách peer từ tracker (làm mới định kỳ: peer chết sẽ tự biến mất)
          - khi còn chunk thiếu: pick_rarest -> tạo task download_one (giới hạn bằng self.sem)
          - đủ chunk -> store.assemble() -> announce event=completed
        """
        raise NotImplementedError

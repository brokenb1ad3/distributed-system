"""C1–C3: kiểm thử đồng thời / xung đột."""
import asyncio
import threading

from conftest import TOKEN, Node, make_meta, run

from peer.tracker_client import TrackerClient


def test_C1_three_leechers_download_concurrently_from_limited_seeder(sample, tmp_path, tracker_url):
    """3 leecher cùng tải, seeder chỉ nhận 2 kết nối cùng lúc (BUSY) -> tất cả vẫn hội tụ về cùng 1 file."""
    async def go():
        meta = make_meta(sample, tracker_url)
        seed = await Node("peer-1", tmp_path, tracker_url, meta, max_uploads=2, upload_delay=0.01).start()
        await seed.seed_from(sample)
        leechers = [await Node(f"peer-{i}", tmp_path, tracker_url, meta).start() for i in (2, 3, 4)]

        async def leech(n):
            await n.announce("started")
            hb = asyncio.create_task(n.tracker.announce_loop(n.store, n.log, 0.2))
            try:
                return await n.downloader().run()
            finally:
                hb.cancel()
        outs = await asyncio.gather(*(leech(n) for n in leechers))
        return outs, seed.uploader.active
    outs, still_active = run(go())
    original = sample.read_bytes()
    assert all(o.read_bytes() == original for o in outs)
    assert still_active == 0


def test_C2_concurrent_writes_of_same_chunk_store_it_exactly_once(sample, tmp_path):
    async def go():
        meta = make_meta(sample)
        seed = Node("peer-1", tmp_path, "http://x", meta)
        from peer.storage import ChunkStore
        seed.store = ChunkStore.from_seed_file(seed.dir, meta, sample)
        target = Node("peer-2", tmp_path, "http://x", meta)
        data = seed.store.get(5)
        results = await asyncio.gather(*(target.store.put(5, data) for _ in range(20)))
        return results, target.store.have
    results, have = run(go())
    assert results.count(True) == 1 and results.count(False) == 19   # chỉ 1 lần ghi thành công
    assert have == {5}


def test_C3_many_peers_announce_to_tracker_at_the_same_time(sample, tmp_path, tracker_url):
    """30 thread announce cùng lúc: tracker không mất cập nhật nào."""
    meta = make_meta(sample, tracker_url)
    boot = TrackerClient(tracker_url, TOKEN, "peer-0", "127.0.0.1", 9000)
    boot._request("POST", "/files", meta)
    errors = []

    def worker(i):
        c = TrackerClient(tracker_url, TOKEN, f"peer-{i}", "127.0.0.1", 10000 + i)
        try:
            for k in range(5):
                c._request("POST", "/announce", {"peer_id": c.peer_id, "host": c.host, "port": c.port,
                                                 "file_id": meta["file_id"], "chunks": list(range(k + 1)),
                                                 "event": "update"})
        except Exception as e:
            errors.append(e)
    threads = [threading.Thread(target=worker, args=(i,)) for i in range(1, 31)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    peers = boot._request("GET", f"/files/{meta['file_id']}/peers")
    assert not errors
    assert len(peers) == 30 and all(p["chunks"] == [0, 1, 2, 3, 4] for p in peers)

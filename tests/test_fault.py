"""R1–R5: kiểm thử lỗi và phục hồi."""
import asyncio
import time

from conftest import TOKEN, Node, make_meta, run, start_tracker

from peer.storage import ChunkStore
from peer.tracker_client import TrackerClient


def test_R1_source_dies_mid_download_switches_to_other_source(sample, tmp_path, tracker_url):
    async def go():
        meta = make_meta(sample, tracker_url)
        a = await Node("peer-1", tmp_path, tracker_url, meta, upload_delay=0.05).start()
        await a.seed_from(sample)
        b = await Node("peer-2", tmp_path, tracker_url, meta, upload_delay=0.05).start()
        await b.give(a.store, range(21))
        await b.announce()
        c = await Node("peer-3", tmp_path, tracker_url, meta).start()
        dl = c.downloader(max_parallel=4, per_peer_limit=2)
        task = asyncio.create_task(dl.run())
        while len(c.store.have) < 4:
            await asyncio.sleep(0.01)
        await a.stop()                                   # peer-1 chết giữa chừng, không báo ai
        have_at_kill = len(c.store.have)
        out = await task
        return out, dl.stats, have_at_kill
    out, stats, have_at_kill = run(go())
    assert 0 < have_at_kill < 21
    assert out.read_bytes() == sample.read_bytes()
    assert stats["source_failures"] >= 1                 # đã phát hiện nguồn hỏng
    assert stats["by_source"]["peer-2"] > 0              # và chuyển sang nguồn còn lại


def test_R2_resume_after_restart_rechecks_hashes(sample, tmp_path, tracker_url):
    async def go():
        meta = make_meta(sample, tracker_url)
        a = await Node("peer-1", tmp_path, tracker_url, meta).start()
        await a.seed_from(sample)
        c = await Node("peer-3", tmp_path, tracker_url, meta).start()
        await c.give(a.store, range(0, 12))              # "đã tải được 12 chunk rồi bị kill"
        (c.store.chunk_dir / "3.chunk").write_bytes(b"hong")   # 1 chunk trên đĩa bị hỏng

        restarted = ChunkStore.open_existing(c.dir, meta["file_id"])   # khởi động lại chương trình
        resumed = set(restarted.have)
        c.store = restarted
        dl = c.downloader()
        out = await dl.run()
        return resumed, out, dl.stats
    resumed, out, stats = run(go())
    assert resumed == set(range(12)) - {3}               # chunk hỏng bị loại khi resume
    assert sum(stats["by_source"].values()) == 21 - 11   # chỉ tải phần còn thiếu
    assert out.read_bytes() == sample.read_bytes()


def test_R3_tracker_expires_peer_without_heartbeat(sample, tmp_path):
    httpd, url = start_tracker(ttl=1.0)
    try:
        meta = make_meta(sample, url)
        c = TrackerClient(url, TOKEN, "peer-1", "127.0.0.1", 9001)
        c._request("POST", "/files", meta)
        body = {"peer_id": "peer-1", "host": "127.0.0.1", "port": 9001, "file_id": meta["file_id"],
                "chunks": [0], "event": "started"}
        c._request("POST", "/announce", body)
        assert len(c._request("GET", f"/files/{meta['file_id']}/peers")) == 1
        time.sleep(0.6)
        c._request("POST", "/announce", dict(body, event="update"))     # heartbeat gia hạn lease
        time.sleep(0.6)
        assert len(c._request("GET", f"/files/{meta['file_id']}/peers")) == 1
        time.sleep(2.2)                                                 # ngừng heartbeat
        assert c._request("GET", f"/files/{meta['file_id']}/peers") == []
        assert httpd.state.swarm[meta["file_id"]] == {}                 # reaper đã xóa hẳn
    finally:
        httpd.shutdown()


def test_R4_unresponsive_source_times_out_and_is_retried_elsewhere(sample, tmp_path, tracker_url):
    """Nguồn nhận kết nối nhưng không bao giờ trả lời (message bị mất) -> timeout -> retry nguồn khác."""
    async def go():
        meta = make_meta(sample, tracker_url)
        a = await Node("peer-1", tmp_path, tracker_url, meta).start()
        await a.seed_from(sample)

        async def blackhole(reader, writer):
            await asyncio.sleep(30)
        hole = await asyncio.start_server(blackhole, "127.0.0.1", 0)
        hole_port = hole.sockets[0].getsockname()[1]
        ghost = TrackerClient(tracker_url, TOKEN, "peer-9", "127.0.0.1", hole_port)
        await ghost.announce(meta["file_id"], list(range(21)))

        c = await Node("peer-3", tmp_path, tracker_url, meta).start()
        dl = c.downloader(chunk_timeout=0.3, dead_cooldown=30)
        out = await dl.run()
        hole.close()
        return out, dl.stats
    out, stats = run(go())
    assert out.read_bytes() == sample.read_bytes()
    assert stats["timeouts"] >= 1 and stats["retries"] >= 1
    assert "peer-9" not in stats["by_source"]


def test_R5_tracker_restart_keeps_metadata_and_download_survives_tracker_outage(sample, tmp_path):
    db = tmp_path / "tracker_db.json"
    httpd, url = start_tracker(db_path=db)
    port = httpd.server_address[1]

    async def phase1():
        meta = make_meta(sample, url)
        a = await Node("peer-1", tmp_path, url, meta, upload_delay=0.03).start()
        await a.seed_from(sample)
        c = await Node("peer-3", tmp_path, url, meta).start()
        dl = c.downloader()
        task = asyncio.create_task(dl.run())
        while len(c.store.have) < 3:
            await asyncio.sleep(0.01)
        httpd.shutdown()                       # tracker sập giữa lúc tải
        httpd.server_close()
        out = await task                       # vẫn tải xong nhờ danh sách nguồn đã biết
        return meta, out
    meta, out = run(phase1())
    assert out.read_bytes() == sample.read_bytes()

    httpd2, _ = start_tracker(db_path=db, port=port)    # tracker khởi động lại
    try:
        c = TrackerClient(url, TOKEN, "peer-1", "127.0.0.1", 9001)
        assert c._request("GET", f"/files/{meta['file_id']}") == meta
    finally:
        httpd2.shutdown()

"""F1–F9: kiểm thử chức năng."""
import json
import math
import urllib.request

from conftest import CHUNK, TOKEN, Node, make_meta, run

from common import protocol as P
from common.metadata import chunk_length, load_metadata, save_metadata, sha256_hex, verify_chunk
from peer.downloader import pick_rarest
from peer.storage import ChunkStore


def test_F1_metadata_has_all_fields_and_correct_hashes(sample, tmp_path):
    meta = make_meta(sample)
    data = sample.read_bytes()
    for key in ("file_id", "name", "size", "chunk_size", "num_chunks", "chunk_hashes", "file_hash", "tracker"):
        assert key in meta
    assert meta["size"] == len(data)
    assert meta["num_chunks"] == math.ceil(len(data) / CHUNK) == 21
    assert meta["file_hash"] == sha256_hex(data)
    assert meta["chunk_hashes"][3] == sha256_hex(data[3 * CHUNK:4 * CHUNK])
    assert chunk_length(meta, 20) == CHUNK // 2            # chunk cuối ngắn hơn
    save_metadata(meta, tmp_path / "m.json")
    assert load_metadata(tmp_path / "m.json") == meta


def test_F2_split_then_merge_gives_identical_file(sample, tmp_path):
    meta = make_meta(sample)
    store = ChunkStore.from_seed_file(tmp_path / "seed", meta, sample)
    assert store.is_complete()
    out = store.assemble()
    assert out.read_bytes() == sample.read_bytes()


def test_F3_seeder_registers_file_and_leecher_fetches_metadata(sample, tmp_path, tracker_url):
    async def go():
        meta = make_meta(sample, tracker_url)
        seeder = await Node("peer-1", tmp_path, tracker_url, meta).start()
        await seeder.seed_from(sample)
        again = await seeder.tracker.register_file(meta)          # đăng ký lại: idempotent
        got = await seeder.tracker.get_metadata(meta["file_id"])
        files = await seeder.tracker.list_files()
        return meta, again, got, files
    meta, again, got, files = run(go())
    assert got == meta and again["status"] == "exists"
    assert files[0]["file_id"] == meta["file_id"] and files[0]["peers"] == 1


def test_F4_announce_and_peer_discovery(sample, tmp_path, tracker_url):
    async def go():
        meta = make_meta(sample, tracker_url)
        a = await Node("peer-1", tmp_path, tracker_url, meta).start()
        await a.seed_from(sample)
        b = await Node("peer-2", tmp_path, tracker_url, meta).start()
        await b.give(a.store, [0, 5, 7])
        await b.announce("started")
        seen_by_b = await b.tracker.get_peers(meta["file_id"])
        seen_by_a = await a.tracker.get_peers(meta["file_id"])
        await b.announce("stopped")
        after_stop = await a.tracker.get_peers(meta["file_id"])
        return a, b, seen_by_a, seen_by_b, after_stop
    a, b, seen_by_a, seen_by_b, after_stop = run(go())
    assert [p["peer_id"] for p in seen_by_b] == ["peer-1"] and len(seen_by_b[0]["chunks"]) == 21
    assert seen_by_a[0]["peer_id"] == "peer-2" and seen_by_a[0]["chunks"] == [0, 5, 7]
    assert seen_by_a[0]["port"] == b.port
    assert after_stop == []


def test_F5_fetch_single_chunk_over_tcp(sample, tmp_path, tracker_url):
    async def go():
        meta = make_meta(sample, tracker_url)
        a = await Node("peer-1", tmp_path, tracker_url, meta).start()
        await a.seed_from(sample)
        b = await Node("peer-2", tmp_path, tracker_url, meta).start()
        data = await b.downloader().fetch_chunk({"peer_id": "peer-1", "host": "127.0.0.1", "port": a.port}, 4)
        return meta, data
    meta, data = run(go())
    assert verify_chunk(meta, 4, data)


def test_F6_parallel_download_from_two_peers(sample, tmp_path, tracker_url):
    async def go():
        meta = make_meta(sample, tracker_url)
        a = await Node("peer-1", tmp_path, tracker_url, meta, upload_delay=0.02).start()
        await a.seed_from(sample)
        b = await Node("peer-2", tmp_path, tracker_url, meta, upload_delay=0.02).start()
        await b.give(a.store, range(21))
        await b.announce()
        c = await Node("peer-3", tmp_path, tracker_url, meta).start()
        dl = c.downloader(max_parallel=4, per_peer_limit=2)
        out = await dl.run()
        return out, dl.stats
    out, stats = run(go())
    assert out.read_bytes() == sample.read_bytes()
    assert set(stats["by_source"]) == {"peer-1", "peer-2"}          # tải từ CẢ HAI nguồn
    assert sum(stats["by_source"].values()) == 21


def test_F7_peer_uploads_before_finishing_its_own_download(sample, tmp_path, tracker_url):
    """peer-2 mới có 1 nửa file vẫn phục vụ được peer-3."""
    async def go():
        meta = make_meta(sample, tracker_url)
        a = await Node("peer-1", tmp_path, tracker_url, meta).start()
        await a.seed_from(sample)
        b = await Node("peer-2", tmp_path, tracker_url, meta).start()
        await b.give(a.store, range(0, 10))
        await b.announce()
        assert not b.store.is_complete()
        c = await Node("peer-3", tmp_path, tracker_url, meta).start()
        dl = c.downloader()
        await dl.run()
        return dl.stats, b.uploader.sent_chunks, b.store.is_complete()
    stats, sent_by_partial, b_complete = run(go())
    assert not b_complete and sent_by_partial > 0
    assert stats["by_source"]["peer-2"] == sent_by_partial


def test_F8_rarest_first_picks_least_replicated_chunk():
    peers = [{"peer_id": "a", "chunks": {0, 1, 2, 3}}, {"peer_id": "b", "chunks": {0, 1, 2}},
             {"peer_id": "c", "chunks": {0, 1}}]
    assert pick_rarest({0, 1, 2, 3}, peers, peers) == 3            # chỉ 1 peer có chunk 3
    assert pick_rarest({0, 1, 2}, peers, peers) == 2
    assert pick_rarest({0, 1}, peers, peers) in (0, 1)
    assert pick_rarest({0, 1, 2, 3}, peers, [peers[2]]) in (0, 1)   # chỉ peer c rảnh
    assert pick_rarest({9}, peers, peers) is None


def test_F9_tracker_never_serves_file_content_and_health(sample, tmp_path, tracker_url):
    """Tracker chỉ có API metadata/peer: mọi đường dẫn khác trả 404."""
    async def go():
        meta = make_meta(sample, tracker_url)
        a = await Node("peer-1", tmp_path, tracker_url, meta).start()
        await a.seed_from(sample)
        return meta
    meta = run(go())
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    assert json.loads(opener.open(tracker_url + "/health").read()) == {"status": "ok"}
    for path in (f"/files/{meta['file_id']}/chunks/0", f"/files/{meta['file_id']}/data", "/download"):
        req = urllib.request.Request(tracker_url + path, headers={"X-Peer-Token": TOKEN})
        try:
            opener.open(req)
            raise AssertionError("tracker không được phục vụ dữ liệu file")
        except urllib.error.HTTPError as e:
            assert e.code == 404


def test_protocol_frame_roundtrip():
    import asyncio

    async def go():
        reader = asyncio.StreamReader()

        class W:
            def write(self, b): reader.feed_data(b)
            async def drain(self): pass
        await P.send_frame(W(), {"type": P.CHUNK, "msg_id": "x", "index": 3}, b"abc")
        reader.feed_eof()
        return await P.read_frame(reader)
    h, p = run(go())
    assert h["index"] == 3 and h["payload_len"] == 3 and p == b"abc"

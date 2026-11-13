"""S1–S7: kiểm thử bảo mật và dữ liệu không hợp lệ."""
import asyncio
import json
import os
import struct

import pytest
from conftest import TOKEN, Node, make_meta, run

from common import protocol as P
from common.config import load_dotenv
from common.metadata import MetadataError, validate_metadata
from peer.storage import IntegrityError
from peer.tracker_client import TrackerClient, TrackerError


def test_S1_tracker_rejects_missing_or_wrong_token(sample, tracker_url):
    meta = make_meta(sample, tracker_url)
    for token in ("", "wrong-token"):
        bad = TrackerClient(tracker_url, token, "peer-x", "127.0.0.1", 9001)
        for method, path, body in (("POST", "/files", meta), ("GET", f"/files/{meta['file_id']}", None),
                                   ("GET", "/files", None)):
            with pytest.raises(TrackerError) as e:
                bad._request(method, path, body)
            assert e.value.status == 401


def test_S2_corrupt_chunk_is_rejected_and_sender_is_suspected(sample, tmp_path, tracker_url):
    """peer-2 độc hại luôn gửi chunk hỏng: chunk bị loại, file cuối vẫn đúng và chỉ đến từ peer-1."""
    async def go():
        meta = make_meta(sample, tracker_url)
        a = await Node("peer-1", tmp_path, tracker_url, meta).start()
        await a.seed_from(sample)
        evil = await Node("peer-2", tmp_path, tracker_url, meta, corrupt_rate=1.0).start()
        await evil.give(a.store, range(21))
        await evil.announce()
        c = await Node("peer-3", tmp_path, tracker_url, meta).start()
        dl = c.downloader()
        out = await dl.run()
        with pytest.raises(IntegrityError):
            await a.store.__class__(tmp_path / "x", meta).put(0, b"du lieu gia")
        return out, dl.stats
    out, stats = run(go())
    assert out.read_bytes() == sample.read_bytes()
    assert stats["rejected"] >= 1
    assert "peer-2" not in stats["by_source"]


def test_S3_uploader_drops_malformed_and_oversized_frames(sample, tmp_path, tracker_url):
    async def go():
        meta = make_meta(sample, tracker_url)
        a = await Node("peer-1", tmp_path, tracker_url, meta).start()
        await a.seed_from(sample)

        async def send_raw(raw):
            r, w = await asyncio.open_connection("127.0.0.1", a.port)
            w.write(raw)
            await w.drain()
            data = await asyncio.wait_for(r.read(), 2)       # server phải đóng kết nối
            w.close()
            return data
        huge_header = await send_raw(struct.pack(">I", 10_000_000))
        not_json = await send_raw(struct.pack(">I", 5) + b"hello")
        hdr = json.dumps({"type": "DELETE_ALL"}).encode()
        unknown_type = await send_raw(struct.pack(">I", len(hdr)) + hdr)
        hdr = json.dumps({"type": P.GET_CHUNK, "payload_len": 99_999_999}).encode()
        huge_payload = await send_raw(struct.pack(">I", len(hdr)) + hdr)

        # request hợp lệ về cú pháp nhưng sai nghiệp vụ -> ERROR, không sập
        r, w = await asyncio.open_connection("127.0.0.1", a.port)
        await P.send_frame(w, {"type": P.GET_CHUNK, "msg_id": "m1", "file_id": "../../etc/passwd", "index": 0})
        e1, _ = await P.read_frame(r)
        await P.send_frame(w, {"type": P.GET_CHUNK, "msg_id": "m2", "file_id": meta["file_id"], "index": -5})
        e2, _ = await P.read_frame(r)
        await P.send_frame(w, {"type": P.GET_CHUNK, "msg_id": "m3", "file_id": meta["file_id"], "index": 0})
        ok, payload = await P.read_frame(r)                  # server vẫn phục vụ bình thường
        w.close()
        return huge_header, not_json, unknown_type, huge_payload, e1, e2, ok, len(payload)
    huge_header, not_json, unknown_type, huge_payload, e1, e2, ok, n = run(go())
    assert huge_header == not_json == unknown_type == huge_payload == b""
    assert (e1["type"], e1["code"]) == (P.ERROR, "NOT_FOUND")
    assert (e2["type"], e2["code"]) == (P.ERROR, "NOT_FOUND")
    assert ok["type"] == P.CHUNK and n > 0


def test_S4_invalid_metadata_and_path_traversal_are_rejected(sample, tracker_url):
    meta = make_meta(sample, tracker_url)
    bad_cases = {
        "path traversal": dict(meta, name="../../evil.sh"),
        "windows path": dict(meta, name="C:\\Windows\\x"),
        "hash count": dict(meta, chunk_hashes=meta["chunk_hashes"][:-1]),
        "num_chunks": dict(meta, num_chunks=meta["num_chunks"] + 1),
        "chunk too big": dict(meta, chunk_size=64 * 1024 * 1024),
        "bad hash": dict(meta, file_hash="xyz"),
        "forged file_id": dict(meta, file_id="0" * 16),
        "missing field": {k: v for k, v in meta.items() if k != "file_hash"},
        "wrong type": dict(meta, size="100"),
    }
    client = TrackerClient(tracker_url, TOKEN, "peer-1", "127.0.0.1", 9001)
    for label, bad in bad_cases.items():
        with pytest.raises(MetadataError):
            validate_metadata(bad)
        with pytest.raises(TrackerError) as e:
            client._request("POST", "/files", bad)
        assert e.value.status == 400, label
    with pytest.raises(TrackerError) as e:
        client._request("POST", "/files", ["not", "an", "object"])
    assert e.value.status == 400


def test_S5_peer_id_cannot_be_hijacked_and_bad_announce_is_rejected(sample, tracker_url):
    meta = make_meta(sample, tracker_url)
    real = TrackerClient(tracker_url, TOKEN, "peer-1", "127.0.0.1", 9001)
    real._request("POST", "/files", meta)
    base = {"peer_id": "peer-1", "host": "127.0.0.1", "port": 9001, "file_id": meta["file_id"],
            "chunks": [0, 1], "event": "started"}
    real._request("POST", "/announce", base)

    def status(body):
        with pytest.raises(TrackerError) as e:
            real._request("POST", "/announce", body)
        return e.value.status
    assert status(dict(base, host="10.6.6.6", port=6666)) == 409           # mạo danh peer-1
    real._request("POST", "/announce", dict(base, host="10.6.6.6", event="stopped"))   # giả lệnh rời mạng: bị bỏ qua
    assert status(dict(base, chunks=[0, 999999])) == 400                    # chunk ngoài phạm vi
    assert status(dict(base, peer_id="peer 1; rm -rf /")) == 400
    assert status(dict(base, port=70000)) == 400
    assert status(dict(base, event="hack")) == 400
    assert status(dict(base, file_id="f" * 16)) == 404
    peers = real._request("GET", f"/files/{meta['file_id']}/peers")
    assert [(p["host"], p["port"], p["chunks"]) for p in peers] == [("127.0.0.1", 9001, [0, 1])]


def test_S6_tracker_limits_request_body_size(tracker_url):
    client = TrackerClient(tracker_url, TOKEN, "peer-1", "127.0.0.1", 9001)
    with pytest.raises(TrackerError) as e:
        client._request("POST", "/files", {"junk": "x" * (3 * 1024 * 1024)})
    assert e.value.status in (413, None)       # 413, hoặc server đóng kết nối trước khi client gửi xong


def test_S7_dotenv_strips_inline_comments_but_keeps_quoted_values(tmp_path, monkeypatch):
    """.env.example ghi chú thích cuối dòng; chú thích không được lọt vào giá trị (từng làm tracker không khởi động)."""
    env = tmp_path / ".env"
    env.write_text(
        "# dòng chú thích\n"
        "\n"
        "T_TTL=10              # giây: quá thời gian này thì coi như peer đã rời mạng\n"
        "T_HOST=127.0.0.1\t# địa chỉ quảng bá\n"
        "T_TOKEN=abc#123=xyz\n"
        'T_DQ="a # b"   # chú thích sau dấu nháy\n'
        "T_SQ='c # d'\n"
        "T_EMPTY=\n"
        "T_ONLY_COMMENT=   # để trống\n"
        "T_KEEP=tu-file\n",
        encoding="utf-8")
    names = ("T_TTL", "T_HOST", "T_TOKEN", "T_DQ", "T_SQ", "T_EMPTY", "T_ONLY_COMMENT")
    for name in names:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("T_KEEP", "tu-moi-truong")
    load_dotenv(env)
    got = {name: os.environ[name] for name in names}
    for name in names:                          # load_dotenv ghi thẳng vào os.environ nên phải tự dọn
        monkeypatch.delenv(name)
    assert got == {"T_TTL": "10", "T_HOST": "127.0.0.1", "T_TOKEN": "abc#123=xyz", "T_DQ": "a # b",
                   "T_SQ": "c # d", "T_EMPTY": "", "T_ONLY_COMMENT": ""}
    assert float(got["T_TTL"]) == 10.0
    assert os.environ["T_KEEP"] == "tu-moi-truong"      # biến đã có trong môi trường không bị ghi đè

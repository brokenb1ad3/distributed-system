import asyncio

import pytest

from common import protocol as P


async def _roundtrip(header, payload=b""):
    reader = asyncio.StreamReader()

    class W:
        def write(self, b): reader.feed_data(b)
        async def drain(self): pass

    await P.send_frame(W(), header, payload)
    reader.feed_eof()
    return await P.read_frame(reader)


def test_roundtrip():
    h, p = asyncio.run(_roundtrip({"type": P.CHUNK, "msg_id": "x", "index": 3}, b"abc"))
    assert h["index"] == 3 and h["payload_len"] == 3 and p == b"abc"


def test_reject_unknown_type():
    with pytest.raises(P.ProtocolError):
        asyncio.run(_roundtrip({"type": "HACK"}))


def test_reject_oversize_payload():
    with pytest.raises(P.ProtocolError):
        asyncio.run(_roundtrip({"type": P.CHUNK}, b"x" * (P.MAX_PAYLOAD_SIZE + 1)))

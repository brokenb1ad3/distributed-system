"""[NGƯỜI A] Client gọi API của tracker (dùng httpx.AsyncClient)."""
from __future__ import annotations

import asyncio

import httpx


class TrackerClient:
    def __init__(self, base_url: str, token: str, peer_id: str, host: str, port: int):
        self.base = base_url.rstrip("/")
        self.peer_id, self.host, self.port = peer_id, host, port
        self.http = httpx.AsyncClient(headers={"X-Peer-Token": token}, timeout=5.0)

    async def register_file(self, meta: dict) -> None:
        r = await self.http.post(f"{self.base}/files", json=meta)
        r.raise_for_status()

    async def get_metadata(self, file_id: str) -> dict:
        r = await self.http.get(f"{self.base}/files/{file_id}")
        r.raise_for_status()
        return r.json()

    async def announce(self, file_id: str, chunks: list[int], event: str = "update") -> int:
        r = await self.http.post(f"{self.base}/announce", json={
            "peer_id": self.peer_id, "host": self.host, "port": self.port,
            "file_id": file_id, "chunks": sorted(chunks), "event": event})
        r.raise_for_status()
        return r.json().get("interval", 5)

    async def get_peers(self, file_id: str) -> list[dict]:
        r = await self.http.get(f"{self.base}/files/{file_id}/peers")
        r.raise_for_status()
        return [p for p in r.json() if p["peer_id"] != self.peer_id]

    async def announce_loop(self, file_id: str, store) -> None:
        """Heartbeat: announce định kỳ. TODO(A): tracker không trả lời thì retry, không làm crash peer."""
        while True:
            try:
                interval = await self.announce(file_id, list(store.have))
            except httpx.HTTPError:
                interval = 2
            await asyncio.sleep(interval)

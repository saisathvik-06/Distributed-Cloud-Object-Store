"""HTTP calls from the controller to workers."""
import httpx

from . import config

CHECKSUM_HEADER = "X-Chunk-Checksum"


class WorkerError(Exception):
    """The worker could not be reached or answered unexpectedly."""


class ChunkMissing(WorkerError):
    """The worker answered, and it does not have the chunk."""


class ChunkCorruptOnWorker(WorkerError):
    """The worker's own read check found the chunk damaged."""


class WorkerClient:
    def __init__(self):
        # retries=1: one retry on a fresh connection, which covers stale pooled
        # connections to a worker that has just restarted.
        self._http = httpx.AsyncClient(
            transport=httpx.AsyncHTTPTransport(retries=1),
            timeout=httpx.Timeout(config.WORKER_TRANSFER_TIMEOUT_S, connect=config.WORKER_CONNECT_TIMEOUT_S),
        )

    async def close(self) -> None:
        await self._http.aclose()

    async def put_chunk(self, address: str, chunk_id: str, data: bytes, checksum: str) -> None:
        try:
            resp = await self._http.put(
                f"{address}/internal/chunks/{chunk_id}",
                content=data,
                headers={CHECKSUM_HEADER: checksum, "Content-Type": "application/octet-stream"},
            )
        except httpx.HTTPError as exc:
            raise WorkerError(f"{address}: {type(exc).__name__}") from exc
        if resp.status_code not in (200, 201):
            raise WorkerError(f"{address}: PUT returned {resp.status_code}")
        if resp.json().get("checksum") != checksum:
            raise WorkerError(f"{address}: stored checksum differs from the one sent")

    async def get_chunk(self, address: str, chunk_id: str) -> bytes:
        try:
            resp = await self._http.get(f"{address}/internal/chunks/{chunk_id}")
        except httpx.HTTPError as exc:
            raise WorkerError(f"{address}: {type(exc).__name__}") from exc
        if resp.status_code == 200:
            return resp.content
        if resp.status_code == 404:
            raise ChunkMissing(chunk_id)
        code = None
        try:
            code = resp.json().get("error", {}).get("code")
        except ValueError:
            pass
        if code == "CHUNK_CORRUPT":
            raise ChunkCorruptOnWorker(chunk_id)
        raise WorkerError(f"{address}: GET returned {resp.status_code}")

    async def delete_chunk(self, address: str, chunk_id: str) -> bool:
        """Best effort. True when the chunk is gone from the worker."""
        try:
            resp = await self._http.delete(f"{address}/internal/chunks/{chunk_id}")
        except httpx.HTTPError:
            return False
        return resp.status_code in (204, 404)

    async def list_chunks(self, address: str, worker_id: str) -> list[dict]:
        try:
            resp = await self._http.get(f"{address}/internal/workers/{worker_id}/chunks")
        except httpx.HTTPError as exc:
            raise WorkerError(f"{address}: {type(exc).__name__}") from exc
        if resp.status_code != 200:
            raise WorkerError(f"{address}: inventory returned {resp.status_code}")
        return resp.json()["chunks"]

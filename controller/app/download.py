"""Verified reads.

Invariant: the controller never hands out bytes that fail the SHA-256 check
against its own metadata. A bad or unreachable copy is skipped and the other
replica is used; bad copies are flagged for the repair loop.
"""
import tempfile
import hashlib
import logging
import random

from .context import Context
from .errors import ApiError, not_found
from .repository import run_db
from .worker_errors import ChunkCorruptOnWorker, ChunkMissing, WorkerError

log = logging.getLogger("controller.download")


class NoValidReplica(Exception):
    pass


async def fetch_verified_chunk(ctx: Context, chunk: dict, only_workers=None) -> tuple[bytes, str]:
    """Return (data, worker_id) from the first replica that passes verification."""
    candidates = [
        r["worker_id"] for r in chunk["replicas"]
        if r["state"] == "HEALTHY" and ctx.membership.is_alive(r["worker_id"])
        and (only_workers is None or r["worker_id"] in only_workers)
    ]
    random.shuffle(candidates)
    chunk_id = chunk["chunk_id"]
    for worker_id in candidates:
        address = ctx.membership.address_of(worker_id)
        try:
            data = await ctx.workers.get_chunk(address, chunk_id)
        except ChunkCorruptOnWorker:
            await _mark_corrupt(ctx, chunk_id, worker_id, "worker's own check failed")
            continue
        except ChunkMissing:
            log.error("REPLICA_MISSING chunk %s is gone from %s; it will be re-created", chunk_id, worker_id)
            await run_db(ctx.repo.remove_replica, chunk_id, worker_id)
            ctx.repair.wake()
            continue
        except WorkerError as exc:
            log.warning("chunk %s: %s unreachable (%s), trying another copy", chunk_id, worker_id, exc)
            continue
        if len(data) != chunk["size"] or hashlib.sha256(data).hexdigest() != chunk["checksum"]:
            await _mark_corrupt(ctx, chunk_id, worker_id, "SHA-256 mismatch at controller")
            continue
        return data, worker_id
    raise NoValidReplica(chunk_id)


async def _mark_corrupt(ctx: Context, chunk_id: str, worker_id: str, reason: str) -> None:
    log.error("CORRUPTION_DETECTED chunk %s on %s (%s); using another copy and scheduling repair",
              chunk_id, worker_id, reason)
    await run_db(ctx.repo.set_replica_state, chunk_id, worker_id, "CORRUPT")
    ctx.repair.wake()


def _unavailable(obj: dict, index: int) -> ApiError:
    return ApiError(
        503, "DATA_UNAVAILABLE",
        f"Chunk {index} of object {obj['object_id']} has no valid copy on an available worker",
    )


class PreparedDownload:
    def __init__(self, spool):
        self.spool = spool

    async def close(self):
        await run_db(self.spool.close)

    async def body(self):
        try:
            while True:
                block = await run_db(self.spool.read, 1024 * 1024)
                if not block:
                    break
                yield block
        finally:
            await self.close()


async def open_download(ctx: Context, obj: dict) -> PreparedDownload:
    """Verify the complete object to temporary disk before sending HTTP headers.

    Memory stays bounded by chunk size. Deletion waits until the snapshot is
    complete, then may proceed while the independent spool is sent to the client.
    """
    spool = tempfile.TemporaryFile(mode="w+b")
    try:
        async with ctx.object_lock(obj["object_id"]):
            current = await run_db(ctx.repo.get_object, obj["object_id"])
            if current is None:
                raise not_found(obj["object_id"])
            chunks = await run_db(ctx.repo.get_chunks, obj["object_id"])
            if len(chunks) != current["chunk_count"]:
                raise _unavailable(current, 0)
            digest = hashlib.sha256()
            total = 0
            for index, chunk in enumerate(chunks):
                if chunk["chunk_index"] != index:
                    raise _unavailable(current, index)
                try:
                    data, _ = await fetch_verified_chunk(ctx, chunk)
                except NoValidReplica:
                    raise _unavailable(current, index) from None
                digest.update(data)
                total += len(data)
                await run_db(spool.write, data)
            if total != current["size"] or digest.hexdigest() != current["checksum"]:
                raise ApiError(503, "DATA_UNAVAILABLE", "Object metadata or checksum is inconsistent")
            await run_db(spool.seek, 0)
        return PreparedDownload(spool)
    except BaseException:
        spool.close()
        raise

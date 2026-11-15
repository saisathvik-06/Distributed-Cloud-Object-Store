"""Verified reads.

Invariant: the controller never hands out bytes that fail the SHA-256 check
against its own metadata. A bad or unreachable copy is skipped and the other
replica is used; bad copies are flagged for the repair loop.
"""
import hashlib
import logging
import random

from .context import Context
from .errors import ApiError
from .repository import run_db
from .worker_client import ChunkCorruptOnWorker, ChunkMissing, WorkerError

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


async def open_download(ctx: Context, obj: dict):
    """Prepare a download. Returns an async byte iterator.

    Anything that can be known before the response starts is checked here, so
    it can still be reported with a clean 503:
      * every chunk has at least one healthy copy on an alive worker;
      * chunk 0 is fetched and verified before any header is sent.
    """
    chunks = await run_db(ctx.repo.get_chunks, obj["object_id"])
    for chunk in chunks:
        if not any(r["state"] == "HEALTHY" and ctx.membership.is_alive(r["worker_id"]) for r in chunk["replicas"]):
            raise _unavailable(obj, chunk["chunk_index"])
    first = None
    if chunks:
        try:
            first, _ = await fetch_verified_chunk(ctx, chunks[0])
        except NoValidReplica:
            raise _unavailable(obj, 0) from None

    async def body():
        if first is not None:
            yield first
        for chunk in chunks[1:]:
            try:
                data, _ = await fetch_verified_chunk(ctx, chunk)
            except NoValidReplica:
                # Headers are already sent, so the status cannot change. Abort the
                # connection: the client sees fewer bytes than Content-Length.
                log.error("DOWNLOAD_ABORTED object %s: chunk %s has no valid copy",
                          obj["object_id"], chunk["chunk_id"])
                raise RuntimeError("no valid replica while streaming") from None
            yield data

    return body()

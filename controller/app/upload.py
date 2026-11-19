"""Upload pipeline: cut the object into chunks and store each chunk twice."""
import asyncio
import hashlib
import logging
import uuid
from dataclasses import dataclass

from . import config
from .context import Context
from .deletion import purge_object
from .errors import ApiError
from .placement import choose_workers
from .repository import run_db
from .worker_errors import WorkerError

log = logging.getLogger("controller.upload")


@dataclass
class StoredObject:
    object_id: str
    metadata: dict
    chunks: list[dict]


def chunk_id_for(object_id: str, index: int) -> str:
    return f"{object_id}-{index:05d}"


async def _read_slice(file, size: int) -> bytes:
    """Read exactly `size` bytes unless the stream ends first."""
    parts = []
    remaining = size
    while remaining > 0:
        part = await file.read(remaining)
        if not part:
            break
        parts.append(part)
        remaining -= len(part)
    return b"".join(parts)


async def _store_chunk(ctx: Context, chunk_id: str, data: bytes, checksum: str, load: dict[str, int]) -> list[str]:
    """Write one chunk to REPLICATION_FACTOR different workers. Returns their IDs."""
    confirmed: list[str] = []
    tried: set[str] = set()
    while len(confirmed) < config.REPLICATION_FACTOR:
        candidates = [w for w in ctx.membership.alive_ids() if w not in tried]
        targets = choose_workers(candidates, load, config.REPLICATION_FACTOR - len(confirmed))
        if not targets:
            raise ApiError(
                503, "STORAGE_UNAVAILABLE",
                f"Could not store {config.REPLICATION_FACTOR} copies of a chunk: not enough workers available",
            )
        tried.update(targets)
        results = await asyncio.gather(
            *(ctx.workers.put_chunk(ctx.membership.address_of(w), chunk_id, data, checksum) for w in targets),
            return_exceptions=True,
        )
        for worker_id, result in zip(targets, results):
            if isinstance(result, WorkerError):
                log.warning("chunk %s: write to %s failed (%s), trying another worker", chunk_id, worker_id, result)
            elif isinstance(result, BaseException):
                raise result
            else:
                confirmed.append(worker_id)
                load[worker_id] = load.get(worker_id, 0) + 1
    return confirmed


async def store_object(ctx: Context, file, name: str, content_type: str, custom_metadata: dict) -> StoredObject:
    """Store an uploaded file and capture its creation response under the lifecycle lock.

    The object stays invisible (state UPLOADING) until every chunk has its
    copies confirmed; one transaction then commits it.
    """
    if len(ctx.membership.alive_ids()) < config.REPLICATION_FACTOR:
        raise ApiError(
            503, "STORAGE_UNAVAILABLE",
            f"At least {config.REPLICATION_FACTOR} storage workers must be available to store an object",
        )

    object_id = str(uuid.uuid4())
    async with ctx.object_lock(object_id):
        try:
            await run_db(ctx.repo.create_object, object_id, name, content_type, custom_metadata)
            load = await run_db(ctx.repo.replica_counts_by_worker)
            object_hash = hashlib.sha256()
            total = 0
            index = 0
            while True:
                data = await _read_slice(file, config.CHUNK_SIZE)   # one chunk in memory at a time
                if not data:
                    break
                total += len(data)
                if config.MAX_OBJECT_SIZE and total > config.MAX_OBJECT_SIZE:
                    raise ApiError(400, "OBJECT_TOO_LARGE", f"Object exceeds the {config.MAX_OBJECT_SIZE} byte limit")
                object_hash.update(data)
                checksum = hashlib.sha256(data).hexdigest()
                chunk_id = chunk_id_for(object_id, index)
                # The chunk row is written before any data is sent, so reconciliation
                # never mistakes an in-flight chunk for garbage.
                await run_db(ctx.repo.add_chunk, chunk_id, object_id, index, len(data), checksum)
                for worker_id in await _store_chunk(ctx, chunk_id, data, checksum, load):
                    await run_db(ctx.repo.add_replica, chunk_id, worker_id)
                index += 1
            await run_db(ctx.repo.commit_object, object_id, total, index, object_hash.hexdigest())
            result = StoredObject(object_id, await run_db(ctx.repo.get_object, object_id),
                                  await run_db(ctx.repo.get_chunks, object_id))
        except BaseException:
            log.warning("UPLOAD_ABORTED object %s: removing what was written", object_id)
            await asyncio.shield(purge_object(ctx, object_id, lock_held=True))
            raise
    log.info("OBJECT_STORED %s name=%r size=%d chunks=%d", object_id, name, total, index)
    return result

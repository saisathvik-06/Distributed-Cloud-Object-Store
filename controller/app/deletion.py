"""Physical removal of an object's chunks followed by removal of its metadata."""
import asyncio
import logging

from .context import Context
from .repository import run_db

log = logging.getLogger("controller.deletion")


async def purge_object(ctx: Context, object_id: str, every_alive_worker: bool = False) -> None:
    """Delete an object's chunk files from alive workers, then drop its rows.

    Copies on DEAD workers cannot be reached now; they become orphans and are
    removed by reconciliation when that worker registers again.

    `every_alive_worker` is for abandoned uploads, where a chunk may have been
    written to a worker before its replica row was recorded.
    """
    chunks = await run_db(ctx.repo.get_chunks, object_id)
    alive = ctx.membership.alive_ids()
    limit = asyncio.Semaphore(8)

    async def remove(worker_id: str, chunk_id: str) -> None:
        address = ctx.membership.address_of(worker_id)
        if address is None:
            return
        async with limit:
            if not await ctx.workers.delete_chunk(address, chunk_id):
                log.warning("could not delete chunk %s from %s; reconciliation will remove it", chunk_id, worker_id)

    jobs = []
    for chunk in chunks:
        holders = alive if every_alive_worker else [
            r["worker_id"] for r in chunk["replicas"] if r["worker_id"] in alive
        ]
        jobs.extend(remove(worker_id, chunk["chunk_id"]) for worker_id in holders)
    if jobs:
        await asyncio.gather(*jobs)
    await run_db(ctx.repo.remove_object, object_id)

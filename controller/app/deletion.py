"""Durable, retryable physical deletion, serialized with upload and repair."""
import asyncio
import logging

from . import config
from .context import Context
from .repository import run_db

log = logging.getLogger("controller.deletion")


async def retry_deletions(ctx: Context) -> None:
    pending = await run_db(ctx.repo.pending_deletions)
    limit = asyncio.Semaphore(8)

    async def remove(item):
        worker_id, chunk_id = item["worker_id"], item["chunk_id"]
        if not ctx.membership.is_alive(worker_id):
            return
        async with limit:
            try:
                done = await ctx.workers.delete_chunk(ctx.membership.address_of(worker_id), chunk_id)
                if done:
                    await run_db(ctx.repo.finish_chunk_deletion, chunk_id, worker_id)
            except Exception:
                log.exception("deletion postponed: %s on %s", chunk_id, worker_id)

    await asyncio.gather(*(remove(item) for item in pending))


async def purge_object(ctx: Context, object_id: str, *, lock_held: bool = False) -> None:
    # Include ALL registered workers: a PUT may have succeeded before its reply
    # or replica record was lost. Queue entries survive removal of object rows.
    if lock_held:
        await run_db(ctx.repo.queue_object_deletion, object_id)
    else:
        async with ctx.object_lock(object_id):
            await run_db(ctx.repo.queue_object_deletion, object_id)
    await retry_deletions(ctx)


async def run_cleanup(ctx: Context) -> None:
    while True:
        try:
            await retry_deletions(ctx)
        except Exception:
            log.exception("cleanup pass failed; will retry")
        await asyncio.sleep(config.DELETE_RETRY_INTERVAL_S)

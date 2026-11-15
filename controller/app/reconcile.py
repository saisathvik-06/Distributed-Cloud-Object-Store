"""Inventory reconciliation, run every time a worker registers.

The worker is the only source of truth about what is on its disk; the
controller is the only source of truth about what should be there.
"""
import asyncio
import logging
import time

from . import config
from .context import Context
from .repository import run_db
from .worker_client import WorkerError

log = logging.getLogger("controller.reconcile")


async def reconcile_worker(ctx: Context, worker_id: str) -> None:
    address = ctx.membership.address_of(worker_id)
    # Replica rows are read BEFORE the inventory: a chunk written in between is
    # then in neither list and is left alone, instead of looking "lost".
    recorded = await run_db(ctx.repo.replicas_for_worker, worker_id)
    inventory = None
    for _ in range(5):
        try:
            inventory = await ctx.workers.list_chunks(address, worker_id)
            break
        except WorkerError:
            await asyncio.sleep(1.0)   # the worker may still be starting its server
    if inventory is None:
        log.warning("reconciliation of %s skipped: inventory not reachable", worker_id)
        return

    reported = {c["chunk_id"]: c for c in inventory}
    known = await run_db(ctx.repo.chunk_checksums, list(reported))
    now = time.time()
    orphans = adopted = lost = 0

    for chunk_id, item in reported.items():
        if chunk_id not in known:
            # Unknown to the controller: its object was deleted or its upload abandoned.
            if now - float(item.get("stored_at") or 0) < config.ORPHAN_MIN_AGE_S:
                continue
            if await ctx.workers.delete_chunk(address, chunk_id):
                orphans += 1
        elif chunk_id not in recorded:
            if item.get("checksum") == known[chunk_id]:
                await run_db(ctx.repo.add_replica, chunk_id, worker_id)
                adopted += 1
            else:
                await ctx.workers.delete_chunk(address, chunk_id)

    for chunk_id in recorded:
        if chunk_id not in reported:
            await run_db(ctx.repo.remove_replica, chunk_id, worker_id)
            lost += 1

    log.info(
        "RECONCILED %s: %d chunks on disk, %d orphans deleted, %d copies adopted, %d recorded copies missing",
        worker_id, len(reported), orphans, adopted, lost,
    )
    ctx.repair.wake()

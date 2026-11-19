"""Periodic inventory reconciliation; verify disk bytes before adopting replicas."""
import asyncio
import hashlib
import logging
import time

from . import config
from .context import Context
from .repository import run_db
from .worker_errors import ChunkCorruptOnWorker, ChunkMissing, WorkerError

log = logging.getLogger("controller.reconcile")


async def reconcile_worker(ctx: Context, worker_id: str) -> None:
    async with ctx.worker_lock(worker_id):
        if not ctx.membership.is_alive(worker_id):
            return
        address = ctx.membership.address_of(worker_id)
        recorded = await run_db(ctx.repo.replicas_for_worker, worker_id)
        try:
            inventory = await ctx.workers.list_chunks(address, worker_id)
        except WorkerError:
            return  # periodic pass retries even when startup registration was too early
        reported = {c["chunk_id"]: c for c in inventory}
        for cid in set(recorded) | set(reported):
            chunk = await run_db(ctx.repo.get_chunk, cid)
            object_id = chunk["object_id"] if chunk else cid.rsplit("-", 1)[0]
            async with ctx.object_lock(object_id):
                # Re-read after acquiring the lock: the upload/delete/repair may have completed.
                chunk = await run_db(ctx.repo.get_chunk, cid)
                if chunk is None:
                    item = reported.get(cid)
                    if item and time.time() - float(item.get("stored_at") or 0) >= config.ORPHAN_MIN_AGE_S:
                        await ctx.workers.delete_chunk(address, cid)
                    continue
                if chunk["object_state"] != "COMMITTED":
                    continue
                try:
                    # Inventory snapshots and sidecars are not proof of current disk contents.
                    data = await ctx.workers.get_chunk(address, cid)
                    if len(data) != chunk["size"] or hashlib.sha256(data).hexdigest() != chunk["checksum"]:
                        raise ChunkCorruptOnWorker(cid)
                except ChunkMissing:
                    await run_db(ctx.repo.remove_replica, cid, worker_id)
                except ChunkCorruptOnWorker:
                    await run_db(ctx.repo.add_replica, cid, worker_id)
                    await run_db(ctx.repo.set_replica_state, cid, worker_id, "CORRUPT")
                    log.error("CORRUPTION_DETECTED %s on %s during reconciliation", cid, worker_id)
                except WorkerError:
                    continue
                else:
                    await run_db(ctx.repo.add_replica, cid, worker_id)
        ctx.repair.wake()


async def run_reconciliation(ctx: Context) -> None:
    while True:
        await asyncio.sleep(config.RECONCILE_INTERVAL_S)
        for worker_id in ctx.membership.alive_ids():
            try:
                await reconcile_worker(ctx, worker_id)
            except Exception:
                log.exception("reconciliation of %s failed; will retry", worker_id)

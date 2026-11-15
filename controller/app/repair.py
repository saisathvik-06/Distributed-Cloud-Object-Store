"""Replication manager: one background task that keeps every chunk at the
required number of healthy copies.

Per chunk of a committed object it
  * repairs a CORRUPT copy in place from a healthy one,
  * re-creates copies lost with a dead worker (after the grace period),
  * removes surplus copies (for example after a dead worker returns).
"""
import asyncio
import logging
import time

from . import config
from .context import Context
from .download import NoValidReplica, fetch_verified_chunk
from .placement import choose_workers
from .repository import run_db
from .worker_client import WorkerError

log = logging.getLogger("controller.repair")


class RepairManager:
    def __init__(self, ctx: Context):
        self.ctx = ctx
        self._wake = asyncio.Event()
        self._unavailable: set[str] = set()   # chunks already reported, to avoid log spam

    def wake(self) -> None:
        self._wake.set()

    async def run(self) -> None:
        while True:
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=config.REPAIR_INTERVAL_S)
            except asyncio.TimeoutError:
                pass
            self._wake.clear()
            if time.monotonic() - self.ctx.membership.started_at < config.STARTUP_GRACE_S:
                await asyncio.sleep(1.0)   # workers are still re-registering after a controller start
                continue
            try:
                await self.run_once()
            except Exception:
                log.exception("repair pass failed; will retry")

    async def run_once(self) -> None:
        ctx = self.ctx
        chunks = await run_db(ctx.repo.committed_chunks_with_replicas)
        load = await run_db(ctx.repo.replica_counts_by_worker)
        for chunk in chunks:
            await self._repair_chunk(chunk, load)

    async def _repair_chunk(self, chunk: dict, load: dict[str, int]) -> None:
        ctx = self.ctx
        rf = config.REPLICATION_FACTOR
        chunk_id = chunk["chunk_id"]
        alive = set(ctx.membership.alive_ids())
        healthy = [r["worker_id"] for r in chunk["replicas"] if r["state"] == "HEALTHY" and r["worker_id"] in alive]
        corrupt = [r["worker_id"] for r in chunk["replicas"] if r["state"] == "CORRUPT" and r["worker_id"] in alive]
        # Healthy copies on workers that died moments ago are still expected back.
        awaited = [r["worker_id"] for r in chunk["replicas"]
                   if r["state"] == "HEALTHY" and ctx.membership.in_grace(r["worker_id"])]

        if not healthy:
            if chunk_id not in self._unavailable:
                self._unavailable.add(chunk_id)
                log.error("CHUNK_UNAVAILABLE %s has no healthy copy on an alive worker; nothing to repair from", chunk_id)
            return
        self._unavailable.discard(chunk_id)

        # 1. Corrupt copies: overwrite in place, or simply drop them if they are surplus.
        for worker_id in corrupt:
            if len(healthy) >= rf:
                if await ctx.workers.delete_chunk(ctx.membership.address_of(worker_id), chunk_id):
                    await run_db(ctx.repo.remove_replica, chunk_id, worker_id)
                    load[worker_id] = max(0, load.get(worker_id, 0) - 1)
                    log.info("CORRUPT_COPY_REMOVED chunk %s on %s (enough healthy copies exist)", chunk_id, worker_id)
            elif await self._copy(chunk, healthy, worker_id, "REPAIRED_IN_PLACE"):
                healthy.append(worker_id)

        # 2. Under-replication: copy to workers that do not hold the chunk yet.
        missing = rf - len(healthy) - len(awaited)
        if missing > 0:
            holders = set(healthy) | set(corrupt)
            targets = choose_workers([w for w in alive if w not in holders], load, missing)
            for worker_id in targets:   # empty when too few workers are alive; retried next pass
                if await self._copy(chunk, healthy, worker_id, "RE_REPLICATED"):
                    healthy.append(worker_id)
                    load[worker_id] = load.get(worker_id, 0) + 1

        # 3. Over-replication: trim down to exactly rf, from the most loaded workers.
        while len(healthy) > rf:
            worker_id = max(healthy, key=lambda w: load.get(w, 0))
            if not await ctx.workers.delete_chunk(ctx.membership.address_of(worker_id), chunk_id):
                break
            await run_db(ctx.repo.remove_replica, chunk_id, worker_id)
            healthy.remove(worker_id)
            load[worker_id] = max(0, load.get(worker_id, 0) - 1)
            log.info("SURPLUS_COPY_REMOVED chunk %s from %s (now %d copies)", chunk_id, worker_id, len(healthy))

    async def _copy(self, chunk: dict, sources: list[str], target: str, event: str) -> bool:
        """Copy a chunk from a verified healthy source to `target`."""
        ctx = self.ctx
        chunk_id = chunk["chunk_id"]
        try:
            data, source = await fetch_verified_chunk(ctx, chunk, only_workers=set(sources))
        except NoValidReplica:
            log.error("repair of chunk %s postponed: no source passed verification", chunk_id)
            return False
        address = ctx.membership.address_of(target)
        try:
            await ctx.workers.put_chunk(address, chunk_id, data, chunk["checksum"])
        except WorkerError as exc:
            log.warning("repair of chunk %s: write to %s failed (%s)", chunk_id, target, exc)
            return False
        # The object may have been deleted while we were copying.
        if not await run_db(ctx.repo.add_replica, chunk_id, target):
            await ctx.workers.delete_chunk(address, chunk_id)
            return False
        log.info("%s chunk %s: %s -> %s", event, chunk_id, source, target)
        return True

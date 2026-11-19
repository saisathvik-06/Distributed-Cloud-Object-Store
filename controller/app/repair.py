"""Verify bytes, repair missing/corrupt copies, and safely prune surplus copies."""
import asyncio
import hashlib
import logging
import time

from . import config
from .context import Context
from .placement import choose_workers
from .repository import run_db
from .worker_errors import ChunkCorruptOnWorker, ChunkMissing, WorkerError

log = logging.getLogger("controller.repair")


class RepairManager:
    def __init__(self, ctx: Context):
        self.ctx = ctx
        self._wake = asyncio.Event()
        self._pass_lock = asyncio.Lock()
        self._last_scrub = float("-inf")

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
                continue
            try:
                await self.run_once()
            except Exception:
                log.exception("repair pass failed; will retry")

    async def run_once(self, *, scrub: bool = False) -> None:
        async with self._pass_lock:
            scrub = scrub or time.monotonic() - self._last_scrub >= config.SCRUB_INTERVAL_S
            chunks = await run_db(self.ctx.repo.committed_chunks_with_replicas)
            load = await run_db(self.ctx.repo.replica_counts_by_worker)
            for snapshot in chunks:
                async with self.ctx.object_lock(snapshot["object_id"]):
                    chunk = await run_db(self.ctx.repo.get_chunk, snapshot["chunk_id"])
                    if chunk is None or chunk["object_state"] != "COMMITTED":
                        continue
                    live = [r for r in chunk["replicas"] if self.ctx.membership.is_alive(r["worker_id"])]
                    if scrub or len(live) != config.REPLICATION_FACTOR or any(r["state"] != "HEALTHY" for r in live):
                        await self._repair_chunk(chunk, load)
            if scrub:
                self._last_scrub = time.monotonic()

    async def _repair_chunk(self, chunk: dict, load: dict[str, int]) -> None:
        """Caller holds the object lock. Destructive decisions never trust health flags alone."""
        ctx, cid = self.ctx, chunk["chunk_id"]
        valid = []
        source = None
        for rep in chunk["replicas"]:
            wid = rep["worker_id"]
            if not ctx.membership.is_alive(wid):
                continue
            try:
                data = await ctx.workers.get_chunk(ctx.membership.address_of(wid), cid)
                if len(data) != chunk["size"] or hashlib.sha256(data).hexdigest() != chunk["checksum"]:
                    raise ChunkCorruptOnWorker(cid)
            except ChunkMissing:
                await run_db(ctx.repo.remove_replica, cid, wid)
                log.warning("REPLICA_MISSING %s on %s", cid, wid)
            except ChunkCorruptOnWorker:
                await run_db(ctx.repo.set_replica_state, cid, wid, "CORRUPT")
                log.error("CORRUPTION_DETECTED %s on %s", cid, wid)
            except WorkerError:
                continue
            else:
                valid.append(wid)
                source = data
                await run_db(ctx.repo.set_replica_state, cid, wid, "HEALTHY")

        if source is None:
            log.error("CHUNK_UNAVAILABLE %s: no verified source; retaining all copies", cid)
            return

        awaited = sum(1 for r in chunk["replicas"] if r["state"] == "HEALTHY"
                      and ctx.membership.in_grace(r["worker_id"]))
        required = max(1, config.REPLICATION_FACTOR - awaited)
        targets = choose_workers([w for w in ctx.membership.alive_ids() if w not in valid], load,
                                 len(ctx.membership.alive_ids()))
        for wid in targets:
            if len(valid) >= required:
                break
            address = ctx.membership.address_of(wid)
            try:
                await ctx.workers.put_chunk(address, cid, source, chunk["checksum"])
            except WorkerError:
                continue
            if not await run_db(ctx.repo.add_replica, cid, wid):
                await ctx.workers.delete_chunk(address, cid)
                return
            valid.append(wid)
            load[wid] = load.get(wid, 0) + 1
            log.info("REPAIRED %s on %s", cid, wid)

        # At least two OTHER copies have passed verification before any prune.
        while len(valid) > config.REPLICATION_FACTOR:
            wid = max(valid, key=lambda w: load.get(w, 0))
            if not await ctx.workers.delete_chunk(ctx.membership.address_of(wid), cid):
                break
            await run_db(ctx.repo.remove_replica, cid, wid)
            valid.remove(wid)
            load[wid] = max(0, load.get(wid, 0) - 1)
            log.info("SURPLUS_COPY_REMOVED %s on %s", cid, wid)

        if len(valid) >= config.REPLICATION_FACTOR:
            current = await run_db(ctx.repo.get_chunk, cid)
            for rep in current["replicas"] if current else []:
                wid = rep["worker_id"]
                if wid not in valid and rep["state"] == "CORRUPT" and ctx.membership.is_alive(wid):
                    if await ctx.workers.delete_chunk(ctx.membership.address_of(wid), cid):
                        await run_db(ctx.repo.remove_replica, cid, wid)

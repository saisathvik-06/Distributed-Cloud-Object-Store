"""Registration and heartbeat loop.

The worker registers at startup (retrying until the controller answers) and
then heartbeats. It never announces a shutdown: a stopped worker must look like
a real failure so that the controller's failure detector is what notices.
"""
import asyncio
import logging

import httpx
from starlette.concurrency import run_in_threadpool

from . import config
from .storage import ChunkStore

log = logging.getLogger("worker.registration")


async def _register(client: httpx.AsyncClient, store: ChunkStore) -> float:
    """Register with the controller, retrying forever. Returns the heartbeat interval."""
    delay = 1.0
    while True:
        try:
            stats = await run_in_threadpool(store.stats)
            resp = await client.post(
                f"{config.CONTROLLER_URL}/internal/workers/register",
                json={"worker_id": config.WORKER_ID, "address": config.ADVERTISE_URL, **stats},
            )
            if resp.status_code == 200:
                interval = float(resp.json().get("heartbeat_interval_s", 3))
                log.info("REGISTERED with controller, heartbeat every %ss", interval)
                return interval
            log.warning("registration rejected: %s %s", resp.status_code, resp.text[:200])
        except httpx.HTTPError as exc:
            log.warning("controller not reachable (%s), retrying in %.0fs", type(exc).__name__, delay)
        await asyncio.sleep(delay)
        delay = min(delay * 2, 5.0)


async def run(store: ChunkStore) -> None:
    async with httpx.AsyncClient(timeout=httpx.Timeout(5.0, connect=2.0)) as client:
        interval = await _register(client, store)
        while True:
            await asyncio.sleep(interval)
            try:
                stats = await run_in_threadpool(store.stats)
                resp = await client.post(
                    f"{config.CONTROLLER_URL}/internal/workers/heartbeat",
                    json={"worker_id": config.WORKER_ID, **stats},
                )
                if resp.status_code == 404:
                    # The controller restarted or declared us dead: join again.
                    log.info("controller does not recognise this worker, registering again")
                    interval = await _register(client, store)
            except httpx.HTTPError as exc:
                log.warning("heartbeat failed (%s)", type(exc).__name__)

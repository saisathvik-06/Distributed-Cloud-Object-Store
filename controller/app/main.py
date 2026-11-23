"""Controller: the only component clients talk to.

Runs as exactly one process, because membership state and the background
tasks (failure detector, repair loop) live in memory.
"""
import asyncio
import logging
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request

from . import api_admin, api_internal, api_objects, config, db, errors
from .context import Context
from .deletion import purge_object, run_cleanup
from .reconcile import run_reconciliation
from .membership import Membership
from .repair import RepairManager
from .repository import Repository, run_db
from .worker_client import WorkerClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)
log = logging.getLogger("controller")


async def _finish_interrupted_work(ctx: Context, uploading: list[str], deleting: list[str]) -> None:
    """After a controller restart: purge half-finished uploads and finish half-finished deletions.

    Waits for the startup grace so that workers have registered again first.
    """
    await asyncio.sleep(config.STARTUP_GRACE_S)
    for object_id in uploading:
        log.info("RECOVERY purging interrupted upload %s", object_id)
        await purge_object(ctx, object_id)
    for object_id in deleting:
        log.info("RECOVERY finishing interrupted deletion of %s", object_id)
        await purge_object(ctx, object_id)


@asynccontextmanager
async def lifespan(app: FastAPI):
    repo = Repository(db.connect(config.DB_PATH))
    membership = Membership()
    membership.load_known(repo.list_workers())
    ctx = Context(repo=repo, membership=membership, workers=WorkerClient())
    ctx.repair = RepairManager(ctx)
    app.state.ctx = ctx

    # Captured before any request is served: everything in these states is left over from a previous run.
    uploading = repo.object_ids_in_state("UPLOADING")
    deleting = repo.object_ids_in_state("DELETING")

    tasks = [
        asyncio.create_task(membership.run_detector()),
        asyncio.create_task(ctx.repair.run()),
        asyncio.create_task(run_cleanup(ctx)),
        asyncio.create_task(run_reconciliation(ctx)),
    ]
    if uploading or deleting:
        ctx.spawn(_finish_interrupted_work(ctx, uploading, deleting))
    log.info(
        "controller started: chunk size %d bytes, replication factor %d, worker timeout %.0fs, repair grace %.0fs",
        config.CHUNK_SIZE, config.REPLICATION_FACTOR, config.WORKER_TIMEOUT_S, config.REPAIR_GRACE_S,
    )
    try:
        yield
    finally:
        tasks += list(ctx.background)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await ctx.workers.close()
        repo.close()


app = FastAPI(title="Mini Distributed Cloud Object Store - Controller", version="1.0.0", lifespan=lifespan)
errors.install(app)
app.include_router(api_objects.router)
app.include_router(api_internal.router)
app.include_router(api_admin.router)


@app.middleware("http")
async def log_client_requests(request: Request, call_next):
    """Log client API calls (heartbeats are deliberately not logged)."""
    if not request.url.path.startswith("/api/"):
        return await call_next(request)
    started = time.monotonic()
    response = await call_next(request)
    log.info("%s %s -> %d (%.0f ms)", request.method, request.url.path, response.status_code,
             (time.monotonic() - started) * 1000)
    return response


@app.get("/health")
async def health():
    return {"status": "ok", "role": "controller"}

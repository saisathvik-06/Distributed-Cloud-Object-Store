"""Internal API served by the controller: worker registration and heartbeats."""
import re

from fastapi import APIRouter, Request

from . import config
from .context import Context
from .errors import ApiError
from .reconcile import reconcile_worker
from .repository import run_db
from .schemas import HeartbeatRequest, RegisterRequest

router = APIRouter(prefix="/internal/workers", tags=["internal"])

WORKER_ID_RE = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")


@router.post("/register")
async def register_worker(body: RegisterRequest, request: Request):
    ctx: Context = request.app.state.ctx
    if not WORKER_ID_RE.fullmatch(body.worker_id) or not body.address.startswith(("http://", "https://")):
        raise ApiError(400, "INVALID_REQUEST", "worker_id or address is malformed")
    address = body.address.rstrip("/")
    await run_db(ctx.repo.upsert_worker, body.worker_id, address)
    ctx.membership.register(body.worker_id, address, body.model_dump())
    # Every (re)join is reconciled against what is really on the worker's disk.
    ctx.spawn(reconcile_worker(ctx, body.worker_id))
    return {
        "status": "registered",
        "worker_id": body.worker_id,
        "heartbeat_interval_s": config.HEARTBEAT_INTERVAL_S,
    }


@router.post("/heartbeat")
async def worker_heartbeat(body: HeartbeatRequest, request: Request):
    ctx: Context = request.app.state.ctx
    if not ctx.membership.heartbeat(body.worker_id, body.model_dump()):
        # Unknown, or declared DEAD: the worker must register again so it is reconciled.
        raise ApiError(404, "WORKER_NOT_REGISTERED", f"Worker {body.worker_id} must register again")
    return {"status": "ok"}

"""Cluster status for operators and for the demonstration (an addition beyond the required API)."""
import time

from fastapi import APIRouter, Request

from . import config
from .context import Context
from .repository import run_db
from .schemas import chunk_status

router = APIRouter(prefix="/api/v1/admin", tags=["admin"])


@router.get("/cluster")
async def cluster_status(request: Request):
    ctx: Context = request.app.state.ctx
    replica_counts = await run_db(ctx.repo.replica_counts_by_worker)
    chunks = await run_db(ctx.repo.committed_chunks_with_replicas)
    objects = await run_db(ctx.repo.count_objects)

    now = time.monotonic()
    workers = [
        {
            "worker_id": w.worker_id,
            "address": w.address,
            "state": w.state,
            "seconds_since_heartbeat": None if w.last_heartbeat is None else round(now - w.last_heartbeat, 1),
            "recorded_replicas": replica_counts.get(w.worker_id, 0),
            "reported_chunks": w.chunk_count,
            "used_bytes": w.used_bytes,
            "free_bytes": w.free_bytes,
        }
        for w in ctx.membership.all()
    ]

    summary = {"healthy": 0, "under_replicated": 0, "unavailable": 0, "over_replicated": 0}
    corrupt = 0
    for chunk in chunks:
        summary[chunk_status(chunk["replicas"], ctx.membership)] += 1
        corrupt += sum(1 for r in chunk["replicas"] if r["state"] == "CORRUPT")

    return {
        "workers": workers,
        "alive_workers": len(ctx.membership.alive_ids()),
        "replication": {
            "replication_factor": config.REPLICATION_FACTOR,
            "objects": objects,
            "chunks": len(chunks),
            **summary,
            "corrupt_replicas": corrupt,
            "fully_replicated": bool(chunks) and summary["healthy"] == len(chunks) or not chunks,
        },
        "settings": {
            "chunk_size_bytes": config.CHUNK_SIZE,
            "heartbeat_interval_s": config.HEARTBEAT_INTERVAL_S,
            "worker_timeout_s": config.WORKER_TIMEOUT_S,
            "repair_grace_s": config.REPAIR_GRACE_S,
            "repair_interval_s": config.REPAIR_INTERVAL_S,
        },
    }

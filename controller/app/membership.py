"""Worker registry and failure detector.

Liveness is kept in memory and measured only on the controller's monotonic
clock, so it never depends on clocks agreeing across containers and a
heartbeat never waits on the database.
"""
import asyncio
import logging
import time
from dataclasses import dataclass

from . import config

log = logging.getLogger("controller.membership")

ALIVE = "ALIVE"
DEAD = "DEAD"


@dataclass
class WorkerInfo:
    worker_id: str
    address: str
    state: str = DEAD
    last_heartbeat: float | None = None   # monotonic seconds
    died_at: float | None = None          # monotonic seconds
    chunk_count: int = 0
    used_bytes: int = 0
    free_bytes: int = 0


class Membership:
    def __init__(self):
        self._workers: dict[str, WorkerInfo] = {}
        self.started_at = time.monotonic()

    def load_known(self, rows: list[dict]) -> None:
        """Workers remembered from before a controller restart start as DEAD
        until they register again; the startup grace keeps repairs quiet meanwhile."""
        for row in rows:
            self._workers[row["worker_id"]] = WorkerInfo(
                worker_id=row["worker_id"], address=row["address"], state=DEAD, died_at=self.started_at
            )

    # -- updates -----------------------------------------------------------
    def register(self, worker_id: str, address: str, stats: dict) -> None:
        info = self._workers.get(worker_id)
        was_dead = info is not None and info.state == DEAD
        if info is None:
            info = self._workers[worker_id] = WorkerInfo(worker_id=worker_id, address=address)
        info.address = address
        info.state = ALIVE
        info.last_heartbeat = time.monotonic()
        info.died_at = None
        self._apply_stats(info, stats)
        log.info("WORKER_REGISTERED %s at %s%s", worker_id, address, " (back from DEAD)" if was_dead else "")

    def heartbeat(self, worker_id: str, stats: dict) -> bool:
        """Returns False when the worker must register again (unknown or DEAD)."""
        info = self._workers.get(worker_id)
        if info is None or info.state != ALIVE:
            return False
        info.last_heartbeat = time.monotonic()
        self._apply_stats(info, stats)
        return True

    @staticmethod
    def _apply_stats(info: WorkerInfo, stats: dict) -> None:
        info.chunk_count = int(stats.get("chunk_count") or 0)
        info.used_bytes = int(stats.get("used_bytes") or 0)
        info.free_bytes = int(stats.get("free_bytes") or 0)

    # -- queries -----------------------------------------------------------
    def all(self) -> list[WorkerInfo]:
        return sorted(self._workers.values(), key=lambda w: w.worker_id)

    def get(self, worker_id: str) -> WorkerInfo | None:
        return self._workers.get(worker_id)

    def alive_ids(self) -> list[str]:
        return sorted(w.worker_id for w in self._workers.values() if w.state == ALIVE)

    def is_alive(self, worker_id: str) -> bool:
        info = self._workers.get(worker_id)
        return info is not None and info.state == ALIVE

    def state_of(self, worker_id: str) -> str:
        info = self._workers.get(worker_id)
        return info.state if info else "UNKNOWN"

    def address_of(self, worker_id: str) -> str | None:
        info = self._workers.get(worker_id)
        return info.address if info else None

    def in_grace(self, worker_id: str) -> bool:
        """True while a dead worker is still expected back, so its copies are not yet re-created."""
        info = self._workers.get(worker_id)
        if info is None or info.state != DEAD or info.died_at is None:
            return False
        return time.monotonic() - info.died_at < config.REPAIR_GRACE_S

    # -- failure detector --------------------------------------------------
    def detect_failures(self) -> list[str]:
        now = time.monotonic()
        died = []
        for info in self._workers.values():
            if info.state == ALIVE and now - (info.last_heartbeat or 0) > config.WORKER_TIMEOUT_S:
                info.state = DEAD
                info.died_at = now
                died.append(info.worker_id)
                log.warning(
                    "WORKER_DEAD %s: no heartbeat for %.0fs; its copies will be re-created after %.0fs grace",
                    info.worker_id, config.WORKER_TIMEOUT_S, config.REPAIR_GRACE_S,
                )
        return died

    async def run_detector(self) -> None:
        while True:
            await asyncio.sleep(1.0)
            self.detect_failures()

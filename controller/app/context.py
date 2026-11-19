"""The shared services of the controller, created once at startup."""
import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any, TYPE_CHECKING
from weakref import WeakValueDictionary

from .membership import Membership
from .repository import Repository
if TYPE_CHECKING:
    from .worker_client import WorkerClient


@dataclass
class Context:
    repo: Repository
    membership: Membership
    workers: "WorkerClient"
    repair: Any = None                       # RepairManager, set right after construction
    background: set = field(default_factory=set)
    _object_locks: Any = field(default_factory=WeakValueDictionary)
    _worker_locks: Any = field(default_factory=WeakValueDictionary)

    def object_lock(self, object_id: str) -> asyncio.Lock:
        """All lifecycle work on one object shares a lock; idle locks are reclaimed."""
        lock = self._object_locks.get(object_id)
        if lock is None:
            lock = asyncio.Lock()
            self._object_locks[object_id] = lock
        return lock

    def worker_lock(self, worker_id: str) -> asyncio.Lock:
        lock = self._worker_locks.get(worker_id)
        if lock is None:
            lock = asyncio.Lock()
            self._worker_locks[worker_id] = lock
        return lock

    def spawn(self, coro) -> None:
        """Run a coroutine in the background, keeping a reference so it is not garbage collected."""
        task = asyncio.create_task(coro)
        self.background.add(task)
        def finished(done):
            self.background.discard(done)
            if not done.cancelled() and done.exception() is not None:
                exc = done.exception()
                logging.getLogger("controller.background").error(
                    "background task failed", exc_info=(type(exc), exc, exc.__traceback__))
        task.add_done_callback(finished)

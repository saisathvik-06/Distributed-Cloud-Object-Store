"""The shared services of the controller, created once at startup."""
import asyncio
from dataclasses import dataclass, field
from typing import Any

from .membership import Membership
from .repository import Repository
from .worker_client import WorkerClient


@dataclass
class Context:
    repo: Repository
    membership: Membership
    workers: WorkerClient
    repair: Any = None                       # RepairManager, set right after construction
    background: set = field(default_factory=set)

    def spawn(self, coro) -> None:
        """Run a coroutine in the background, keeping a reference so it is not garbage collected."""
        task = asyncio.create_task(coro)
        self.background.add(task)
        task.add_done_callback(self.background.discard)

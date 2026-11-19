"""Real SQLite + real worker disk storage, with injectable transport failures."""
import asyncio
import hashlib
import importlib.util
import io
from pathlib import Path
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'controller'))
from app.context import Context
from app.db import connect
from app.membership import Membership
from app.repository import Repository
from app.repair import RepairManager
from app.worker_errors import WorkerError, ChunkMissing, ChunkCorruptOnWorker

spec = importlib.util.spec_from_file_location('worker_storage', ROOT / 'worker/app/storage.py')
storage = importlib.util.module_from_spec(spec)
spec.loader.exec_module(storage)


class File:
    def __init__(self, data, short_reads=False):
        self.stream = io.BytesIO(data)
        self.short_reads = short_reads

    async def read(self, size):
        return self.stream.read(min(size, 711) if self.short_reads else size)


class DiskWorkers:
    def __init__(self, directory):
        self.stores = {f'w{i}': storage.ChunkStore(str(directory / f'w{i}'), 1048576) for i in range(1, 5)}
        self.offline = set()
        self.fail_put = set()
        self.fail_delete = set()
        self.after_put = None
        self.before_get = None
        self.reads = 0

    async def put_chunk(self, address, cid, data, checksum):
        address = address.removeprefix("http://")
        if address in self.offline or address in self.fail_put:
            raise WorkerError('injected PUT failure')
        self.stores[address].put(cid, data, checksum)
        if self.after_put:
            await self.after_put(address, cid)

    async def get_chunk(self, address, cid):
        address = address.removeprefix("http://")
        self.reads += 1
        if address in self.offline:
            raise WorkerError('injected outage')
        if self.before_get:
            await self.before_get(address, cid)
        try:
            return self.stores[address].get(cid)[0]
        except storage.ChunkNotFound:
            raise ChunkMissing(cid) from None
        except storage.ChunkCorrupt:
            raise ChunkCorruptOnWorker(cid) from None

    async def delete_chunk(self, address, cid):
        address = address.removeprefix("http://")
        if address in self.offline or address in self.fail_delete:
            return False
        self.stores[address].delete(cid)
        return True

    async def list_chunks(self, address, worker_id):
        address = address.removeprefix("http://")
        if address in self.offline:
            raise WorkerError('injected outage')
        return self.stores[address].inventory()

    def copies(self, cid=None):
        return [(w, item['chunk_id']) for w, st in self.stores.items()
                for item in st.inventory() if cid is None or item['chunk_id'] == cid]

    def corrupt(self, worker, cid):
        Path(self.stores[worker]._paths(cid)[0]).write_bytes(b'CORRUPTED')


def make_context(directory):
    repo = Repository(connect(str(directory / 'metadata.db')))
    membership = Membership()
    workers = DiskWorkers(directory)
    for worker_id in workers.stores:
        repo.upsert_worker(worker_id, worker_id)
        membership.register(worker_id, worker_id, {})
    ctx = Context(repo, membership, workers)
    ctx.repair = RepairManager(ctx)
    return ctx


async def consume(prepared):
    return b''.join([part async for part in prepared.body()])

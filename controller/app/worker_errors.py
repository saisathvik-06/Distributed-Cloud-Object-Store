"""Transport-independent worker failures."""


class WorkerError(Exception):
    pass


class ChunkMissing(WorkerError):
    pass


class ChunkCorruptOnWorker(WorkerError):
    pass

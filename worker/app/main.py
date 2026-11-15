"""Storage worker: a plain chunk server.

It knows nothing about objects, only chunks, and never talks to other workers.
"""
import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from starlette.concurrency import run_in_threadpool
from starlette.exceptions import HTTPException as StarletteHTTPException

from . import config, registration
from .storage import (
    ChecksumMismatch,
    ChunkCorrupt,
    ChunkNotFound,
    ChunkStore,
    ChunkTooLarge,
    InvalidChunkId,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("worker")

CHECKSUM_HEADER = "X-Chunk-Checksum"


def error(status: int, code: str, message: str) -> JSONResponse:
    return JSONResponse(status_code=status, content={"error": {"code": code, "message": message}})


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.store = ChunkStore(config.DATA_DIR, config.MAX_CHUNK_BYTES)
    task = asyncio.create_task(registration.run(app.state.store))
    log.info("%s started, data directory %s", config.WORKER_ID, config.DATA_DIR)
    try:
        yield
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


app = FastAPI(title=f"Object store worker {config.WORKER_ID}", lifespan=lifespan)


@app.exception_handler(InvalidChunkId)
async def _invalid_id(request: Request, exc: InvalidChunkId):
    return error(400, "INVALID_CHUNK_ID", "Chunk ID is malformed")


@app.exception_handler(ChunkNotFound)
async def _not_found(request: Request, exc: ChunkNotFound):
    return error(404, "CHUNK_NOT_FOUND", f"Chunk {exc} is not stored on {config.WORKER_ID}")


@app.exception_handler(ChunkCorrupt)
async def _corrupt(request: Request, exc: ChunkCorrupt):
    log.error("CORRUPTION_DETECTED chunk %s no longer matches its stored checksum", exc)
    return error(500, "CHUNK_CORRUPT", f"Chunk {exc} on {config.WORKER_ID} failed its checksum")


@app.exception_handler(StarletteHTTPException)
async def _http_error(request: Request, exc: StarletteHTTPException):
    return error(exc.status_code, "HTTP_ERROR", str(exc.detail))


@app.exception_handler(Exception)
async def _unexpected(request: Request, exc: Exception):
    log.exception("unexpected error")
    return error(500, "INTERNAL_ERROR", "Unexpected worker error")


@app.get("/health")
async def health():
    return {"status": "ok", "worker_id": config.WORKER_ID}


@app.put("/internal/chunks/{chunk_id}")
async def put_chunk(chunk_id: str, request: Request):
    store: ChunkStore = request.app.state.store
    data = await request.body()
    expected = request.headers.get(CHECKSUM_HEADER)
    try:
        checksum, existed = await run_in_threadpool(store.put, chunk_id, data, expected)
    except ChecksumMismatch as exc:
        return error(400, "CHECKSUM_MISMATCH", f"Chunk was damaged in transit: {exc}")
    except ChunkTooLarge:
        return error(400, "CHUNK_TOO_LARGE", f"Chunk exceeds {config.MAX_CHUNK_BYTES} bytes")
    return JSONResponse(
        status_code=200 if existed else 201,
        content={"chunk_id": chunk_id, "size": len(data), "checksum": checksum, "worker_id": config.WORKER_ID},
    )


@app.get("/internal/chunks/{chunk_id}")
async def get_chunk(chunk_id: str, request: Request):
    store: ChunkStore = request.app.state.store
    data, checksum = await run_in_threadpool(store.get, chunk_id)
    return Response(content=data, media_type="application/octet-stream", headers={CHECKSUM_HEADER: checksum})


@app.delete("/internal/chunks/{chunk_id}", status_code=204)
async def delete_chunk(chunk_id: str, request: Request):
    store: ChunkStore = request.app.state.store
    await run_in_threadpool(store.delete, chunk_id)  # idempotent
    return Response(status_code=204)


@app.get("/internal/workers/{worker_id}/chunks")
async def list_chunks(worker_id: str, request: Request):
    if worker_id != config.WORKER_ID:
        return error(404, "WRONG_WORKER", f"This is {config.WORKER_ID}, not {worker_id}")
    store: ChunkStore = request.app.state.store
    chunks = await run_in_threadpool(store.inventory)
    return {"worker_id": config.WORKER_ID, "chunk_count": len(chunks), "chunks": chunks}

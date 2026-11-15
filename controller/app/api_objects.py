"""Client-facing REST API: /api/v1/objects."""
import json
import mimetypes
from urllib.parse import quote

from fastapi import APIRouter, File, Form, Query, Request, Response, UploadFile
from fastapi.responses import JSONResponse, StreamingResponse

from .context import Context
from .deletion import purge_object
from .download import open_download
from .errors import ApiError, not_found
from .repository import run_db
from .schemas import normalise_object_id, object_document, validate_custom_metadata
from .upload import store_object

router = APIRouter(prefix="/api/v1/objects", tags=["objects"])


def _ctx(request: Request) -> Context:
    return request.app.state.ctx


def _object_id(raw: str) -> str:
    object_id = normalise_object_id(raw)
    if object_id is None:
        raise not_found(raw)
    return object_id


async def _document(ctx: Context, object_id: str) -> dict:
    obj = await run_db(ctx.repo.get_object, object_id)
    if obj is None:
        raise not_found(object_id)
    chunks = await run_db(ctx.repo.get_chunks, object_id)
    return object_document(obj, chunks, ctx.membership)


def _clean_name(filename: str | None) -> str:
    # The name is only ever stored as text, never used as a path.
    name = (filename or "").replace("\\", "/").rsplit("/", 1)[-1].strip()
    return name or "unnamed"


def _content_type(declared: str | None, name: str) -> str:
    if declared and declared != "application/octet-stream":
        return declared
    guessed, _ = mimetypes.guess_type(name)
    return guessed or "application/octet-stream"


@router.post("", status_code=201)
async def upload_object(
    request: Request,
    file: UploadFile = File(..., description="Binary object data"),
    metadata: str | None = Form(None, description="JSON object of custom key-value metadata"),
):
    ctx = _ctx(request)
    custom: dict = {}
    if metadata is not None and metadata.strip():
        try:
            custom = json.loads(metadata)
        except ValueError:
            raise ApiError(400, "INVALID_METADATA", "The 'metadata' field is not valid JSON") from None
        validate_custom_metadata(custom, allow_null=False)

    name = _clean_name(file.filename)
    object_id = await store_object(ctx, file, name, _content_type(file.content_type, name), custom)
    document = await _document(ctx, object_id)
    return JSONResponse(status_code=201, content=document, headers={"Location": f"/api/v1/objects/{object_id}"})


@router.get("")
async def list_objects(
    request: Request,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    ctx = _ctx(request)
    rows, total = await run_db(ctx.repo.list_objects, limit, offset)
    return {"objects": rows, "total": total, "limit": limit, "offset": offset}


@router.get("/{object_id}")
async def download_object(object_id: str, request: Request):
    ctx = _ctx(request)
    object_id = _object_id(object_id)
    obj = await run_db(ctx.repo.get_object, object_id)
    if obj is None:
        raise not_found(object_id)
    body = await open_download(ctx, obj)
    ascii_name = obj["name"].encode("ascii", "replace").decode().replace('"', "'")
    headers = {
        "Content-Length": str(obj["size"]),
        "Content-Disposition": f"attachment; filename=\"{ascii_name}\"; filename*=UTF-8''{quote(obj['name'])}",
        "ETag": f'"{obj["checksum"]}"',
        "X-Object-Checksum": obj["checksum"],
    }
    return StreamingResponse(body, media_type=obj["content_type"], headers=headers)


@router.get("/{object_id}/metadata")
async def get_metadata(object_id: str, request: Request):
    return await _document(_ctx(request), _object_id(object_id))


@router.patch("/{object_id}/metadata")
async def update_metadata(object_id: str, request: Request):
    ctx = _ctx(request)
    object_id = _object_id(object_id)
    try:
        patch = json.loads(await request.body() or b"null")
    except ValueError:
        raise ApiError(400, "INVALID_REQUEST", "Request body is not valid JSON") from None
    validate_custom_metadata(patch, allow_null=True)

    outcome = await run_db(ctx.repo.patch_metadata, object_id, patch)
    if outcome == "not_found":
        raise not_found(object_id)
    if outcome == "deleting":
        raise ApiError(409, "OBJECT_BEING_DELETED", f"Object {object_id} is being deleted")
    return await _document(ctx, object_id)


@router.delete("/{object_id}", status_code=204)
async def delete_object(object_id: str, request: Request):
    ctx = _ctx(request)
    object_id = _object_id(object_id)
    outcome = await run_db(ctx.repo.begin_delete, object_id)
    if outcome == "not_found":
        raise not_found(object_id)
    if outcome == "deleting":
        raise ApiError(409, "OBJECT_BEING_DELETED", f"Deletion of object {object_id} is already in progress")
    await purge_object(ctx, object_id)
    return Response(status_code=204)

"""Request validation and response shapes for the client API."""
import json
import uuid

from pydantic import BaseModel

from . import config
from .errors import ApiError
from .membership import Membership


class RegisterRequest(BaseModel):
    worker_id: str
    address: str
    chunk_count: int = 0
    used_bytes: int = 0
    free_bytes: int = 0


class HeartbeatRequest(BaseModel):
    worker_id: str
    chunk_count: int = 0
    used_bytes: int = 0
    free_bytes: int = 0


def normalise_object_id(raw: str) -> str | None:
    """Canonical form of a valid Object ID, or None. A malformed ID cannot exist, so callers answer 404."""
    try:
        parsed = uuid.UUID(raw)
    except (ValueError, AttributeError, TypeError):
        return None
    return str(parsed) if str(parsed) == raw.lower() else None


def validate_custom_metadata(value, allow_null: bool) -> dict:
    """Custom metadata is a flat JSON object: string keys; string, number or boolean values.

    With `allow_null` (PATCH), a null value means "remove this key".
    """
    if not isinstance(value, dict):
        raise ApiError(400, "INVALID_METADATA", "Custom metadata must be a JSON object of key-value pairs")
    if len(value) > config.MAX_METADATA_KEYS:
        raise ApiError(400, "INVALID_METADATA", f"Custom metadata may have at most {config.MAX_METADATA_KEYS} keys")
    for key, item in value.items():
        if not isinstance(key, str) or not key.strip() or len(key) > 128:
            raise ApiError(400, "INVALID_METADATA", "Metadata keys must be non-empty strings of at most 128 characters")
        if item is None:
            if not allow_null:
                raise ApiError(400, "INVALID_METADATA", f"Metadata value for '{key}' must not be null")
        elif isinstance(item, (dict, list)) or not isinstance(item, (str, int, float, bool)):
            raise ApiError(400, "INVALID_METADATA", f"Metadata value for '{key}' must be a string, number or boolean")
    if len(json.dumps(value).encode("utf-8")) > config.MAX_METADATA_BYTES:
        raise ApiError(400, "INVALID_METADATA", f"Custom metadata may not exceed {config.MAX_METADATA_BYTES} bytes")
    return value


def chunk_status(replicas: list[dict], membership: Membership) -> str:
    """Derived from the number of HEALTHY replicas on ALIVE workers."""
    live = sum(1 for r in replicas if r["state"] == "HEALTHY" and membership.is_alive(r["worker_id"]))
    if live == 0:
        return "unavailable"
    if live < config.REPLICATION_FACTOR:
        return "under_replicated"
    if live > config.REPLICATION_FACTOR:
        return "over_replicated"
    return "healthy"


def object_document(obj: dict, chunks: list[dict], membership: Membership) -> dict:
    """System metadata, custom metadata, and every chunk with its storage locations."""
    return {
        "object_id": obj["object_id"],
        "name": obj["name"],
        "size": obj["size"],
        "content_type": obj["content_type"],
        "checksum": obj["checksum"],
        "checksum_algorithm": "sha256",
        "chunk_count": obj["chunk_count"],
        "chunk_size": config.CHUNK_SIZE,
        "replication_factor": config.REPLICATION_FACTOR,
        "created_at": obj["created_at"],
        "modified_at": obj["modified_at"],
        "custom_metadata": obj["custom_metadata"],
        "chunks": [
            {
                "chunk_id": c["chunk_id"],
                "index": c["chunk_index"],
                "size": c["size"],
                "checksum": c["checksum"],
                "status": chunk_status(c["replicas"], membership),
                "replicas": [
                    {
                        "worker_id": r["worker_id"],
                        "worker_state": membership.state_of(r["worker_id"]),
                        "replica_state": r["state"],
                    }
                    for r in c["replicas"]
                ],
            }
            for c in chunks
        ],
    }

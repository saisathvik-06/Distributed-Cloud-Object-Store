"""Strict JSON metadata validation, also applied inside PATCH transactions."""
import json

from . import config
from .errors import ApiError


def parse_json(raw):
    def reject_constant(value):
        raise ValueError(f"Non-finite JSON number: {value}")
    return json.loads(raw, parse_constant=reject_constant)


def validate_custom_metadata(value, allow_null: bool = True) -> dict:
    # JSON objects allow arbitrary JSON values. A top-level PATCH null removes a key.
    if not isinstance(value, dict):
        raise ApiError(400, "INVALID_METADATA", "Custom metadata must be a JSON object")
    if config.MAX_METADATA_KEYS and len(value) > config.MAX_METADATA_KEYS:
        raise ApiError(400, "INVALID_METADATA", f"Metadata may have at most {config.MAX_METADATA_KEYS} keys")
    try:
        encoded = json.dumps(value, allow_nan=False, ensure_ascii=False).encode("utf-8")
    except (ValueError, TypeError, RecursionError):
        raise ApiError(400, "INVALID_METADATA", "Metadata must contain valid, finite JSON values") from None
    if config.MAX_METADATA_BYTES and len(encoded) > config.MAX_METADATA_BYTES:
        raise ApiError(400, "INVALID_METADATA", f"Metadata may not exceed {config.MAX_METADATA_BYTES} bytes")
    return value

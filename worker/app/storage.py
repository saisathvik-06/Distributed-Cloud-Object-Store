"""On-disk chunk store.

Layout under the data directory:
    chunks/<chunk_id>.chunk   the bytes
    chunks/<chunk_id>.meta    JSON sidecar: size, sha256, stored_at
    tmp/                      in-progress writes, emptied at startup

Chunks are immutable. A write goes to tmp/, is flushed to disk and then renamed
into place, so a crash can never leave a half-written chunk under its real name.
"""
import hashlib
import json
import os
import re
import shutil
import time
import uuid

# <uuid>-<5 digit index>. Validated before the ID is ever used in a file path.
CHUNK_ID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}-\d{5}$"
)


class InvalidChunkId(Exception):
    pass


class ChecksumMismatch(Exception):
    pass


class ChunkTooLarge(Exception):
    pass


class ChunkNotFound(Exception):
    pass


class ChunkCorrupt(Exception):
    pass


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class ChunkStore:
    def __init__(self, data_dir: str, max_chunk_bytes: int):
        self.chunks_dir = os.path.join(data_dir, "chunks")
        self.tmp_dir = os.path.join(data_dir, "tmp")
        self.max_chunk_bytes = max_chunk_bytes
        os.makedirs(self.chunks_dir, exist_ok=True)
        os.makedirs(self.tmp_dir, exist_ok=True)
        self._clear_tmp()

    # -- helpers -----------------------------------------------------------
    def _clear_tmp(self) -> None:
        for name in os.listdir(self.tmp_dir):
            try:
                os.remove(os.path.join(self.tmp_dir, name))
            except OSError:
                pass

    def _paths(self, chunk_id: str) -> tuple[str, str]:
        if not CHUNK_ID_RE.match(chunk_id):
            raise InvalidChunkId(chunk_id)
        base = os.path.join(self.chunks_dir, chunk_id)
        return base + ".chunk", base + ".meta"

    def _atomic_write(self, final_path: str, data: bytes) -> None:
        tmp_path = os.path.join(self.tmp_dir, uuid.uuid4().hex)
        with open(tmp_path, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, final_path)

    @staticmethod
    def _read_meta(meta_path: str) -> dict | None:
        try:
            with open(meta_path, "r", encoding="utf-8") as f:
                meta = json.load(f)
            return meta if isinstance(meta, dict) else None
        except (OSError, ValueError):
            return None

    # -- operations --------------------------------------------------------
    def put(self, chunk_id: str, data: bytes, expected_sha256: str | None) -> tuple[str, bool]:
        """Store a chunk. Returns (sha256, already_existed)."""
        chunk_path, meta_path = self._paths(chunk_id)
        if len(data) > self.max_chunk_bytes:
            raise ChunkTooLarge(len(data))
        actual = sha256_hex(data)
        if expected_sha256 and expected_sha256.lower() != actual:
            raise ChecksumMismatch(f"expected {expected_sha256}, received {actual}")
        existed = os.path.exists(chunk_path)
        meta = {"size": len(data), "sha256": actual, "stored_at": time.time()}
        self._atomic_write(chunk_path, data)
        self._atomic_write(meta_path, json.dumps(meta).encode("utf-8"))
        return actual, existed

    def get(self, chunk_id: str) -> tuple[bytes, str]:
        """Read a chunk and verify it against the sidecar. Returns (data, sha256)."""
        chunk_path, meta_path = self._paths(chunk_id)
        try:
            with open(chunk_path, "rb") as f:
                data = f.read()
        except FileNotFoundError:
            raise ChunkNotFound(chunk_id) from None
        actual = sha256_hex(data)
        meta = self._read_meta(meta_path)
        # With a missing or unreadable sidecar this worker cannot judge the data;
        # the controller still verifies against its own authoritative checksum.
        if meta is not None and meta.get("sha256") != actual:
            raise ChunkCorrupt(chunk_id)
        return data, actual

    def delete(self, chunk_id: str) -> bool:
        """Delete a chunk. Returns False when it was already absent."""
        chunk_path, meta_path = self._paths(chunk_id)
        existed = False
        for path in (chunk_path, meta_path):
            try:
                os.remove(path)
                existed = True
            except FileNotFoundError:
                pass
        return existed

    def inventory(self) -> list[dict]:
        """Everything on disk, as recorded by the sidecars (no re-hashing)."""
        items = []
        with os.scandir(self.chunks_dir) as entries:
            for entry in entries:
                if not entry.name.endswith(".chunk"):
                    continue
                chunk_id = entry.name[: -len(".chunk")]
                if not CHUNK_ID_RE.match(chunk_id):
                    continue
                try:
                    stat = entry.stat()
                except OSError:
                    continue
                meta = self._read_meta(os.path.join(self.chunks_dir, chunk_id + ".meta")) or {}
                items.append(
                    {
                        "chunk_id": chunk_id,
                        "size": stat.st_size,
                        "checksum": meta.get("sha256"),
                        "stored_at": meta.get("stored_at", stat.st_mtime),
                    }
                )
        items.sort(key=lambda c: c["chunk_id"])
        return items

    def stats(self) -> dict:
        count = 0
        used = 0
        with os.scandir(self.chunks_dir) as entries:
            for entry in entries:
                if entry.name.endswith(".chunk"):
                    count += 1
                    try:
                        used += entry.stat().st_size
                    except OSError:
                        pass
        free = shutil.disk_usage(self.chunks_dir).free
        return {"chunk_count": count, "used_bytes": used, "free_bytes": free}

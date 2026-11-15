"""All SQL lives here.

Methods are synchronous and thread-safe. Async code calls them through
`run_db(...)` so that the event loop is never blocked by the database.
"""
import asyncio
import json
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


async def run_db(fn, *args, **kwargs):
    return await asyncio.to_thread(fn, *args, **kwargs)


class Repository:
    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn
        self._lock = threading.RLock()

    @contextmanager
    def _tx(self):
        """One write transaction; rolled back if the block raises."""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                yield self._conn
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise
            else:
                self._conn.execute("COMMIT")

    def _query(self, sql: str, params=()) -> list[sqlite3.Row]:
        with self._lock:
            return self._conn.execute(sql, params).fetchall()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # -- workers -----------------------------------------------------------
    def upsert_worker(self, worker_id: str, address: str) -> None:
        with self._tx() as c:
            c.execute(
                "INSERT INTO workers (worker_id, address, first_seen) VALUES (?, ?, ?) "
                "ON CONFLICT(worker_id) DO UPDATE SET address = excluded.address",
                (worker_id, address, utc_now()),
            )

    def list_workers(self) -> list[dict]:
        return [dict(r) for r in self._query("SELECT * FROM workers ORDER BY worker_id")]

    # -- object lifecycle --------------------------------------------------
    def create_object(self, object_id: str, name: str, content_type: str, custom_metadata: dict) -> None:
        now = utc_now()
        with self._tx() as c:
            c.execute(
                "INSERT INTO objects (object_id, name, content_type, custom_metadata, state, created_at, modified_at) "
                "VALUES (?, ?, ?, ?, 'UPLOADING', ?, ?)",
                (object_id, name, content_type, json.dumps(custom_metadata), now, now),
            )

    def add_chunk(self, chunk_id: str, object_id: str, index: int, size: int, checksum: str) -> None:
        with self._tx() as c:
            c.execute(
                "INSERT INTO chunks (chunk_id, object_id, chunk_index, size, checksum) VALUES (?, ?, ?, ?, ?)",
                (chunk_id, object_id, index, size, checksum),
            )

    def commit_object(self, object_id: str, size: int, chunk_count: int, checksum: str) -> None:
        """The commit point of an upload: the object becomes visible atomically."""
        now = utc_now()
        with self._tx() as c:
            c.execute(
                "UPDATE objects SET size = ?, chunk_count = ?, checksum = ?, state = 'COMMITTED', "
                "created_at = ?, modified_at = ? WHERE object_id = ?",
                (size, chunk_count, checksum, now, now, object_id),
            )

    def get_object(self, object_id: str, committed_only: bool = True) -> dict | None:
        rows = self._query("SELECT * FROM objects WHERE object_id = ?", (object_id,))
        if not rows:
            return None
        obj = dict(rows[0])
        if committed_only and obj["state"] != "COMMITTED":
            return None
        obj["custom_metadata"] = json.loads(obj["custom_metadata"])
        return obj

    def get_chunks(self, object_id: str) -> list[dict]:
        """Chunks in order, each with its replica list."""
        with self._lock:
            chunk_rows = self._conn.execute(
                "SELECT chunk_id, chunk_index, size, checksum FROM chunks WHERE object_id = ? ORDER BY chunk_index",
                (object_id,),
            ).fetchall()
            replica_rows = self._conn.execute(
                "SELECT r.chunk_id, r.worker_id, r.state FROM replicas r "
                "JOIN chunks c ON c.chunk_id = r.chunk_id WHERE c.object_id = ? ORDER BY r.worker_id",
                (object_id,),
            ).fetchall()
        chunks = {r["chunk_id"]: {**dict(r), "replicas": []} for r in chunk_rows}
        for r in replica_rows:
            chunks[r["chunk_id"]]["replicas"].append({"worker_id": r["worker_id"], "state": r["state"]})
        return list(chunks.values())

    def list_objects(self, limit: int, offset: int) -> tuple[list[dict], int]:
        with self._lock:
            total = self._conn.execute("SELECT COUNT(*) FROM objects WHERE state = 'COMMITTED'").fetchone()[0]
            rows = self._conn.execute(
                "SELECT object_id, name, size, content_type, chunk_count, created_at, modified_at FROM objects "
                "WHERE state = 'COMMITTED' ORDER BY created_at, object_id LIMIT ? OFFSET ?",
                (limit, offset),
            ).fetchall()
        return [dict(r) for r in rows], total

    def patch_metadata(self, object_id: str, patch: dict) -> str:
        """Merge keys into custom metadata; a None value removes the key.

        Returns 'ok', 'not_found' or 'deleting'. The read-modify-write happens
        in one transaction so concurrent PATCHes cannot lose each other's keys.
        """
        with self._tx() as c:
            row = c.execute("SELECT state, custom_metadata FROM objects WHERE object_id = ?", (object_id,)).fetchone()
            if row is None or row["state"] == "UPLOADING":
                return "not_found"
            if row["state"] == "DELETING":
                return "deleting"
            merged = json.loads(row["custom_metadata"])
            for key, value in patch.items():
                if value is None:
                    merged.pop(key, None)
                else:
                    merged[key] = value
            c.execute(
                "UPDATE objects SET custom_metadata = ?, modified_at = ? WHERE object_id = ?",
                (json.dumps(merged), utc_now(), object_id),
            )
        return "ok"

    def begin_delete(self, object_id: str) -> str:
        """COMMITTED -> DELETING. Returns 'ok', 'not_found' or 'deleting'."""
        with self._tx() as c:
            row = c.execute("SELECT state FROM objects WHERE object_id = ?", (object_id,)).fetchone()
            if row is None or row["state"] == "UPLOADING":
                return "not_found"
            if row["state"] == "DELETING":
                return "deleting"
            c.execute("UPDATE objects SET state = 'DELETING' WHERE object_id = ?", (object_id,))
        return "ok"

    def remove_object(self, object_id: str) -> None:
        """Drop the object with its chunk and replica rows (cascade)."""
        with self._tx() as c:
            c.execute("DELETE FROM objects WHERE object_id = ?", (object_id,))

    def object_ids_in_state(self, state: str) -> list[str]:
        return [r["object_id"] for r in self._query("SELECT object_id FROM objects WHERE state = ?", (state,))]

    def count_objects(self) -> int:
        return self._query("SELECT COUNT(*) FROM objects WHERE state = 'COMMITTED'")[0][0]

    # -- replicas ----------------------------------------------------------
    def add_replica(self, chunk_id: str, worker_id: str) -> bool:
        """Record a confirmed copy. Returns False if the chunk no longer exists."""
        with self._tx() as c:
            cur = c.execute(
                "INSERT OR REPLACE INTO replicas (chunk_id, worker_id, state, created_at) "
                "SELECT ?, ?, 'HEALTHY', ? WHERE EXISTS (SELECT 1 FROM chunks WHERE chunk_id = ?)",
                (chunk_id, worker_id, utc_now(), chunk_id),
            )
            return cur.rowcount > 0

    def set_replica_state(self, chunk_id: str, worker_id: str, state: str) -> None:
        with self._tx() as c:
            c.execute(
                "UPDATE replicas SET state = ? WHERE chunk_id = ? AND worker_id = ?", (state, chunk_id, worker_id)
            )

    def remove_replica(self, chunk_id: str, worker_id: str) -> None:
        with self._tx() as c:
            c.execute("DELETE FROM replicas WHERE chunk_id = ? AND worker_id = ?", (chunk_id, worker_id))

    def replicas_for_worker(self, worker_id: str) -> dict[str, str]:
        rows = self._query("SELECT chunk_id, state FROM replicas WHERE worker_id = ?", (worker_id,))
        return {r["chunk_id"]: r["state"] for r in rows}

    def replica_counts_by_worker(self) -> dict[str, int]:
        rows = self._query("SELECT worker_id, COUNT(*) AS n FROM replicas GROUP BY worker_id")
        return {r["worker_id"]: r["n"] for r in rows}

    def chunk_checksums(self, chunk_ids: list[str]) -> dict[str, str]:
        """Checksums of the given chunks that the controller knows about."""
        known: dict[str, str] = {}
        for start in range(0, len(chunk_ids), 500):
            batch = chunk_ids[start : start + 500]
            marks = ",".join("?" * len(batch))
            for r in self._query(f"SELECT chunk_id, checksum FROM chunks WHERE chunk_id IN ({marks})", batch):
                known[r["chunk_id"]] = r["checksum"]
        return known

    def committed_chunks_with_replicas(self) -> list[dict]:
        """Every chunk of every committed object, with its replicas. Used by the repair loop."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT c.chunk_id, c.object_id, c.size, c.checksum, r.worker_id, r.state "
                "FROM chunks c JOIN objects o ON o.object_id = c.object_id "
                "LEFT JOIN replicas r ON r.chunk_id = c.chunk_id "
                "WHERE o.state = 'COMMITTED' ORDER BY c.object_id, c.chunk_index"
            ).fetchall()
        chunks: dict[str, dict] = {}
        for r in rows:
            chunk = chunks.setdefault(
                r["chunk_id"],
                {"chunk_id": r["chunk_id"], "object_id": r["object_id"], "size": r["size"],
                 "checksum": r["checksum"], "replicas": []},
            )
            if r["worker_id"] is not None:
                chunk["replicas"].append({"worker_id": r["worker_id"], "state": r["state"]})
        return list(chunks.values())

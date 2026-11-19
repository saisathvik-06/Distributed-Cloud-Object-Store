"""SQLite connection and schema.

One connection is shared by the whole process. Every access goes through the
Repository, which serialises use of the connection with a lock and is always
called from a worker thread (never from the event loop), so a slow disk can
not delay heartbeats.
"""
import os
import sqlite3

SCHEMA = """
CREATE TABLE IF NOT EXISTS objects (
    object_id       TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    size            INTEGER NOT NULL DEFAULT 0,
    content_type    TEXT NOT NULL,
    checksum        TEXT,
    chunk_count     INTEGER NOT NULL DEFAULT 0,
    custom_metadata TEXT NOT NULL DEFAULT '{}',
    state           TEXT NOT NULL CHECK (state IN ('UPLOADING', 'COMMITTED', 'DELETING')),
    created_at      TEXT NOT NULL,
    modified_at     TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS chunks (
    chunk_id    TEXT PRIMARY KEY,
    object_id   TEXT NOT NULL REFERENCES objects(object_id) ON DELETE CASCADE,
    chunk_index INTEGER NOT NULL,
    size        INTEGER NOT NULL,
    checksum    TEXT NOT NULL,
    UNIQUE (object_id, chunk_index)
);

CREATE TABLE IF NOT EXISTS replicas (
    chunk_id   TEXT NOT NULL REFERENCES chunks(chunk_id) ON DELETE CASCADE,
    worker_id  TEXT NOT NULL,
    state      TEXT NOT NULL CHECK (state IN ('HEALTHY', 'CORRUPT')),
    created_at TEXT NOT NULL,
    PRIMARY KEY (chunk_id, worker_id)
);

CREATE TABLE IF NOT EXISTS workers (
    worker_id  TEXT PRIMARY KEY,
    address    TEXT NOT NULL,
    first_seen TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_objects_state ON objects(state, created_at);
CREATE INDEX IF NOT EXISTS idx_replicas_worker ON replicas(worker_id);

-- Independent of object rows: physical cleanup must survive metadata deletion/restart.
CREATE TABLE IF NOT EXISTS pending_deletions (
    chunk_id TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    PRIMARY KEY (chunk_id, worker_id)
);
"""


def connect(path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    # isolation_level=None: transactions are opened and closed explicitly.
    conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(SCHEMA)
    return conn

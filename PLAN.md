# Mini Distributed Cloud Object Store — Implementation Plan

**Status:** plan only. No code has been written.
**Source:** professor's assignment PDF, "Cloud Computing — Infrastructure Project: Mini Distributed Cloud Object Store".
**Written:** 2026-11-16.

**Choices already made**

| Topic | Choice |
|---|---|
| Language / framework | Python + FastAPI for Controller and Workers |
| Client | Command-line client (runs in a container) plus scripted demos |
| Scope | Core only: exactly what the PDF requires, done cleanly |
| Deployment | Docker Compose, 1 controller + 4 workers, named volumes |

---

## 1. What is being built

A small object store with three kinds of process:

- **Controller (1).** The only thing a client talks to. It accepts uploads, cuts them into 1 MB chunks, decides which two workers hold each chunk, keeps all metadata, serves downloads by reassembling chunks, watches worker health, and repairs replication.
- **Workers (4).** Simple chunk servers. Each stores chunk files on its own persistent Docker volume and reports to the controller.
- **Client (1 or more).** A CLI that calls the controller's REST API. `curl` also works.

The client never learns where chunks live. All object data passes through the controller.

```
   CLI container / curl
            |
            |  REST  /api/v1/...            (host port 8000)
            v
      +------------+        metadata        +---------------------+
      | CONTROLLER |----------------------->| SQLite file on the  |
      +------------+                        | controller's volume |
        |        ^                          +---------------------+
        |        |
        |        |  register, heartbeat     (worker -> controller)
        |        |
        |  PUT / GET / DELETE chunk, list chunks   (controller -> worker)
        v        |
  +---------+ +---------+ +---------+ +---------+
  | worker1 | | worker2 | | worker3 | | worker4 |
  | volume1 | | volume2 | | volume3 | | volume4 |
  +---------+ +---------+ +---------+ +---------+
```

---

## 2. Requirements traceability

Every numbered section of the PDF, and where this plan covers it.

| PDF section | Requirement | Covered in |
|---|---|---|
| 2 | 1 controller, 4 workers, 1+ clients, 1 MB chunks, replication factor 2, Docker, persistent worker storage, no Kubernetes | Sections 4, 9, 10; Phase 0 |
| 3 | Unique Object ID; name, size, content type, created and modified times, checksum, chunk count, chunk info and locations; custom key-value metadata | Section 6 (data model), Section 7.1 (object document) |
| 4 | 1 MB chunks, smaller last chunk, unique chunk ID, order preserved | Section 8.1; Phase 3 |
| 4 | Two copies on different workers, spread across workers | Section 5 (placement); Phase 3 |
| 4 | Detect lost replication factor and restore it when possible | Sections 8.6 and 8.7; Phase 6 |
| 5.1 to 5.6 | Six client endpoints with stated status codes | Section 7.1; Phases 3 and 4 |
| 5.7 | Status codes 200, 201, 204, 400, 404, 409, 500, 503 with JSON error messages | Section 7.3; Phase 4 |
| 6 | Six internal worker endpoints | Section 7.2; Phases 1 and 2 |
| 7 | Detect unavailable workers, keep serving when a copy survives, demo with `docker stop worker2` / `docker start worker2` | Sections 8.5 to 8.7; Phase 6 |
| 8 | Detect corruption, never return corrupt data when a valid copy exists, restore the corrupt copy | Section 8.8; Phase 7 |
| 9 | Build, start and usage instructions | Phase 8 (README) |
| 10 | Six demonstrations | Section 11; Phase 8 |

---

## 3. Assumptions to confirm with the professor

The PDF leaves these open. The plan picks a reasonable reading for each; if the professor says otherwise, only the named part changes.

1. **"1 MB" means 1,048,576 bytes (1 MiB).** It is one configuration value, so switching to 1,000,000 is trivial.
2. **When to return 409.** No endpoint in the PDF needs it. Plan: return 409 when a PATCH or DELETE arrives for an object whose deletion is already in progress. Duplicate file names are allowed and do not conflict, because the Object ID is what identifies an object.
3. **`GET /internal/workers/{worker_id}/chunks` is served by the worker.** The worker is the only source of truth about what is on its disk. The controller calls it when a worker joins or rejoins.
4. **PATCH body is a flat JSON object** such as `{"owner": "sai", "course": "CC"}`. A key with value `null` removes that key (an extension beyond "add or update").
5. **Controller metadata is persisted** (SQLite on a volume), even though the PDF only demands persistence for workers. Without it, `docker compose down` followed by `up` would lose every object.
6. **A malformed Object ID returns 404**, the same as an unknown one.
7. **Where the demo is run.** Demo scripts are planned in PowerShell for this Windows machine. If the professor runs it on Linux or macOS, bash versions are needed (about one extra hour; noted in Phase 8).

---

## 4. Components and responsibilities

### Controller

- Client REST API (`/api/v1/...`).
- Receives worker registration and heartbeats.
- **Metadata store:** SQLite file on the `controller-data` volume.
- **Membership:** which workers exist and whether each is ALIVE or DEAD.
- **Placement:** picks two workers for each new chunk.
- **Repair loop:** one background task that restores replication factor 2, removes surplus copies, and repairs corrupt copies.
- **Reconciliation:** when a worker registers, compares the worker's actual chunk list with the metadata and fixes differences.
- Runs as **exactly one process**. Membership state and background tasks live in memory, so multiple server processes would disagree with each other.

### Worker

- Stores, returns and deletes chunk files under `/data` on its own volume.
- Verifies a checksum on every write and every read.
- Registers at startup, then heartbeats every few seconds.
- Has no knowledge of objects, only chunks. It never talks to other workers.

### Client

- CLI packaged as a container, because Python is not installed on this PC.
- Run with `docker compose run --rm client <command>`; a tiny `objstore.ps1` wrapper shortens this.
- Several can run at once, which satisfies "1 or more clients".

---

## 5. Key design decisions

| Decision | Why | Alternative not chosen |
|---|---|---|
| All data flows through the controller | PDF 5.2 says the client must not need worker locations; one place enforces integrity checks | Client talks to workers directly: faster, but leaks locations and complicates the client |
| Object ID is a UUID (version 4) | Unique without coordination; safe in URLs | Sequential numbers: guessable and need a counter |
| Chunk ID is `<object_id>-<5-digit index>` | Unique, and readable on disk during the demo; order is also stored as an explicit index column | Random UUID per chunk: gives no hint which object a file belongs to |
| SHA-256 per chunk and for the whole object | Per-chunk hash finds which copy is bad; whole-object hash lets the client prove the download matches the upload | CRC32 or MD5: faster but weaker, and speed is irrelevant at this scale |
| Controller holds the authoritative checksums; each worker also keeps a sidecar copy | Controller check is end-to-end; the worker's own check catches damage early | Trusting only the worker: a damaged sidecar would hide corruption |
| Chunks are immutable (written once, never modified) | A returning worker can never hold a stale version of a chunk, only a valid, orphaned or surplus one. This removes a whole class of consistency problems | Mutable chunks would need version numbers |
| Upload is visible only after every chunk has two confirmed copies | No half-written objects ever appear in list or download | Accepting with one copy and repairing later: weaker durability at the moment of success |
| Placement: least-loaded alive workers first, random tie-break | Even spread; random ties stop the same two workers always pairing up | Fixed round-robin pairs: when one worker dies, its fixed partner carries all the risk |
| Liveness by heartbeat timeout, measured on the controller's clock only | No dependence on clocks agreeing across containers | Controller polling workers: more traffic, same result |
| Wait a grace period after a worker dies before re-replicating | A quick restart should not trigger copying everything | Repair instantly: wasteful, and hides the "under-replicated" state in the demo |
| Reconcile inventory on every registration | One mechanism handles restart, orphan cleanup, lost files and surplus copies | Periodic full scans: more moving parts (out of scope) |
| Named Docker volumes, not bind mounts | The project folder is inside OneDrive; syncing chunk files can cause locks and slow I/O | Bind mounts: visible on the host but fragile here |
| SQLite from the Python standard library | Zero extra services, transactional, survives restarts | Postgres container: unnecessary weight. JSON file: no transactions |

### Invariants the code must always hold

1. An object is listed or downloadable only after all its chunks have two confirmed copies. The commit is a single database transaction.
2. A replica is recorded only after the worker confirmed a checksum-verified write.
3. The controller never sends a client bytes that fail the SHA-256 check against its own metadata.
4. A surplus copy is deleted only when more than two healthy copies on alive workers exist.
5. A chunk file the controller does not know about is garbage and is deleted, never adopted as a new object.

---

## 6. Data model

### 6.1 Controller metadata (SQLite)

**objects**

| Field | Meaning |
|---|---|
| object_id | UUID, primary key |
| name | Original file name (stored as text only, never used as a path) |
| size | Bytes |
| content_type | From the upload part; guessed from the file name if absent; default `application/octet-stream` |
| checksum | SHA-256 of the whole object |
| chunk_count | Number of chunks (0 for an empty file) |
| custom_metadata | JSON text holding the user's key-value pairs |
| state | UPLOADING, COMMITTED or DELETING |
| created_at, modified_at | UTC timestamps; modified_at changes on PATCH |

**chunks**

| Field | Meaning |
|---|---|
| chunk_id | Primary key, `<object_id>-<index>` |
| object_id | Owner object |
| chunk_index | Position in the object, starting at 0; unique per object |
| size | Bytes (the last chunk may be smaller) |
| checksum | SHA-256 of the chunk |

**replicas**

| Field | Meaning |
|---|---|
| chunk_id, worker_id | Together the primary key |
| state | HEALTHY or CORRUPT |
| created_at | When the copy was confirmed |

**workers**

| Field | Meaning |
|---|---|
| worker_id | `worker1` to `worker4`, primary key |
| address | For example `http://worker2:8000` |
| first_seen | Timestamp |

Liveness (last heartbeat time, ALIVE or DEAD, time of death) is kept **in memory**, not in SQLite, so a heartbeat never waits on the database.

### 6.2 Replication status of a chunk (derived, not stored)

Count the chunk's HEALTHY replicas on ALIVE workers:

| Count | Status |
|---|---|
| 2 | healthy |
| 1 | under-replicated |
| 0 | unavailable |
| 3 or more | over-replicated |

### 6.3 Worker disk layout

```
/data/chunks/<chunk_id>.chunk     the bytes
/data/chunks/<chunk_id>.meta      small JSON: size, sha256, stored_at
/data/tmp/                        in-progress writes, emptied at startup
```

Writes go to `/data/tmp` first, are flushed to disk, then renamed into place, so a crash can never leave a half-written chunk under its real name.

---

## 7. API contracts

### 7.1 Client API (served by the controller)

| Method and path | Request | Success | Errors |
|---|---|---|---|
| POST `/api/v1/objects` | `multipart/form-data`: `file` (required), `metadata` (optional JSON object) | 201, `Location` header, object document | 400 missing file or bad metadata; 503 fewer than 2 workers alive or a chunk could not be stored twice |
| GET `/api/v1/objects/{id}` | none | 200, raw bytes, stored `Content-Type`, `Content-Length`, `Content-Disposition`, `ETag` = checksum | 404; 503 a chunk has no valid reachable copy |
| GET `/api/v1/objects/{id}/metadata` | none | 200, object document | 404 |
| PATCH `/api/v1/objects/{id}/metadata` | JSON object of key to value; `null` removes a key | 200, updated object document | 400 body is not a flat JSON object; 404; 409 deletion in progress |
| DELETE `/api/v1/objects/{id}` | none | 204, empty body | 404; 409 deletion already in progress |
| GET `/api/v1/objects` | optional `limit` (default 100, max 1000) and `offset` | 200, `{objects, total, limit, offset}`; each entry has id, name, size, content type, created time | 400 bad paging values |

**Object document** (returned by upload, get metadata and patch):

- `object_id`, `name`, `size`, `content_type`, `checksum`, `chunk_count`, `chunk_size`, `created_at`, `modified_at`
- `custom_metadata`: the user's key-value pairs, kept separate so they can never collide with system fields
- `chunks`: ordered list; each has `chunk_id`, `index`, `size`, `checksum`, `status` (Section 6.2) and `replicas`, a list of `{worker_id, worker_state, replica_state}`

Custom metadata rules: keys are non-empty strings; values are strings, numbers or booleans; no nesting; at most 64 keys and 16 KB in total. Violations return 400.

**Two additions beyond the PDF** (needed for the demo, allowed because the PDF's list is a minimum):

- `GET /api/v1/admin/cluster`: each worker's state, seconds since last heartbeat, chunk count and bytes used, plus totals of healthy, under-replicated, unavailable and over-replicated chunks.
- `GET /health` on the controller and on each worker, used by Docker health checks.

### 7.2 Internal API

| Method and path | Served by | Called by | Contract |
|---|---|---|---|
| POST `/internal/workers/register` | Controller | Worker at startup, and whenever told it is unknown | Body: worker_id, address, chunk_count, used_bytes. Returns 200 with the heartbeat interval. Starts reconciliation for that worker |
| POST `/internal/workers/heartbeat` | Controller | Worker every 3 s | Body: worker_id, chunk_count, used_bytes, free_bytes. Returns 200. Returns 404 if the worker is unknown or currently marked DEAD, which tells the worker to register again |
| PUT `/internal/chunks/{chunk_id}` | Worker | Controller | Raw bytes plus an `X-Chunk-Checksum` header. 201 on success. 400 if the checksum does not match, the body exceeds the chunk size, or the ID is malformed |
| GET `/internal/chunks/{chunk_id}` | Worker | Controller | 200 with bytes and checksum header. 404 if absent. 500 with code `CHUNK_CORRUPT` if the bytes on disk no longer match the sidecar |
| DELETE `/internal/chunks/{chunk_id}` | Worker | Controller | 204. Deleting a chunk that is already gone also succeeds |
| GET `/internal/workers/{worker_id}/chunks` | Worker | Controller during reconciliation; you during the demo | List of `{chunk_id, size, checksum, stored_at}`. 404 if the ID in the path is not this worker |

Because every return from DEAD goes through `register`, reconciliation always runs before a returning worker is trusted.

The worker must validate `chunk_id` against a strict pattern before using it in a file path. This is the one place where a bad input could escape the data directory.

### 7.3 Error model

Every error is JSON in one shape: `{"error": {"code": "OBJECT_NOT_FOUND", "message": "Object 5f0c... does not exist"}}`.

| Status | When |
|---|---|
| 400 | Missing `file`, invalid metadata JSON, invalid paging values, PATCH body not a flat object |
| 404 | Unknown or malformed Object ID; unknown route |
| 409 | PATCH or DELETE on an object whose deletion is in progress |
| 500 | Unexpected failure (caught by a global handler; no stack trace in the response) |
| 503 | Fewer than 2 alive workers on upload; a chunk with no valid reachable copy on download |

FastAPI answers validation failures with 422 by default. A global handler must convert these to 400, since 422 is not in the PDF's list.

---

## 8. Core flows

### 8.1 Upload

1. Validate: `file` part present; `metadata`, if given, is a flat JSON object. Otherwise 400.
2. Require at least 2 ALIVE workers. Otherwise 503.
3. Generate the Object ID. Insert the object row with state UPLOADING.
4. Read the file in 1 MB slices. For each slice:
   - compute its SHA-256 and feed the same bytes into a running whole-object SHA-256;
   - insert the chunk row;
   - pick 2 workers (Section 5) and send the chunk to both at the same time;
   - if a worker fails, try the next alive worker not yet used for this chunk;
   - once 2 workers have confirmed, insert the 2 replica rows.
5. After the last slice, one transaction records size, chunk count, checksum and timestamps, and sets state to COMMITTED.
6. Return 201 with the object document.

If two copies cannot be obtained for any chunk: delete whatever was written (best effort), remove the rows, return 503. Only one chunk is held in memory at a time, so memory use does not grow with file size.

### 8.2 Download

1. Look up the object. Not found or not COMMITTED: 404.
2. Pre-check: every chunk must have at least one HEALTHY replica on an ALIVE worker. Otherwise 503.
3. Fetch and verify **chunk 0 before sending any response headers**, so a small object with no valid copy still gets a clean 503.
4. Stream chunks in index order. For each chunk, try its replicas in random order:
   - fetch, compute SHA-256, compare with the metadata;
   - match: send it to the client;
   - mismatch or `CHUNK_CORRUPT`: mark that replica CORRUPT, wake the repair loop, try the next replica;
   - connection error: try the next replica;
   - worker says 404: remove that replica row, wake the repair loop, try the next replica.
5. If a later chunk has no valid copy, the response has already started, so the status code cannot change. The controller aborts the connection. The client notices because the byte count is short of `Content-Length`, and the CLI also checks the whole-object SHA-256.

### 8.3 Metadata read and update

- GET builds the object document from the database. No worker is contacted.
- PATCH merges the given keys into `custom_metadata` inside one write transaction (so two simultaneous PATCHes cannot lose each other's keys), updates `modified_at`, and returns the document. Object data is untouched.

### 8.4 Delete

1. One transaction: if the object is COMMITTED, set DELETING. If missing: 404. If already DELETING: 409.
2. Delete every replica on ALIVE workers, in parallel with a concurrency limit. "Already gone" counts as success.
3. Remove the object, chunk and replica rows. Return 204.

Copies on a DEAD worker cannot be deleted now. They become orphans and are removed by reconciliation when that worker returns (Section 8.7).

### 8.5 Heartbeats and failure detection

- Worker: registers at startup, retrying with increasing delay until the controller answers. Then sends a heartbeat every 3 s. A 404 reply means "register again".
- Controller: a detector runs every second. A worker with no heartbeat for 10 s becomes DEAD and its time of death is recorded.
- The worker does **not** announce a clean shutdown. `docker stop` must look like a real failure so the detector is what notices.

### 8.6 Repair loop

One background task. It runs every 5 s and is also woken immediately by events (worker death grace expiring, corruption found, reconciliation finished).

For each chunk of a COMMITTED object:

- **No healthy copy on an alive worker:** nothing can be done. Log it and report it as unavailable.
- **Fewer than 2:** if the missing copy belongs to a worker that died less than 15 s ago, wait. Otherwise copy: read from a healthy source, verify the SHA-256, write to the least-loaded alive worker that does not already hold it, record the replica. If the source fails verification, mark it CORRUPT and try another source.
- **CORRUPT replica on an alive worker:** overwrite it in place from a healthy copy, then mark it HEALTHY.
- **More than 2:** delete surplus copies, CORRUPT ones first, then from the most-loaded worker.

After writing a new copy, re-check that the chunk still exists before recording it. If the object was deleted meanwhile, delete the new copy.

Repairs are suppressed for 20 s after the controller starts, so it does not copy everything while workers are still re-registering.

### 8.7 Reconciliation when a worker registers

The controller fetches the worker's chunk list and compares it with its replica rows for that worker:

| Finding | Action |
|---|---|
| Worker has a chunk the controller has no record of (object was deleted, or an upload was abandoned) | Tell the worker to delete it. Skip files younger than 60 s to avoid racing an upload in progress |
| Worker has a known chunk with a matching checksum but no replica row | Record it as a replica. If that makes three copies, the repair loop trims one |
| Controller has a replica row but the worker does not report the chunk | Remove the row. The repair loop re-replicates |
| Both agree | Nothing to do |

### 8.8 Corruption handling

- **Detection, layer 1 (worker):** on every read the worker recomputes the hash and compares it with its sidecar.
- **Detection, layer 2 (controller):** on every read, for downloads and for repair sources, the controller compares against its own metadata. This still works if a sidecar is damaged too.
- **Avoidance:** a corrupt copy is skipped and the other replica is served.
- **Restoration:** the repair loop overwrites the corrupt copy from the good one.
- **Known limit of Core scope:** corruption is found when a chunk is read, not before. A chunk nobody reads can stay silently damaged. A periodic background scrubber would close this gap and is listed under out of scope.

### 8.9 Controller restart

- Metadata comes back from SQLite.
- Objects left in UPLOADING are purged; objects left in DELETING are finished.
- Workers get 404 on their next heartbeat, re-register, and are reconciled. No manual steps.

---

## 9. Failure-mode summary

| Scenario | How it is noticed | What the system does | End state |
|---|---|---|---|
| One worker stops | No heartbeat for 10 s | Reads use the other copy; uploads use the remaining 3; after the grace period, lost copies are recreated | 2 copies of everything, on 3 workers |
| That worker returns after repair | It registers | Reconcile; the third copies are trimmed | Exactly 2 copies |
| That worker returns within the grace period | It registers | Reconcile only; nothing is copied | 2 copies |
| Two workers stop | No heartbeats | Chunks with a surviving copy are served and re-replicated onto the 2 survivors; chunks with both copies on the stopped pair return 503 | Partial availability until one returns |
| Three workers stop | No heartbeats | Chunks on the survivor are readable; uploads return 503; no repair possible | Under-replicated until workers return |
| A chunk file is corrupted | SHA-256 mismatch on read | Other copy is served; bad copy is overwritten | 2 healthy copies |
| Both copies corrupted | Mismatch on both | 503; corrupt bytes are never returned | Object needs re-upload |
| A chunk file is deleted from disk | Worker returns 404, or the file is absent at reconciliation | Treated as a lost copy and recreated | 2 copies |
| Worker dies during an upload | Chunk write fails | That chunk goes to another worker | Upload succeeds if 2 or more workers are alive |
| Object deleted while a worker is down | Not noticed at the time | Rows removed; leftover files deleted when the worker returns | No garbage |
| Controller restarts | Not applicable | See Section 8.9 | Consistent |
| Controller is down | Not applicable | Nothing works. The assignment specifies a single controller, so it is a single point of failure by design | Recovers on restart |

---

## 10. Deployment and configuration

### 10.1 Docker Compose layout

| Service | Container name | Host port | Volume | Notes |
|---|---|---|---|---|
| controller | `controller` | 8000 | `controller-data` at `/data` | Health check on `/health` |
| worker1 to worker4 | `worker1` to `worker4` | 8001 to 8004, bound to localhost only | `worker1-data` to `worker4-data` at `/data` | Same image, different `WORKER_ID`; start after the controller is healthy |
| client | none (one-off runs) | none | `./demo/work` mounted at `/work` | Under a `tools` profile so `docker compose up` does not start it |

Details that matter:

- **Fixed container names** are required so the PDF's exact commands `docker stop worker2` and `docker start worker2` work.
- Workers are addressed by **service name, never IP address**. A restarted container may get a new IP.
- No automatic restart policy on workers; a stopped worker must stay stopped for the demo.
- A short stop timeout (about 5 s) keeps `docker stop` quick.
- Worker ports are published only for inspection during the demo. Clients never use them.
- Kubernetes is not used anywhere.

### 10.2 Settings (environment variables, with defaults)

| Setting | Default | Where | Purpose |
|---|---|---|---|
| Chunk size | 1,048,576 bytes | Controller | Size of each chunk |
| Replication factor | 2 | Controller | Copies per chunk |
| Heartbeat interval | 3 s | Controller, handed to workers at registration | How often workers report |
| Worker timeout | 10 s | Controller | Silence before a worker is DEAD |
| Repair grace | 15 s | Controller | Wait after death before re-replicating |
| Repair interval | 5 s | Controller | How often the repair loop runs |
| Startup grace | 20 s | Controller | No repairs right after the controller starts |
| Worker request timeout | 2 s connect, 15 s transfer | Controller | Limits on calls to workers |
| Maximum object size | 2 GB | Controller | Larger uploads are rejected with 400 |
| Worker ID, controller address, advertised address | per worker | Worker | Identity and where to register |
| Controller address | `http://controller:8000` | Client | Where the CLI sends requests |

With these defaults, the failure demo timeline is: stop at 0 s, DEAD by about 10 to 13 s, repair starts at about 25 to 30 s and finishes within seconds.

---

## 11. Demonstration plan (PDF section 10)

One script per required demonstration. Each prints a banner per step and pauses for Enter so it can be narrated live. Keep `docker compose logs -f controller` open in a second terminal to show events as they happen.

| # | Demonstration | Steps | What proves it |
|---|---|---|---|
| 1 | Object storage | Generate a small file; upload; download; compare | SHA-256 of the download equals the original |
| 2 | Chunking and distribution | Upload a 5.5 MB file; show object info; list chunk files on each worker | 6 chunks (5 full, 1 partial), files present on all four workers |
| 3 | Replication | Show each chunk's two workers from the metadata; cross-check against each worker's own chunk list | Every chunk appears on exactly 2 different workers |
| 4 | Metadata | Upload with custom metadata; read it; PATCH it; read again | Keys changed, `modified_at` advanced, checksum and `created_at` unchanged, no data re-sent |
| 5 | Worker failure | Show the cluster before; `docker stop worker2`; show DEAD and under-replicated chunks; download and verify; wait for repair; show 2 copies restored; `docker start worker2`; show it ALIVE and surplus copies trimmed | Download succeeds during the outage; replication factor is 2 before, restored during, and exactly 2 after |
| 6 | Data integrity | Pick a chunk and a worker from the metadata; overwrite some bytes of that chunk file with `docker exec`; download and verify; show the controller log; show the replica repaired | Download still matches the original; log shows the mismatch; the damaged file's hash is correct again |

Optional extras if time allows: in demo 5, restart the worker inside the grace period to show that no copying happens; in demo 6, corrupt both copies to show a 503 instead of bad data.

A `reset` script (`docker compose down -v`, then `up`) gives a clean cluster before each rehearsal.

---

## 12. Repository layout

```
Object store/
  PLAN.md                     this file
  README.md                   build, start, usage (required by PDF section 9)
  DEMO.md                     step-by-step runbook for the six demonstrations
  docker-compose.yml
  objstore.ps1                wrapper around "docker compose run --rm client"
  .gitignore  .gitattributes  .dockerignore
  controller/
    Dockerfile
    requirements.txt
    app/
      main.py                 app setup; starts and stops background tasks
      config.py               settings from environment
      errors.py               error types and the global JSON error handlers
      db.py                   SQLite connection and schema creation
      repository.py           every SQL statement lives here
      schemas.py              request and response shapes
      api_objects.py          the six client endpoints
      api_internal.py         register and heartbeat
      api_admin.py            cluster status
      worker_client.py        HTTP calls to workers
      membership.py           worker registry and failure detector
      placement.py            choosing workers for a chunk
      upload.py               chunking and replicated writes
      download.py             verified streaming reads
      repair.py               the repair loop
      reconcile.py            inventory reconciliation
  worker/
    Dockerfile
    requirements.txt
    app/
      main.py                 the internal endpoints
      config.py
      storage.py              atomic write, verified read, delete, inventory
      registration.py         register and heartbeat loop
  client/
    Dockerfile
    requirements.txt
    objstore_cli.py
  demo/
    01_object_storage.ps1 ... 06_data_integrity.ps1
    reset.ps1
    work/                     sample and downloaded files (not committed)
```

Dependencies are kept minimal: FastAPI, Uvicorn, HTTPX and python-multipart for the services; HTTPX plus a small CLI and table library for the client. Pin exact versions in each `requirements.txt` during Phase 0.

---

## 13. Implementation phases

Build in this order. Each phase ends with an acceptance check; do not start the next phase until it passes. Commit after each phase. Time figures are rough estimates for one person.

### Phase 0: Scaffolding (about 0.5 day)

Tasks
- Decide whether to move the project out of OneDrive (see Section 14). Initialise git.
- Add `.gitignore`, `.dockerignore`, and `.gitattributes` forcing LF line endings.
- Create the folder layout, the Dockerfiles, and a `/health` endpoint in the controller and the worker.
- Write `docker-compose.yml` as described in Section 10.1.

Acceptance
- `docker compose up --build -d` gives 5 healthy containers.
- `curl.exe http://localhost:8000/health` succeeds.
- `docker stop worker2` and `docker start worker2` work by name.

### Phase 1: Worker chunk store (about 0.5 to 1 day)

Tasks
- Storage module: chunk ID validation, atomic write with sidecar, verified read, idempotent delete, inventory listing, temp folder cleanup at startup.
- The four worker endpoints from Section 7.2, with JSON errors and a size limit.

Acceptance
- Store a chunk with the right checksum: 201. With a wrong checksum: 400 and nothing is stored.
- Read returns identical bytes.
- After `docker restart worker1` the chunk is still there (persistence).
- After manually damaging the file, a read returns `CHUNK_CORRUPT`.
- Delete returns 204, twice in a row.
- The inventory lists exactly what is on disk.

### Phase 2: Membership and failure detection (about 1 day)

Tasks
- Controller: register and heartbeat endpoints, in-memory registry, workers table, detector task, cluster status endpoint.
- Worker: registration with retry, heartbeat loop, re-register on 404.
- Log lines for registered, dead and returned.

Acceptance
- Cluster status shows 4 ALIVE.
- After `docker stop worker2`, it shows DEAD within about 13 s. After `docker start worker2`, ALIVE within about 5 s.
- After `docker restart controller`, all 4 return to ALIVE with no manual action.

### Phase 3: Metadata store and upload (about 1 to 1.5 days)

Tasks
- SQLite schema and repository layer. Database calls run off the main event loop, with a single writer.
- Placement, the worker HTTP client, and the upload flow from Section 8.1.
- Request validation; cleanup when an upload fails; purge of UPLOADING objects at startup.

Acceptance
- Uploading a 3.5 MB file returns 201 with 4 chunks sized 1 MB, 1 MB, 1 MB and 0.5 MB.
- Every chunk has 2 replicas on different workers; all four workers hold something; the worker inventories total 8 files.
- With three workers stopped, upload returns 503 and leaves no rows or files behind.
- Missing `file` returns 400; malformed `metadata` returns 400.
- An empty file returns 201 with 0 chunks.

### Phase 4: Download, metadata, list, delete, error model (about 1 day)

Tasks
- Verified streaming download with correct headers.
- Metadata GET and PATCH; list with paging; the delete flow.
- Global error handlers (422 to 400, JSON for 404, 405 and 500); the 409 rule; finishing DELETING objects at startup.

Acceptance
- Round trip matches by SHA-256 for files of 300 KB, exactly 1 MB, 1 MB plus 1 byte, and 5.5 MB, and for a real image or PDF.
- `Content-Type` on download matches the upload.
- PATCH adds, updates and removes keys; `modified_at` changes; checksum and `created_at` do not.
- After delete: 204, then 404 on every endpoint for that ID, and its chunk files are gone from the workers.
- An unknown ID returns 404 with a JSON body.

### Phase 5: Client CLI (about 0.5 day)

Tasks
- Commands: `upload` (with `--meta key=value`), `download` (checks size and SHA-256), `info` (metadata plus a chunk-to-worker table), `set-meta`, `delete`, `list`, `cluster`, `verify` (compare a stored object with a local file), `gen` (create a random test file of a given size).
- Dockerfile, compose service, the `objstore.ps1` wrapper, the `demo/work` mount.

Acceptance
- Every API operation can be done from the CLI.
- `download` exits with a failure code if the checksum does not match.

### Phase 6: Failure handling and re-replication (about 1.5 days)

Tasks
- Replica failover on reads (Section 8.2).
- Chunk status (Section 6.2) shown in `info` and `cluster`.
- The repair loop (Section 8.6), reconciliation on registration (Section 8.7), startup grace.

Acceptance
- The full demo 5 sequence passes, in both the "long outage" and "quick restart" variants.
- Delete an object while worker2 is stopped; after it restarts, its leftover chunk files are removed.
- With two workers stopped, objects that still have a copy download correctly and the rest return 503 with a JSON error.

### Phase 7: Data integrity (about 0.5 to 1 day)

Tasks
- Controller-side verification on every chunk read, for downloads and repair sources.
- CORRUPT replica state and in-place repair.
- "Verify chunk 0 before headers" and stream abort on later failure.

Acceptance
- Demo 6 passes.
- Corrupting both copies gives 503, never bad data.
- Truncating a chunk file is detected. Deleting a chunk file is detected and the copy is restored.

### Phase 8: Demo scripts and documentation (about 1 day)

Tasks
- The six demo scripts and `reset.ps1`. Add bash versions if the demo will run on Linux or macOS.
- `README.md`: prerequisites, build, start, CLI usage, `curl.exe` examples, short architecture summary, settings table, known limitations.
- `DEMO.md`: the runbook with expected output at each step.
- Full dry run starting from `docker compose down -v`.

Acceptance
- On a clean checkout, following only the README, all six demos pass in order, twice in a row.

**Total:** roughly 7 to 9 focused days.

---

## 14. Risks and pitfalls

| Risk | Consequence | Mitigation |
|---|---|---|
| Project folder is inside OneDrive | Sync can lock files or slow builds; git repositories inside OneDrive are a known source of trouble | Chunk data is in named volumes, so it is unaffected. Recommended: move the project to a plain folder such as `C:\dev\object-store`, or pause OneDrive sync while working |
| Python is not installed on this PC | No local run; editor has no code completion | Everything runs in containers. Rebuilds are fast because dependency layers are cached. Installing Python locally is optional, for editor support only |
| In PowerShell, `curl` is an alias for a different command, and quoting JSON for native programs is awkward | Confusing errors when testing by hand | Use `curl.exe` explicitly, and prefer the CLI container for anything with JSON |
| FastAPI buffers the whole upload to a temporary file before the handler runs | Controller needs temporary disk space equal to the upload size; large uploads take longer | Acceptable for this scope. Memory stays bounded. A streaming parser is a later optimisation |
| Slow work on the controller's event loop (database calls, hashing) | Heartbeats are delayed and healthy workers are declared DEAD | Liveness is in memory only; database calls run in a thread pool; only 1 MB is hashed at a time |
| Controller started with more than one server process | Each process has its own membership and repair loop; behaviour becomes inconsistent | Start it with a single process, and say so in the Dockerfile and README |
| Workers start before the controller is ready | Registration fails | Health-check ordering in Compose, plus retry with backoff in the worker |
| Reused connections to a restarted worker are stale | First request after a restart fails | Treat connection errors as "try the next replica"; one retry on a fresh connection |
| Windows line endings in files copied into Linux images | Scripts fail inside containers | `.gitattributes` forcing LF |
| Repair starts before all workers have re-registered | Unnecessary copying after a controller restart | Startup grace (Section 8.6) |
| Reconciliation deletes a chunk that belongs to an upload in progress | Data loss for that upload | Chunk rows are inserted before data is sent, and files younger than 60 s are skipped |

---

## 15. Out of scope

Deliberately excluded under "Core only". Each could be added later without redesign.

- Periodic background scrubber that verifies every chunk on a timer.
- Automated test suite. The acceptance checks and demo scripts act as end-to-end tests.
- Web dashboard.
- Rebalancing data onto a worker after it rejoins.
- HTTP range requests and resumable uploads.
- Authentication and TLS. Internal endpoints are reachable only on the Compose network and localhost.
- More than one controller. The assignment specifies exactly one.
- Streaming the upload to workers while it is still arriving from the client.

---

## 16. How to execute this plan later

1. Get answers to Section 3 from the professor, or accept the stated assumptions.
2. Work through Section 13 one phase at a time, passing each acceptance check before moving on.
3. Treat Sections 5 to 9 as the specification. If an implementation detail conflicts with an invariant in Section 5, the invariant wins.
4. Rehearse Section 11 from a clean reset at least twice before the real demonstration.

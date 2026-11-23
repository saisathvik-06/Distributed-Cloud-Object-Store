# Mini Distributed Cloud Object Store

A small distributed object store built for a Cloud Computing infrastructure project.
It stores arbitrary binary objects (documents, images, video, audio, archives) by cutting them
into 1 MB chunks and keeping two copies of every chunk on different storage workers.

- **1 Controller**: the REST API, metadata, chunk placement, failure detection and repair.
- **4 Workers**: chunk servers, each with its own persistent Docker volume.
- **Client**: a command-line tool (runs in a container) or plain `curl`.
- **Deployment**: Docker Compose. No Kubernetes.

```
   CLI / curl
       |  REST /api/v1/...   (localhost:8000)
       v
  +------------+     metadata     +--------------------+
  | CONTROLLER |----------------->| SQLite on a volume |
  +------------+                  +--------------------+
     |      ^
     |      |  register, heartbeat          (worker -> controller)
     v      |
     PUT / GET / DELETE chunk, list chunks  (controller -> worker)
  +---------+ +---------+ +---------+ +---------+
  | worker1 | | worker2 | | worker3 | | worker4 |
  | volume  | | volume  | | volume  | | volume  |
  +---------+ +---------+ +---------+ +---------+
```

Clients upload and download through the controller. Metadata identifies replica workers without exposing their addresses.

## What it does

| Requirement | How it is met |
|---|---|
| Chunking | Objects are cut into 1,048,576-byte chunks; the last may be smaller. Chunk IDs are `<object_id>-<index>` and order is stored explicitly |
| Replication factor 2 | Every chunk is written to two different workers before the upload is acknowledged |
| Distribution | Least-loaded workers first, with a random tie-break |
| Metadata | Name, size, content type, created and modified times, SHA-256 checksum, chunk count, chunk locations, plus custom key-value metadata |
| Worker failure | Heartbeats every 3 s; a worker silent for 10 s is marked DEAD. Reads fail over to the other copy |
| Restoring replication | After a 15 s grace period, lost copies are re-created on other workers. When the worker returns, surplus copies are trimmed back to exactly 2 |
| Data integrity | Fresh SHA-256 verification on reads, periodic scrubbing and reconciliation. Replica pruning verifies surviving copies first |
| Persistence | Workers and the controller keep their data on named Docker volumes |

## Prerequisites

- Docker Desktop (or Docker Engine) with Docker Compose v2.
- The object store and containerized client run with Docker and Compose v2. The full integration test runner also requires Python 3.10+ on the host.

## Build and start

```bash
docker compose --profile tools build
```

```bash
docker compose up -d
```

Check that all five containers are healthy and all four workers are registered:

```bash
docker compose ps
```

```bash
docker compose run --rm client cluster
```

Stop the system (data is kept):

```bash
docker compose down
```

Stop and erase all stored data:

```bash
docker compose down -v
```

## Using the client

The client runs as a one-off container. Files are read from and written to `demo/work/`
(mounted at `/work` inside the container), so use file names relative to that folder.

Short wrappers:

- Windows PowerShell: `.\objstore.ps1 <command>`
- macOS / Linux (bash): `bash objstore.sh <command>`
- Any shell: `docker compose run --rm client <command>`

If PowerShell refuses to run `.\objstore.ps1` ("running scripts is disabled"), run
`Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass` once in that window; it only affects that window.

| Command | What it does |
|---|---|
| `gen 5.5MB big.bin` | Create a test file of random bytes in `demo/work/` |
| `upload big.bin --meta owner=sai --meta course=CC` | Upload a file with optional custom metadata |
| `list` | List stored objects |
| `info <object_id>` | Show metadata, chunks, and which workers hold each copy |
| `download <object_id> -o copy.bin` | Download and verify size and SHA-256 |
| `verify <object_id> big.bin` | Download and compare with a local file |
| `set-meta <object_id> owner=x --unset course` | Add, update or remove custom metadata |
| `delete <object_id>` | Delete the object and its chunks |
| `cluster` | Worker health and replication status |

Add `--json` to `upload`, `info`, `list`, `set-meta` or `cluster` for raw JSON.

Example session (PowerShell):

```powershell
.\objstore.ps1 gen 5.5MB big.bin
.\objstore.ps1 upload big.bin --meta owner=sai
.\objstore.ps1 list
.\objstore.ps1 info <object_id>
.\objstore.ps1 verify <object_id> big.bin
```

The same session on macOS / Linux (bash):

```bash
bash objstore.sh gen 5.5MB big.bin
bash objstore.sh upload big.bin --meta owner=sai
bash objstore.sh list
bash objstore.sh info <object_id>
bash objstore.sh verify <object_id> big.bin
```

## REST API

Base URL: `http://localhost:8000`. The examples below use Bash and `curl`. In Windows PowerShell, use `curl.exe` to invoke the curl executable.

| Method | Endpoint | Purpose | Success |
|---|---|---|---|
| POST | `/api/v1/objects` | Upload (`multipart/form-data`: `file` required, `metadata` optional JSON) | 201 |
| GET | `/api/v1/objects/{object_id}` | Download the object | 200 |
| GET | `/api/v1/objects/{object_id}/metadata` | System and custom metadata, with chunk locations | 200 |
| PATCH | `/api/v1/objects/{object_id}/metadata` | Add or update custom metadata (`null` removes a key) | 200 |
| DELETE | `/api/v1/objects/{object_id}` | Delete the object and its data | 204 |
| GET | `/api/v1/objects?limit=100&offset=0` | List objects | 200 |
| GET | `/api/v1/admin/cluster` | Worker health and replication summary (addition for the demo) | 200 |

Examples (bash):

```bash
curl -F "file=@photo.jpg" -F 'metadata={"owner":"sai","album":"trip"}' http://localhost:8000/api/v1/objects
```

```bash
OBJECT_ID='paste-the-ID-returned-by-upload'
curl -o copy.jpg "http://localhost:8000/api/v1/objects/$OBJECT_ID"
```

```bash
curl "http://localhost:8000/api/v1/objects/$OBJECT_ID/metadata"
```

```bash
curl -X PATCH -H "Content-Type: application/json" -d '{"album":"goa","owner":null}' "http://localhost:8000/api/v1/objects/$OBJECT_ID/metadata"
```

```bash
curl -X DELETE "http://localhost:8000/api/v1/objects/$OBJECT_ID"
```

Interactive API documentation is served at `http://localhost:8000/docs`.

### Errors

Every error is JSON in one shape:

```json
{"error": {"code": "OBJECT_NOT_FOUND", "message": "Object 5f0c... does not exist"}}
```

| Status | When |
|---|---|
| 400 | Missing `file`, invalid metadata, invalid paging values, PATCH body not a JSON object |
| 404 | Unknown or malformed Object ID, unknown route |
| 409 | PATCH or DELETE while the object is already being deleted |
| 500 | Unexpected internal error |
| 503 | Fewer than 2 workers available for an upload, or a chunk has no valid reachable copy |

### Internal API (controller and workers only)

| Method | Endpoint | Served by | Purpose |
|---|---|---|---|
| POST | `/internal/workers/register` | Controller | A worker joins or rejoins; triggers reconciliation |
| POST | `/internal/workers/heartbeat` | Controller | A worker reports that it is alive |
| PUT | `/internal/chunks/{chunk_id}` | Worker | Store a chunk (checksum sent in `X-Chunk-Checksum`) |
| GET | `/internal/chunks/{chunk_id}` | Worker | Read a chunk (verified against its stored checksum) |
| DELETE | `/internal/chunks/{chunk_id}` | Worker | Delete a chunk |
| GET | `/internal/workers/{worker_id}/chunks` | Worker | List the chunks on that worker's disk |

Worker ports are published on localhost (8001 to 8004) only so they can be inspected during the demo, for example
`curl http://localhost:8002/internal/workers/worker2/chunks`.

## Demonstrations

`DEMO.md` is the step-by-step runbook for the six required demonstrations. Each script comes in two
versions that do exactly the same thing: `.ps1` for Windows PowerShell and `.sh` for macOS / Linux.
Start Docker Desktop first and run them from the project folder.

Windows (PowerShell):

```powershell
powershell -ExecutionPolicy Bypass -File .\demo\reset.ps1                  # clean cluster (erases stored data)
powershell -ExecutionPolicy Bypass -File .\demo\run_all.ps1                # all six, pausing between steps
powershell -ExecutionPolicy Bypass -File .\demo\05_worker_failure.ps1      # a single demonstration
```

Add `-NoPause` to run without stopping. Optional variants: `-QuickRestart` (demo 5) and `-BothCopies` (demo 6).

macOS / Linux (bash):

```bash
bash demo/reset.sh                  # clean cluster (erases stored data)
bash demo/run_all.sh                # all six, pausing between steps
bash demo/05_worker_failure.sh      # a single demonstration
```

Add `--no-pause` to run without stopping. Optional variants: `--quick-restart` (demo 5) and `--both-copies` (demo 6).
The bash scripts need only Docker and `curl`, and work with the bash 3.2 that ships with macOS.

To watch the system react live, keep this running in a second terminal:

```bash
docker compose logs -f controller
```

## Configuration

Set in `docker-compose.yml` as environment variables of the controller.

| Variable | Default | Meaning |
|---|---|---|
| `CHUNK_SIZE_BYTES` | 1048576 | Chunk size |
| `REPLICATION_FACTOR` | 2 | Copies per chunk |
| `HEARTBEAT_INTERVAL_S` | 3 | How often workers report |
| `WORKER_TIMEOUT_S` | 10 | Silence before a worker is marked DEAD |
| `REPAIR_GRACE_S` | 15 | Wait after a worker dies before re-creating its copies |
| `REPAIR_INTERVAL_S` | 5 | How often the repair loop runs |
| `STARTUP_GRACE_S` | 20 | No repairs right after the controller starts |
| `MAX_OBJECT_SIZE_BYTES` | 0 | Optional upload size limit; 0 means no extra application limit |
| `MAX_METADATA_KEYS` | 0 | Optional total metadata key limit; checked after PATCH merge |
| `MAX_METADATA_BYTES` | 0 | Optional total serialized UTF-8 metadata size limit |
| `SCRUB_INTERVAL_S` | 30 | Periodically verify stored bytes even without downloads |
| `RECONCILE_INTERVAL_S` | 30 | Repeat inventory reconciliation and orphan cleanup |
| `DELETE_RETRY_INTERVAL_S` | 5 | Retry durable cleanup jobs |
| `ORPHAN_MIN_AGE_S` | 60 | Minimum age before deleting an unknown chunk |

## How it works

**Upload.** The controller creates the object in state `UPLOADING`, reads the file 1 MB at a time,
hashes each chunk, and writes it to two workers in parallel. If a worker fails, another is tried.
When every chunk has two confirmed copies, one database transaction marks the object `COMMITTED`.
If two copies cannot be obtained, the upload fails with HTTP 503. The controller cleans up partial data and retries outstanding deletions.

**Download.** All chunks are fetched in order, verified against their authoritative checksums, and spooled to a
temporary file. The complete object checksum and size are checked before sending response headers. An unavailable
later chunk therefore returns JSON 503 rather than a partial 200. The controller processes file data one chunk at a time and uses temporary disk space for each concurrent download. Once prepared, a download remains valid even
if the object is deleted. The spool is closed after streaming or cancellation.

**Failure detection.** A worker that has not sent a heartbeat for 10 seconds is marked DEAD.
A DEAD worker must register again, which always triggers reconciliation.

**Repair loop.** Every 5 seconds (or when woken), inspect replica state. Every 30 seconds, also scrub actual
bytes even for apparently healthy chunks. All reachable replicas are verified before repairing or pruning a chunk.
Missing/corrupt copies are restored from verified bytes. Surplus copies are deleted only after two other copies
have been verified. With no valid source, preserve the remaining files and report the chunk unavailable.

**Reconciliation.** Runs on registration and every 30 seconds. Inventory is only a hint: actual bytes are verified
before adoption. Recently written unknown files are initially preserved and revisited later. Object lifecycle locks
coordinate reconciliation, upload, verified download preparation, repair and deletion.

**Deletion.** The controller atomically records cleanup jobs for all registered workers before deleting object
metadata. Jobs survive controller restarts and are retried until physical deletion succeeds, including when an
offline worker returns. API deletion makes an object immediately unavailable; offline physical cleanup is eventual.

**Metadata.** Any valid JSON object is accepted, including nested values and upload-time nulls. PATCH merges top-level
keys; a top-level null deletes that key. Non-finite numbers and invalid Unicode are rejected. Optional limits are
applied to the merged metadata transactionally. Empty binary objects are supported.

**Controller restart.** Metadata is reloaded from SQLite, workers re-register automatically, interrupted uploads
are purged and interrupted deletions are finished.

## Design decisions

| Decision | Why |
|---|---|
| All data flows through the controller | The client never needs worker locations (as the brief requires), and every byte is checked in one place before it is returned |
| Object ID is a random UUID | Unique without coordination and safe in URLs; two files with the same name never clash |
| Chunk ID is `<object_id>-<5-digit index>` | Unique, readable on disk during the demo, and keeps the order; the index is also stored as a number |
| SHA-256 per chunk and per object | The chunk hash finds which copy is bad; the object hash proves a download matches the upload |
| Object contents are immutable after upload | Repair can rewrite a physical replica with the original verified bytes |
| An object is visible only after every chunk has two confirmed copies | Half-written objects never appear in a list or download |
| Least-loaded placement with a random tie-break | Spreads chunks evenly, and the same two workers are not always paired |
| 15 s grace period before re-copying | A worker that restarts quickly does not trigger a full copy of its data |
| Liveness measured only on the controller's clock | No dependence on clocks agreeing across containers |
| SQLite on a volume for metadata | Transactional and survives restarts, with no extra database service |
| Named Docker volumes, not bind mounts | Avoids file locking and slow I/O from folders synced by tools such as OneDrive |
| Exactly one controller process | Membership state and background tasks live in memory |

## Failure scenarios

| Scenario | What the system does | End state |
|---|---|---|
| One worker stops | Reads use the other copy; new uploads use the other three workers; after the grace period the lost copies are re-created | 2 copies of everything, on 3 workers |
| That worker returns after the repair | It registers and is reconciled; the extra copies are trimmed | Exactly 2 copies |
| That worker returns within the grace period | Reconciled only; nothing is copied | 2 copies |
| Two workers stop | Chunks with a surviving copy are served and re-copied onto the 2 survivors; chunks whose both copies were on the stopped pair return 503 | Partly available until one returns |
| Three workers stop | Chunks on the survivor can still be read; uploads return 503 | Under-replicated until workers return |
| A stored chunk is corrupted | The checksum mismatch is caught on a read or by the background scan; the valid copy is served and a fresh copy replaces the damaged one | 2 valid copies |
| Both copies of a chunk are corrupted | 503; corrupt bytes are never returned | The object must be uploaded again |
| A chunk file is deleted from a disk | Treated as a lost copy and re-created | 2 copies |
| A worker dies during an upload | That chunk goes to another worker | Upload succeeds if 2 or more workers are alive |
| An object is deleted while a worker is down | Cleanup jobs are kept and retried; the leftover files are removed when the worker returns | No leftover files |
| The controller restarts | The controller reloads metadata, discards incomplete uploads, and resumes pending deletions. Workers re-register automatically | Consistent |
| The controller is down | Nothing works until it restarts (single controller by design) | Recovers on restart |

## Assumptions

The brief leaves a few points open; this is how we read them.

- "1 MB" means 1,048,576 bytes. It is the `CHUNK_SIZE_BYTES` setting, so it is easy to change.
- 409 Conflict is returned for a PATCH or DELETE on an object whose deletion is already in progress. Duplicate file
  names are allowed, because the Object ID identifies an object.
- `GET /internal/workers/{worker_id}/chunks` is served by the worker, which is the only source of truth about what
  is on its own disk.
- Controller metadata is persisted too, so `docker compose down` followed by `up` keeps every object.
- A malformed Object ID returns 404, the same as an unknown one.

## Project layout

```
controller/app/   main, config, context, errors, db, repository, schemas, metadata,
                  api_objects, api_internal, api_admin,
                  membership, placement, upload, download, deletion, repair, reconcile,
                  worker_client, worker_errors
worker/app/       main, config, storage, registration
client/           objstore_cli.py
demo/             six demonstration scripts in .ps1 (Windows) and .sh (macOS / Linux), reset, run_all
tests/            unit, API and isolated Docker integration tests
objstore.ps1      client wrapper for Windows PowerShell
objstore.sh       client wrapper for macOS / Linux
docker-compose.yml
DEMO.md           demonstration runbook
```

## Known limitations

- The controller is a single point of failure. The assignment specifies exactly one controller.
- If both copies of a chunk are lost or corrupted, the object cannot be recovered and downloads return 503.
- Upload parsing and verified downloads use temporary disk space on the controller. Concurrent large transfers
  require sufficient disk capacity; optional upload limits can be configured.
- Per-object coordination is process-local. Run exactly one controller process (as configured in Docker).
- SHA-256 detects damaged bytes but cannot recover a chunk when no valid copy remains.
- There is no authentication or TLS; the system is meant to run locally.

## Automated verification

Run the dependency-free backend and real-disk regression tests:

```bash
python3 -m unittest discover -s tests -p test_core.py -v
```

No Python on your computer? Run the unit and API tests inside the controller image instead
(after `docker compose build`).

macOS / Linux (bash):

```bash
docker run --rm -v "$PWD:/project:ro" -w /project objstore-controller python -m unittest discover -s tests
```

Windows (PowerShell):

```powershell
docker run --rm -v "${PWD}:/project:ro" -w /project objstore-controller python -m unittest discover -s tests
```

Run the complete isolated Docker suite (host requires Python 3.10+ and Docker Compose v2):

```bash
python3 tests/run_docker.py
```

This builds the pinned images, runs backend and actual ASGI API tests inside a container, then runs 15 integration
scenarios against one controller and four workers. It uses a unique Compose project, dynamically assigned localhost
ports, and disposable volumes. The ordinary `objstore` deployment is untouched. Test containers and volumes are
removed afterward; `--keep` preserves them for debugging. Logs/results go to `test-results/`.

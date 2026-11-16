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

Clients only ever talk to the controller and never learn where chunks are stored.

## What it does

| Requirement | How it is met |
|---|---|
| Chunking | Objects are cut into 1,048,576-byte chunks; the last may be smaller. Chunk IDs are `<object_id>-<index>` and order is stored explicitly |
| Replication factor 2 | Every chunk is written to two different workers before the upload is acknowledged |
| Distribution | Least-loaded workers first, with a random tie-break |
| Metadata | Name, size, content type, created and modified times, SHA-256 checksum, chunk count, chunk locations, plus custom key-value metadata |
| Worker failure | Heartbeats every 3 s; a worker silent for 10 s is marked DEAD. Reads fail over to the other copy |
| Restoring replication | After a 15 s grace period, lost copies are re-created on other workers. When the worker returns, surplus copies are trimmed back to exactly 2 |
| Data integrity | SHA-256 per chunk, checked by the worker and again by the controller on every read. A corrupt copy is never returned; it is replaced from the valid copy |
| Persistence | Workers and the controller keep their data on named Docker volumes |

## Prerequisites

- Docker Desktop (or Docker Engine) with Docker Compose v2.
- Nothing else. Python is not needed on the host; everything runs in containers.

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

On Windows PowerShell there is a short wrapper: `.\objstore.ps1 <command>`.
On any shell: `docker compose run --rm client <command>`.

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

## REST API

Base URL: `http://localhost:8000`. In PowerShell use `curl.exe`, because `curl` is an alias for a different command.

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
curl -o copy.jpg http://localhost:8000/api/v1/objects/<object_id>
```

```bash
curl http://localhost:8000/api/v1/objects/<object_id>/metadata
```

```bash
curl -X PATCH -H "Content-Type: application/json" -d '{"album":"goa","owner":null}' http://localhost:8000/api/v1/objects/<object_id>/metadata
```

```bash
curl -X DELETE http://localhost:8000/api/v1/objects/<object_id>
```

Interactive API documentation is served at `http://localhost:8000/docs`.

### Errors

Every error is JSON in one shape:

```json
{"error": {"code": "OBJECT_NOT_FOUND", "message": "Object 5f0c... does not exist"}}
```

| Status | When |
|---|---|
| 400 | Missing `file`, invalid metadata, invalid paging values, PATCH body not a flat JSON object |
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

`DEMO.md` is the step-by-step runbook for the six required demonstrations. Scripts are in `demo/`:

```powershell
.\demo\reset.ps1                    # clean cluster (erases stored data)
.\demo\run_all.ps1                  # all six, pausing between steps
.\demo\05_worker_failure.ps1        # a single demonstration
```

Add `-NoPause` to run without stopping. If scripts are blocked, start PowerShell with
`powershell -ExecutionPolicy Bypass -File .\demo\run_all.ps1`.

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
| `MAX_OBJECT_SIZE_BYTES` | 2147483648 | Largest accepted upload |

## How it works

**Upload.** The controller creates the object in state `UPLOADING`, reads the file 1 MB at a time,
hashes each chunk, and writes it to two workers in parallel. If a worker fails, another is tried.
When every chunk has two confirmed copies, one database transaction marks the object `COMMITTED`.
If two copies cannot be obtained, everything written is removed and the client gets 503.

**Download.** Chunks are fetched in order. Each one is hashed and compared with the controller's metadata before
it is sent to the client. A copy that is corrupt, missing or unreachable is skipped and the other copy is used.

**Failure detection.** A worker that has not sent a heartbeat for 10 seconds is marked DEAD.
A DEAD worker must register again, which always triggers reconciliation.

**Repair loop.** Every 5 seconds (or immediately when something is detected) the controller checks every chunk:
corrupt copies are overwritten from a healthy copy, missing copies are re-created on another worker once the
grace period has passed, and surplus copies are deleted.

**Reconciliation.** When a worker registers, the controller compares the worker's real chunk list with its metadata:
unknown chunks (from deleted objects) are removed, matching chunks are adopted, and recorded copies that are
missing from the disk are scheduled for re-creation.

**Controller restart.** Metadata is reloaded from SQLite, workers re-register automatically, interrupted uploads
are purged and interrupted deletions are finished.

## Project layout

```
controller/app/   main, config, errors, db, repository, schemas,
                  api_objects, api_internal, api_admin,
                  membership, placement, upload, download, deletion, repair, reconcile, worker_client
worker/app/       main, config, storage, registration
client/           objstore_cli.py
demo/             six demonstration scripts, reset.ps1, run_all.ps1
docker-compose.yml
PLAN.md           design and implementation plan
DEMO.md           demonstration runbook
```

## Known limitations

- The controller is a single point of failure. The assignment specifies exactly one controller.
- Corruption is detected when a chunk is read (by a download or a repair), not by a periodic background scan.
- If both copies of a chunk are lost or corrupted, the object cannot be recovered and downloads return 503.
- A download that loses its last valid copy after the response has started is aborted mid-stream;
  the client detects this from the short byte count and the checksum.
- Uploads are buffered to a temporary file on the controller before chunking, so the controller needs
  temporary disk space equal to the upload size.
- There is no authentication or TLS; the system is meant to run locally.

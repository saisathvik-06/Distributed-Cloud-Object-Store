# Verification and regression coverage

## Latest local verification (2026-11-19)

- **49 backend/storage tests passed.** These use real SQLite databases and real worker chunk/sidecar files. The transport is simulated so outages, lost acknowledgements, and specific concurrent interleavings can be reproduced deterministically.
- **14 HTTP/ASGI tests were skipped**, not passed: this environment lacks the pinned FastAPI/httpx/multipart dependencies and could not install them.
- **Docker integration could not run**: accessing the Docker daemon socket returned permission denied. The runner exits at preflight without changing containers or volumes.
- Python source parsing and `git diff --check` passed at verification time.

Do not interpret local backend tests as proof that images build or HTTP/container integration passes. Run the full Docker command below before submission.

## Commands

From this repository:

```bash
python3 -m unittest discover -s tests -p test_core.py -v
python3 tests/run_docker.py
```

The Docker runner uses a new, uniquely named project and volumes, dynamic localhost ports, the repository's pinned dependencies, and exactly one controller plus four workers. It runs all core and HTTP tests in the controller image before exercising the live cluster. It cleans up only its own containers, images and volumes. Add `--keep` to retain a failed cluster; it prints the exact cleanup command.

For local HTTP tests when dependencies are available:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r controller/requirements.txt
.venv/bin/python -m unittest discover -s tests -v
```

On Windows, use `.venv\Scripts\python.exe` in place of `.venv/bin/python`.

## Coverage

| Layer | Checks |
|---|---|
| Core upload | Empty, one byte, 1 MiB minus/exact/plus one, multi-chunk, short input reads, unique IDs, distribution, two distinct replicas, concurrent uploads |
| Failure during upload | Worker fallback, insufficient workers, size limit, partial placement rollback, lost PUT acknowledgement, cancellation cleanup |
| Metadata/database | Concurrent merge, null deletion, nested JSON, non-finite/invalid Unicode rejection, merged limits, foreign keys, visible object states, durable cleanup queue |
| Integrity | Corrupt read fallback, dormant corruption scrub, missing files, no valid source, whole-object checksum, missing chunk rows, corrupt later chunk |
| Pruning | Three recorded replicas with only one valid copy must recover to two valid copies; never delete the last valid source |
| Lifecycle races | Repair overlapping delete, reconciliation overlapping upload, download snapshot surviving delete, cancellation during DB work, lock reclamation |
| Recovery | Worker death/grace/rejoin, interrupted upload state, controller restart with pending physical deletion, transient DELETE failure, orphan age and retry |
| Worker storage | Real disk persistence, checksums, size constraints, strict chunk IDs, large chunk indexes, concurrent writes and sidecar consistency |
| HTTP/ASGI | All six public endpoints, metadata fields, response bodies/headers/statuses, Unicode filenames, binary/content types, pagination, 400/404/409/500/503, concurrency, registration/heartbeat and worker APIs |
| Live Docker | API boundary round-trips, content types, invalid requests, concurrent operations, stop/start each worker, dormant corruption, corrupt later chunk, abrupt controller kill/restart, worker volume persistence, offline deletion plus controller restart, insufficient workers, interrupted state recovery, 32 MiB object, internal APIs, safe pruning, periodic orphan deletion |

The live suite uses faster configurable maintenance timings (1-second heartbeats, 4-second failure timeout, 2-second grace, 3-second scrub/reconciliation) to keep tests practical. Chunk size and replication factor remain the assignment defaults. Interrupted-operation recovery tests deterministically seed the persistent state that an interrupted operation leaves, then kill/restart the actual controller. They do not claim to exercise every possible operating-system crash instruction boundary.

## Important behavior

- Complete downloads are verified to temporary disk before any success headers are sent. This allows a clean JSON 503 even for an unavailable later chunk, at the cost of disk use and latency before the first response byte.
- Deletion is immediately logical and eventually physical for unavailable workers. Durable jobs remain until deletion is acknowledged. Periodic reconciliation also catches late/unrecorded orphan writes.
- A worker is not considered a verified replica merely because its sidecar or controller record says HEALTHY.
- All changes preserve the six required public routes and the existing metadata/list response shapes. Empty objects and nested JSON metadata are supported. PATCH top-level null removes a key.
- The existing database is migrated additively by `CREATE TABLE IF NOT EXISTS pending_deletions`; no existing objects are dropped during initialization.

# Demonstration runbook

Covers the six demonstrations required by the assignment. Each has a script in `demo/` that prints
every step, checks the result, and ends with `PASS` or `FAIL`.

## Before you start

1. Start from a clean cluster (this erases stored data):

   ```powershell
   .\demo\reset.ps1
   ```

2. In a **second terminal**, follow the controller log so the audience sees events as they happen:

   ```powershell
   docker compose logs -f controller
   ```

3. Run the demonstrations one at a time, or all together:

   ```powershell
   .\demo\run_all.ps1
   ```

Each script pauses for Enter between steps. Add `-NoPause` to run straight through.
If PowerShell blocks scripts, run them as `powershell -ExecutionPolicy Bypass -File .\demo\<script>.ps1`.

The whole set takes about three minutes without pauses. Demonstration 5 is the longest (about one minute)
because it waits for real timeouts.

---

## 1. Object storage: `.\demo\01_object_storage.ps1`

| Step | What happens | What to point out |
|---|---|---|
| 1.1 | A 300 KB random file is created | Any binary data works |
| 1.2 | Upload, `POST /api/v1/objects` | The system assigns a unique Object ID and returns the metadata |
| 1.3 | Download, `GET /api/v1/objects/{id}` | The client never sees worker addresses |
| 1.4 | SHA-256 of original, download and stored checksum | All three are identical |

## 2. Chunking and distribution: `.\demo\02_chunking_distribution.ps1`

| Step | What happens | What to point out |
|---|---|---|
| 2.1 | A 5.5 MB file is uploaded | Larger than one chunk |
| 2.2 | Object info lists the chunks | 6 chunks: five of 1,048,576 bytes and a final one of 524,288 bytes, in order, each with its own ID |
| 2.3 | `ls` of `/data/chunks` inside every worker | The chunk files really are spread over the workers' volumes |

## 3. Replication: `.\demo\03_replication.ps1`

| Step | What happens | What to point out |
|---|---|---|
| 3.2 | Object info shows "Copies stored on" for each chunk | Two different workers per chunk |
| 3.3 | Each worker is asked directly what it has on disk (`GET /internal/workers/{id}/chunks`) | Independent evidence, not just the controller's word |
| 3.4 | Metadata and disks are compared chunk by chunk | They agree, and every chunk has exactly 2 copies |

## 4. Metadata: `.\demo\04_metadata.ps1`

| Step | What happens | What to point out |
|---|---|---|
| 4.1 | Upload with `owner=sai`, `course=cloud-computing` | Custom key-value metadata at upload time |
| 4.2 | `GET .../metadata` | System metadata and custom metadata, no data download |
| 4.3 | `PATCH .../metadata`: change `owner`, add `status`, remove `course` | Only a small JSON body is sent |
| 4.4 | Before and after comparison | `modified_at` advanced; checksum, chunks and `created_at` unchanged, so nothing was re-uploaded |

## 5. Worker failure: `.\demo\05_worker_failure.ps1`

Timeline with the default settings: the worker is declared DEAD about 10 s after it stops, and its copies
are re-created about 15 s after that.

| Step | What happens | What to point out |
|---|---|---|
| 5.1 | Upload; cluster status | BEFORE: 4 workers ALIVE, every chunk healthy |
| 5.2 | `docker stop worker2` | The log shows `WORKER_DEAD worker2` after 10 s without a heartbeat |
| 5.3 | Cluster status and object info | DURING: the controller reports which chunks are under-replicated; "Replication factor is NOT maintained" |
| 5.4 | Download and compare | The object is still served, from the surviving copies |
| 5.5 | Wait for the grace period | The log shows `REPAIRED ...` lines; replication factor 2 is restored on the three remaining workers |
| 5.6 | `docker start worker2` | The worker registers again and is reconciled; the log shows `SURPLUS_COPY_REMOVED` as third copies are trimmed |
| 5.7 | Cluster status and object info | AFTER: 4 workers ALIVE, every chunk has exactly 2 copies |

Variant: `.\demo\05_worker_failure.ps1 -QuickRestart` restarts the worker inside the grace period,
showing that its copies are simply re-validated and nothing is copied.

## 6. Data integrity: `.\demo\06_data_integrity.ps1`

| Step | What happens | What to point out |
|---|---|---|
| 6.1 | Upload a 3.5 MB object | Note chunk 1 and the two workers holding it |
| 6.2 | Bytes inside one copy of chunk 1 are overwritten with `docker exec ... dd` | The file's SHA-256 on disk no longer matches the expected value |
| 6.3 | The object is downloaded and compared | Every download still matches the original; the log shows `CORRUPTION_DETECTED` |
| 6.4 | Wait for the repair loop | The log shows `REPAIRED`. The damaged copy is replaced: a fresh copy is written from the valid one (often on a different worker) and the damaged file is deleted, so the chunk again has two copies that pass the checksum |

Variant: `.\demo\06_data_integrity.ps1 -BothCopies` then corrupts both copies of chunk 0.
The download returns `503 DATA_UNAVAILABLE`: the system refuses rather than return corrupt data.

---

## Doing it by hand

The same demonstrations with individual commands, in case a step needs repeating.

```powershell
.\objstore.ps1 gen 5.5MB big.bin
.\objstore.ps1 upload big.bin --meta owner=sai
.\objstore.ps1 info <object_id>
.\objstore.ps1 cluster
docker stop worker2
.\objstore.ps1 cluster
.\objstore.ps1 verify <object_id> big.bin
docker start worker2
.\objstore.ps1 cluster
```

Corrupt one copy of a chunk (take the chunk ID and worker from `info`):

```powershell
docker exec worker1 sh -c "printf 'CORRUPTED' | dd of=/data/chunks/<chunk_id>.chunk bs=1 seek=4096 conv=notrunc"
.\objstore.ps1 verify <object_id> big.bin
docker logs controller --tail 20
```

A read picks one of the two copies at random, so run `verify` a few times until the damaged copy is hit.

## Log events to look for

| Event | Meaning |
|---|---|
| `WORKER_REGISTERED` | A worker joined or rejoined |
| `WORKER_DEAD` | No heartbeat for the timeout period |
| `REPAIRED` | A missing or damaged copy was re-created from a valid one |
| `SURPLUS_COPY_REMOVED` | A third copy was trimmed after a worker returned |
| `CORRUPTION_DETECTED` | A copy failed its SHA-256 check and was skipped |
| `REPLICA_MISSING` | A chunk file had vanished from a worker and will be re-created |
| `CHUNK_UNAVAILABLE` | No valid copy remains on any available worker |

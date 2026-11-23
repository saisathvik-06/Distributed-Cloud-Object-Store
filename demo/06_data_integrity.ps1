# Demonstration 6 - Data Integrity
# Intentionally corrupt stored data; show detection, that corrupt data is never returned,
# and that the damaged copy is restored.
#
# The controller finds corruption in two ways, whichever comes first:
#   * a download reads the damaged copy and its SHA-256 does not match, or
#   * the background scan (every 30 s) re-checks stored chunks.
# Repair writes a fresh copy from the valid one (possibly on a different worker)
# and deletes the damaged file, so the chunk again has two valid copies.
#
#   -BothCopies   additionally corrupt both copies of one chunk, to show a 503 instead of bad data.
param([switch]$NoPause, [switch]$BothCopies)
$global:DemoNoPause = [bool]$NoPause
. "$PSScriptRoot\_common.ps1"
Assert-ClusterReady

# SHA-256 of a chunk file on a worker's disk, or "(no file)" if it is not there.
function Get-DiskHash([string]$Worker, [string]$ChunkId) {
    $out = docker exec $Worker sha256sum "/data/chunks/$ChunkId.chunk" 2>$null
    if ($LASTEXITCODE -ne 0 -or -not $out) { return "(no file)" }
    return ($out -split " ")[0]
}

# Overwrite 17 bytes in the middle of a chunk file, directly on the worker's volume.
# Returns the file's SHA-256 measured in the same command, before anything can repair it.
function Corrupt-Chunk([string]$Worker, [string]$ChunkId) {
    $out = docker exec $Worker sh -c "printf 'CORRUPTED-BY-DEMO' | dd of=/data/chunks/${ChunkId}.chunk bs=1 seek=4096 conv=notrunc 2>/dev/null && sha256sum /data/chunks/${ChunkId}.chunk"
    if ($LASTEXITCODE -ne 0 -or -not $out) { Fail "could not corrupt $ChunkId on $Worker" }
    return ($out -split " ")[0]
}

# The copies of one chunk as the controller currently records them.
function Get-ChunkCopies([string]$ObjectId, [string]$ChunkId) {
    $c = (Get-ObjectInfo $ObjectId).chunks | Where-Object { $_.chunk_id -eq $ChunkId }
    return @($c.replicas)
}

Step "6.1  Upload a 3.5 MB object"
Invoke-Client gen 3.5MB demo6.bin
$obj = Get-ClientJson upload demo6.bin
Note "Object ID: $($obj.object_id)"
Invoke-Client info $obj.object_id
$chunk = $obj.chunks[1]
$worker = $chunk.replicas[0].worker_id
$other = $chunk.replicas[1].worker_id
$expected = $chunk.checksum
Pause-Demo

Step "6.2  Intentionally corrupt chunk 1 on $worker (its other copy is on $other)"
Write-Host "expected SHA-256 (controller metadata) : $expected"
Write-Host "on $worker before corruption           : $(Get-DiskHash $worker $chunk.chunk_id)"
$damaged = Corrupt-Chunk $worker $chunk.chunk_id
Write-Host "on $worker after corruption            : $damaged" -ForegroundColor Red
if ($damaged -eq $expected) { Fail "the file was not modified" }
Pause-Demo

Step "6.3  Download the object: corruption must be detected and the valid copy served"
Note "Detected by whichever comes first: a download reading the damaged copy, or the background scan (every 30 s)."
# Each download reads one of the two copies at random, so keep downloading until the
# corruption is detected. Every single download must still match the original.
$detected = $false
$downloads = 0
$deadline = (Get-Date).AddSeconds(120)
while (-not $detected -and (Get-Date) -lt $deadline) {
    Invoke-Client verify $obj.object_id demo6.bin
    if ($LASTEXITCODE -ne 0) { Fail "a download returned data that does not match the original" }
    $downloads++
    $detected = [bool](docker logs controller 2>&1 | Select-String -Pattern "CORRUPTION_DETECTED.*$($chunk.chunk_id)")
}
if (-not $detected) { Fail "corruption was not detected within 120 s" }
Show-ControllerLog "CORRUPTION_DETECTED.*$($chunk.chunk_id)" 2
Pass "corruption detected by checksum; all $downloads download(s) still matched the original"
Pause-Demo

Step "6.4  Restoration: the damaged copy is replaced from the valid copy"
Wait-Until {
    $c = Get-Cluster
    $copies = Get-ChunkCopies $obj.object_id $chunk.chunk_id
    $good = @($copies | Where-Object { $_.replica_state -eq "HEALTHY" -and (Get-DiskHash $_.worker_id $chunk.chunk_id) -eq $expected })
    ($c.replication.corrupt_replicas -eq 0) -and $c.replication.fully_replicated -and ($copies.Count -eq 2) -and ($good.Count -eq 2)
} "chunk 1 to have two valid copies again" 120
Show-ControllerLog "REPAIRED.*$($chunk.chunk_id)|SURPLUS_COPY_REMOVED.*$($chunk.chunk_id)" 4
$now = Get-ChunkCopies $obj.object_id $chunk.chunk_id
foreach ($r in $now) {
    Write-Host ("copy on {0,-8}: {1}" -f $r.worker_id, (Get-DiskHash $r.worker_id $chunk.chunk_id)) -ForegroundColor Green
}
Write-Host "expected        : $expected"
if (@($now.worker_id) -contains $worker) { Note "The damaged file on $worker was overwritten with a valid copy." }
else { Note "The damaged file on $worker was deleted, and a fresh valid copy was written to another worker." }
Invoke-Client info $obj.object_id
Invoke-Client cluster
Pass "damaged copy replaced; chunk 1 again has two copies that pass the checksum"

if ($BothCopies) {
    Pause-Demo
    Step "6.5  Extra: corrupt BOTH copies of chunk 0 - the system must refuse rather than return bad data"
    $first = $obj.chunks[0].chunk_id
    foreach ($r in (Get-ChunkCopies $obj.object_id $first)) {
        $h = Corrupt-Chunk $r.worker_id $first
        Write-Host "corrupted chunk 0 on $($r.worker_id): $h" -ForegroundColor Red
    }
    $status = 0
    for ($i = 1; $i -le 3; $i++) {
        $status = curl.exe -s -o NUL -w "%{http_code}" "$script:Api/objects/$($obj.object_id)"
        Write-Host "GET /api/v1/objects/$($obj.object_id) -> HTTP $status"
    }
    curl.exe -s "$script:Api/objects/$($obj.object_id)"
    Write-Host ""
    if ($status -eq "503") { Pass "no valid copy left: 503 returned, corrupt data never served" } else { Fail "expected 503, got $status" }
    Note "Cleaning up the unrecoverable object."
    Invoke-Client delete $obj.object_id
}

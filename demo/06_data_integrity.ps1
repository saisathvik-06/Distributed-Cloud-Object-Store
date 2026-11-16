# Demonstration 6 - Data Integrity
# Intentionally corrupt stored data; show detection, that corrupt data is never returned,
# and that the damaged copy is restored.
#
#   -BothCopies   additionally corrupt both copies of one chunk, to show a 503 instead of bad data.
param([switch]$NoPause, [switch]$BothCopies)
$global:DemoNoPause = [bool]$NoPause
. "$PSScriptRoot\_common.ps1"
Assert-ClusterReady

function Get-DiskHash([string]$Worker, [string]$ChunkId) {
    (docker exec $Worker sha256sum "/data/chunks/$ChunkId.chunk").Split(" ")[0]
}

function Corrupt-Chunk([string]$Worker, [string]$ChunkId) {
    # Overwrite 17 bytes in the middle of the chunk file, directly on the worker's volume.
    docker exec $Worker sh -c "printf 'CORRUPTED-BY-DEMO' | dd of=/data/chunks/${ChunkId}.chunk bs=1 seek=4096 conv=notrunc 2>/dev/null"
}

Step "6.1  Upload a 3.5 MB object"
Invoke-Client gen 3.5MB demo6.bin
$obj = Get-ClientJson upload demo6.bin
Note "Object ID: $($obj.object_id)"
Invoke-Client info $obj.object_id
$chunk = $obj.chunks[1]
$worker = $chunk.replicas[0].worker_id
$other = $chunk.replicas[1].worker_id
Pause-Demo

Step "6.2  Intentionally corrupt chunk 1 on $worker (its other copy is on $other)"
$expected = $chunk.checksum
Write-Host "expected SHA-256 (controller metadata) : $expected"
Write-Host "on $worker before corruption           : $(Get-DiskHash $worker $chunk.chunk_id)"
Corrupt-Chunk $worker $chunk.chunk_id
$damaged = Get-DiskHash $worker $chunk.chunk_id
Write-Host "on $worker after corruption            : $damaged" -ForegroundColor Red
if ($damaged -eq $expected) { Fail "the file was not modified" }
Pause-Demo

Step "6.3  Download the object: corruption must be detected and the valid copy served"
# Each read picks a replica at random, so read until the damaged copy has been hit.
$detected = $false
for ($i = 1; $i -le 12 -and -not $detected; $i++) {
    Invoke-Client verify $obj.object_id demo6.bin
    if ($LASTEXITCODE -ne 0) { Fail "a download returned data that does not match the original" }
    $detected = [bool](docker logs controller 2>&1 | Select-String -SimpleMatch "CORRUPTION_DETECTED chunk $($chunk.chunk_id)")
}
if (-not $detected) { Fail "corruption was not detected" }
Show-ControllerLog "CORRUPTION_DETECTED chunk $($chunk.chunk_id)" 1
Pass "corruption detected by checksum; every download still matched the original"
Pause-Demo

Step "6.4  Restoration: the damaged copy is overwritten from the valid copy"
Wait-Until {
    $c = Get-Cluster
    ($c.replication.corrupt_replicas -eq 0) -and $c.replication.fully_replicated -and ((Get-DiskHash $worker $chunk.chunk_id) -eq $expected)
} "the corrupt copy on $worker to be repaired"
Show-ControllerLog "REPAIRED_IN_PLACE chunk $($chunk.chunk_id)" 1
Write-Host "on $worker after repair : $(Get-DiskHash $worker $chunk.chunk_id)" -ForegroundColor Green
Write-Host "expected               : $expected"
Invoke-Client info $obj.object_id
Invoke-Client cluster
Pass "damaged copy restored; replication factor 2 with two valid copies"

if ($BothCopies) {
    Pause-Demo
    Step "6.5  Extra: corrupt BOTH copies of chunk 0 - the system must refuse rather than return bad data"
    foreach ($r in $obj.chunks[0].replicas) { Corrupt-Chunk $r.worker_id $obj.chunks[0].chunk_id }
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

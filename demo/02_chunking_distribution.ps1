# Demonstration 2 - Chunking and Distribution
# Upload an object larger than 1 MB; show it is cut into 1 MB chunks spread across the workers.
param([switch]$NoPause)
$global:DemoNoPause = [bool]$NoPause
. "$PSScriptRoot\_common.ps1"
Assert-ClusterReady

Step "2.1  Create and upload a 5.5 MB file"
Invoke-Client gen 5.5MB demo2.bin
$obj = Get-ClientJson upload demo2.bin
Note "Object ID: $($obj.object_id)"
Pause-Demo

Step "2.2  The object was divided into ordered 1 MB chunks (last chunk smaller)"
Invoke-Client info $obj.object_id
$sizes = @($obj.chunks | ForEach-Object { $_.size })
Write-Host "chunk sizes in order: $($sizes -join ', ')"
$full = @($sizes | Where-Object { $_ -eq 1048576 }).Count
if ($obj.chunk_count -eq 6 -and $full -eq 5 -and $sizes[5] -eq 524288) { Pass "6 chunks: 5 x 1,048,576 bytes + 1 x 524,288 bytes" }
else { Fail "unexpected chunking: $($sizes -join ', ')" }
Pause-Demo

Step "2.3  Chunk files of this object on each worker's persistent volume"
$holding = 0
foreach ($w in $script:Workers) {
    Write-Host "--- $w : /data/chunks" -ForegroundColor Magenta
    $files = docker exec $w sh -c "ls -l /data/chunks | grep $($obj.object_id) | grep '\.chunk'"
    if ($files) { $holding++; $files | ForEach-Object { Write-Host $_ } } else { Write-Host "(none)" }
}
if ($holding -ge 3) { Pass "chunks are distributed across $holding of the 4 workers" }
else { Fail "chunks are on only $holding workers" }

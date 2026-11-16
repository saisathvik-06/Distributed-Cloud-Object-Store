# Demonstration 3 - Replication
# Show that each chunk has two copies and where the copies are stored.
param([switch]$NoPause)
$global:DemoNoPause = [bool]$NoPause
. "$PSScriptRoot\_common.ps1"
Assert-ClusterReady

Step "3.1  Upload a 3.5 MB object"
Invoke-Client gen 3.5MB demo3.bin
$obj = Get-ClientJson upload demo3.bin
Note "Object ID: $($obj.object_id)"
Pause-Demo

Step "3.2  Controller metadata: where each chunk's copies are stored"
Invoke-Client info $obj.object_id
Pause-Demo

Step "3.3  Cross-check against what each worker really has on disk"
$onDisk = @{}
foreach ($w in $script:Workers) {
    $mine = @(Get-WorkerChunks $w | Where-Object { $_.chunk_id -like "$($obj.object_id)*" })
    Write-Host ("{0}: {1} chunk(s) of this object" -f $w, $mine.Count) -ForegroundColor Magenta
    foreach ($c in $mine) {
        Write-Host ("    {0}  {1} bytes  sha256 {2}..." -f $c.chunk_id, $c.size, $c.checksum.Substring(0, 12))
        if (-not $onDisk.ContainsKey($c.chunk_id)) { $onDisk[$c.chunk_id] = @() }
        $onDisk[$c.chunk_id] += $w
    }
}
Pause-Demo

Step "3.4  Result: every chunk has exactly two copies on two different workers"
$ok = $true
foreach ($chunk in $obj.chunks) {
    $recorded = @($chunk.replicas | ForEach-Object { $_.worker_id } | Sort-Object)
    $actual = @($onDisk[$chunk.chunk_id] | Sort-Object -Unique)
    $agree = (($recorded -join ",") -eq ($actual -join ","))
    Write-Host ("chunk {0}: metadata says [{1}]  disk says [{2}]" -f $chunk.index, ($recorded -join ", "), ($actual -join ", "))
    if ($actual.Count -ne 2 -or -not $agree) { $ok = $false }
}
if ($ok) { Pass "replication factor 2 holds for all $($obj.chunk_count) chunks, and metadata matches the disks" }
else { Fail "replication check failed" }
Invoke-Client cluster

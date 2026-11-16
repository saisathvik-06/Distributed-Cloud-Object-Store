# Demonstration 1 - Object Storage
# Upload and retrieve an object; verify that the retrieved object matches the original.
param([switch]$NoPause)
$global:DemoNoPause = [bool]$NoPause
. "$PSScriptRoot\_common.ps1"
Assert-ClusterReady

Step "1.1  Create a test file (300 KB of random bytes)"
Invoke-Client gen 300KB demo1.bin
Pause-Demo

Step "1.2  Upload it  (POST /api/v1/objects -> 201)"
$obj = Get-ClientJson upload demo1.bin
Note "Object ID assigned by the system: $($obj.object_id)"
Invoke-Client info $obj.object_id
Pause-Demo

Step "1.3  Download it  (GET /api/v1/objects/{id} -> 200)"
Invoke-Client download $obj.object_id -o demo1.downloaded.bin
Pause-Demo

Step "1.4  Verify the retrieved object matches the original (SHA-256)"
$original = (Get-FileHash "demo\work\demo1.bin" -Algorithm SHA256).Hash.ToLower()
$retrieved = (Get-FileHash "demo\work\demo1.downloaded.bin" -Algorithm SHA256).Hash.ToLower()
Write-Host "original   : $original"
Write-Host "downloaded : $retrieved"
Write-Host "stored     : $($obj.checksum)"
if ($original -eq $retrieved -and $original -eq $obj.checksum) { Pass "retrieved object is identical to the original" }
else { Fail "checksums differ" }

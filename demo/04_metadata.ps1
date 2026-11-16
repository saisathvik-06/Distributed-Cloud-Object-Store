# Demonstration 4 - Metadata
# Upload custom metadata; retrieve and modify it without re-uploading the object.
param([switch]$NoPause)
$global:DemoNoPause = [bool]$NoPause
. "$PSScriptRoot\_common.ps1"
Assert-ClusterReady

Step "4.1  Upload an object with custom metadata  (owner=sai, course=cloud-computing)"
Invoke-Client gen 2MB demo4.bin
$before = Get-ClientJson upload demo4.bin --meta owner=sai --meta course=cloud-computing
Note "Object ID: $($before.object_id)"
Pause-Demo

Step "4.2  Retrieve metadata without downloading data  (GET /api/v1/objects/{id}/metadata)"
Invoke-Client info $before.object_id
Pause-Demo

Step "4.3  Modify metadata  (PATCH /api/v1/objects/{id}/metadata): update owner, add status, remove course"
Start-Sleep -Seconds 1
Invoke-Client set-meta $before.object_id owner=sathvik status=reviewed --unset course
Pause-Demo

Step "4.4  Result"
$after = Get-ObjectInfo $before.object_id
Write-Host "custom metadata before : $($before.custom_metadata | ConvertTo-Json -Compress)"
Write-Host "custom metadata after  : $($after.custom_metadata | ConvertTo-Json -Compress)"
Write-Host "created_at   : $($before.created_at)  ->  $($after.created_at)"
Write-Host "modified_at  : $($before.modified_at)  ->  $($after.modified_at)"
Write-Host "checksum     : unchanged = $($before.checksum -eq $after.checksum)"
$sameChunks = (($before.chunks | ForEach-Object { $_.chunk_id }) -join ",") -eq (($after.chunks | ForEach-Object { $_.chunk_id }) -join ",")
$ok = ($after.custom_metadata.owner -eq "sathvik") -and ($after.custom_metadata.status -eq "reviewed") -and
      (-not ($after.custom_metadata.PSObject.Properties.Name -contains "course")) -and
      ($before.checksum -eq $after.checksum) -and ($before.created_at -eq $after.created_at) -and
      ($before.modified_at -ne $after.modified_at) -and $sameChunks
if ($ok) { Pass "metadata changed; data, checksum, chunks and creation time untouched (no re-upload)" }
else { Fail "metadata check failed" }

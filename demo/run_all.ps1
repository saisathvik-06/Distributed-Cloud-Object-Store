# Run all six required demonstrations in order.   .\demo\run_all.ps1 [-NoPause]
param([switch]$NoPause)

$scripts = @(
    "01_object_storage.ps1", "02_chunking_distribution.ps1", "03_replication.ps1",
    "04_metadata.ps1", "05_worker_failure.ps1", "06_data_integrity.ps1"
)
foreach ($name in $scripts) {
    if ($NoPause) { & "$PSScriptRoot\$name" -NoPause } else { & "$PSScriptRoot\$name" }
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Demonstration $name failed." -ForegroundColor Red
        exit 1
    }
}
Write-Host ""
Write-Host "All six demonstrations passed." -ForegroundColor Green

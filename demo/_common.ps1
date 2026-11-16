# Shared helpers for the demonstration scripts. Dot-sourced by each script.
# Works in Windows PowerShell 5.1 and PowerShell 7.

$ErrorActionPreference = "Continue"
$script:ProjectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $script:ProjectRoot

$script:Api = "http://localhost:8000/api/v1"
$script:Workers = @("worker1", "worker2", "worker3", "worker4")

function Step([string]$Text) {
    Write-Host ""
    Write-Host ("=" * 100) -ForegroundColor Cyan
    Write-Host "  $Text" -ForegroundColor Cyan
    Write-Host ("=" * 100) -ForegroundColor Cyan
}

function Note([string]$Text) { Write-Host $Text -ForegroundColor Yellow }

function Pass([string]$Text) { Write-Host "PASS: $Text" -ForegroundColor Green }

function Fail([string]$Text) {
    Write-Host "FAIL: $Text" -ForegroundColor Red
    exit 1
}

function Pause-Demo {
    if (-not $global:DemoNoPause) { [void](Read-Host "Press Enter to continue") }
}

# Run the command-line client and show its output.
function Invoke-Client {
    docker compose run --rm -T -e FORCE_COLOR=1 client @args 2>$null
}

# Run the command-line client with --json and return the parsed result.
function Get-ClientJson {
    $text = (docker compose run --rm -T client @args --json 2>$null | Out-String)
    if ($LASTEXITCODE -ne 0 -or -not $text.Trim()) { Fail "client command failed: $args" }
    return ($text | ConvertFrom-Json)
}

function Get-Cluster { Invoke-RestMethod "$script:Api/admin/cluster" }

function Get-ObjectInfo([string]$ObjectId) { Invoke-RestMethod "$script:Api/objects/$ObjectId/metadata" }

# The worker's own list of what is on its disk (worker ports are published on localhost for inspection).
function Get-WorkerChunks([string]$Worker) {
    $port = 8000 + [int]$Worker.Substring(6)
    (Invoke-RestMethod "http://localhost:$port/internal/workers/$Worker/chunks").chunks
}

# Poll until the condition holds; fail the demo if it does not within the timeout.
function Wait-Until([scriptblock]$Condition, [string]$What, [int]$TimeoutSeconds = 90) {
    Write-Host "waiting for: $What " -NoNewline
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        $ok = $false
        try { $ok = & $Condition } catch { $ok = $false }
        if ($ok) { Write-Host " done" -ForegroundColor Green; return }
        Write-Host "." -NoNewline
        Start-Sleep -Seconds 2
    }
    Write-Host ""
    Fail "timed out waiting for: $What"
}

function Assert-ClusterReady {
    try { $cluster = Get-Cluster } catch { Fail "the controller is not reachable; run 'docker compose up -d --build' first" }
    if ($cluster.alive_workers -lt 4) {
        Wait-Until { (Get-Cluster).alive_workers -eq 4 } "all 4 workers ALIVE"
    }
}

function Show-ControllerLog([string]$Pattern, [int]$Last = 12) {
    docker logs controller 2>&1 | Select-String -Pattern $Pattern | Select-Object -Last $Last | ForEach-Object { $_.Line }
}

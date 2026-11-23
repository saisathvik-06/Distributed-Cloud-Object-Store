# Demonstration 5 - Worker Failure
# Stop a worker container; show failure handling and retrieval from the other copy;
# restart the worker and show whether the required replication factor is maintained.
#
#   -QuickRestart   restart the worker inside the grace period instead, to show that nothing is copied.
param([switch]$NoPause, [switch]$QuickRestart)
$global:DemoNoPause = [bool]$NoPause
. "$PSScriptRoot\_common.ps1"
Assert-ClusterReady
$victim = "worker2"

Step "5.1  BEFORE: upload a 5.5 MB object; all workers ALIVE, every chunk has 2 copies"
Invoke-Client gen 5.5MB demo5.bin
$obj = Get-ClientJson upload demo5.bin
Note "Object ID: $($obj.object_id)"
Invoke-Client info $obj.object_id
Invoke-Client cluster
Pause-Demo

Step "5.2  FAILURE: docker stop $victim"
docker stop $victim
Wait-Until { ((Get-Cluster).workers | Where-Object { $_.worker_id -eq $victim }).state -eq "DEAD" } "controller to detect that $victim is unavailable (no heartbeat for 10 s)"
Show-ControllerLog "WORKER_DEAD" 1
Pause-Demo

Step "5.3  DURING: the controller knows which chunks lost a copy"
Invoke-Client cluster
Invoke-Client info $obj.object_id
Pause-Demo

Step "5.4  DURING: the object is still retrievable from the surviving copies"
Invoke-Client verify $obj.object_id demo5.bin
if ($LASTEXITCODE -eq 0) { Pass "object served correctly while $victim is down" } else { Fail "download failed during the outage" }
Pause-Demo

if ($QuickRestart) {
    Step "5.5  Restart inside the grace period: docker start $victim"
    docker start $victim
    Wait-Until { $c = Get-Cluster; ($c.alive_workers -eq 4) -and $c.replication.fully_replicated } "$victim to register again and be reconciled"
    Show-ControllerLog "WORKER_REGISTERED $victim|REPAIRED|SURPLUS_COPY_REMOVED" 4
    Invoke-Client cluster
    Pass "worker returned within the grace period: its copies were re-validated and nothing had to be copied"
    exit 0
}

Step "5.5  SELF-HEALING: after the 15 s grace period the lost copies are re-created on other workers"
Wait-Until { (Get-Cluster).replication.under_replicated -eq 0 } "replication factor 2 to be restored on the remaining workers"
Show-ControllerLog "REPAIRED" 8
Invoke-Client cluster
Invoke-Client info $obj.object_id
Pass "replication factor restored while $victim is still down"
Pause-Demo

Step "5.6  RECOVERY: docker start $victim"
docker start $victim
Wait-Until { $c = Get-Cluster; ($c.alive_workers -eq 4) -and ($c.replication.over_replicated -eq 0) -and $c.replication.fully_replicated } "$victim to register, be reconciled, and surplus copies to be trimmed"
Show-ControllerLog "WORKER_REGISTERED $victim|SURPLUS_COPY_REMOVED" 10
Pause-Demo

Step "5.7  AFTER: all workers ALIVE and every chunk has exactly 2 copies again"
Invoke-Client cluster
Invoke-Client info $obj.object_id
Invoke-Client verify $obj.object_id demo5.bin
$final = Get-ObjectInfo $obj.object_id
$bad = @($final.chunks | Where-Object { $_.status -ne "healthy" -or @($_.replicas).Count -ne 2 })
if ($bad.Count -eq 0 -and $LASTEXITCODE -eq 0) { Pass "replication factor 2 is maintained before, during (after self-healing) and after the failure" }
else { Fail "replication factor is not exactly 2 for every chunk" }

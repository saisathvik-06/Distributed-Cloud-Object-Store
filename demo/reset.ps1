# Wipe everything (containers AND all stored data) and start a clean cluster.
. "$PSScriptRoot\_common.ps1"

Step "Removing containers and volumes"
docker compose --profile tools down -v --remove-orphans

Step "Building and starting 1 controller + 4 workers"
docker compose --profile tools build
docker compose up -d

Wait-Until { (Get-Cluster).alive_workers -eq 4 } "all 4 workers to register with the controller"
Invoke-Client cluster

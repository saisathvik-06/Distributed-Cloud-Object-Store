#!/usr/bin/env bash
# Wipe everything (containers AND all stored data) and start a clean cluster.
#   bash demo/reset.sh
. "$(dirname "$0")/_common.sh"

step "Removing containers and volumes"
docker compose --profile tools down -v --remove-orphans

step "Building and starting 1 controller + 4 workers"
docker compose --profile tools build || fail "build failed"
docker compose up -d || fail "could not start the cluster"

controller_up() { curl -s "$API/admin/cluster" >/dev/null; }
wait_until "the controller to answer" 90 controller_up
wait_until "all 4 workers to register with the controller" 90 all_alive
client cluster

#!/usr/bin/env bash
# Demonstration 5 - Worker Failure
# Stop a worker container; show failure handling and retrieval from the other copy;
# restart the worker and show whether the required replication factor is maintained.
#   bash demo/05_worker_failure.sh [--no-pause] [--quick-restart]
#   --quick-restart   restart the worker inside the grace period instead, to show that nothing is copied.
. "$(dirname "$0")/_common.sh"
assert_cluster_ready
victim="worker2"

victim_dead() { [ "$(worker_state "$victim")" = "DEAD" ]; }
no_under_replicated() { [ "$(replication under_replicated)" = "0" ]; }
back_and_replicated() { all_alive && [ "$(replication fully_replicated)" = "True" ]; }
back_and_trimmed() { all_alive && [ "$(replication over_replicated)" = "0" ] && [ "$(replication fully_replicated)" = "True" ]; }

step "5.1  BEFORE: upload a 5.5 MB object; all workers ALIVE, every chunk has 2 copies"
client gen 5.5MB demo5.bin
json=$(client_json upload demo5.bin) || fail "upload failed"
obj=$(printf '%s' "$json" | json_field "d['object_id']")
note "Object ID: $obj"
client info "$obj"
client cluster
pause_demo

step "5.2  FAILURE: docker stop $victim"
docker stop "$victim"
wait_until "controller to detect that $victim is unavailable (no heartbeat for 10 s)" 90 victim_dead
show_controller_log "WORKER_DEAD" 1
pause_demo

step "5.3  DURING: the controller knows which chunks lost a copy"
client cluster
client info "$obj"
pause_demo

step "5.4  DURING: the object is still retrievable from the surviving copies"
if client verify "$obj" demo5.bin; then pass "object served correctly while $victim is down"; else fail "download failed during the outage"; fi
pause_demo

if [ "$QUICK_RESTART" = 1 ]; then
    step "5.5  Restart inside the grace period: docker start $victim"
    docker start "$victim"
    wait_until "$victim to register again and be reconciled" 90 back_and_replicated
    show_controller_log "WORKER_REGISTERED $victim|REPAIRED|SURPLUS_COPY_REMOVED" 4
    client cluster
    pass "worker returned within the grace period: its copies were re-validated and nothing had to be copied"
    exit 0
fi

step "5.5  SELF-HEALING: after the 15 s grace period the lost copies are re-created on other workers"
wait_until "replication factor 2 to be restored on the remaining workers" 120 no_under_replicated
show_controller_log "REPAIRED" 8
client cluster
client info "$obj"
pass "replication factor restored while $victim is still down"
pause_demo

step "5.6  RECOVERY: docker start $victim"
docker start "$victim"
wait_until "$victim to register, be reconciled, and surplus copies to be trimmed" 120 back_and_trimmed
show_controller_log "WORKER_REGISTERED $victim|SURPLUS_COPY_REMOVED" 10
pause_demo

step "5.7  AFTER: all workers ALIVE and every chunk has exactly 2 copies again"
client cluster
client info "$obj"
client verify "$obj" demo5.bin; verified=$?
healthy=$(api "/objects/$obj/metadata" "all(c['status'] == 'healthy' and len(c['replicas']) == 2 for c in d['chunks'])")
if [ "$healthy" = "True" ] && [ "$verified" = 0 ]; then
    pass "replication factor 2 is maintained before, during (after self-healing) and after the failure"
else
    fail "replication factor is not exactly 2 for every chunk"
fi

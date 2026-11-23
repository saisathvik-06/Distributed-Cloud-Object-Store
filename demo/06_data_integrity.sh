#!/usr/bin/env bash
# Demonstration 6 - Data Integrity
# Intentionally corrupt stored data; show detection, that corrupt data is never returned,
# and that the damaged copy is restored.
#
# The controller finds corruption in two ways, whichever comes first:
#   * a download reads the damaged copy and its SHA-256 does not match, or
#   * the background scan (every 30 s) re-checks stored chunks.
# Repair writes a fresh copy from the valid one (possibly on a different worker)
# and deletes the damaged file, so the chunk again has two valid copies.
#
#   bash demo/06_data_integrity.sh [--no-pause] [--both-copies]
#   --both-copies   additionally corrupt both copies of one chunk, to show a 503 instead of bad data.
. "$(dirname "$0")/_common.sh"
assert_cluster_ready

# SHA-256 of a chunk file on a worker's disk, or "(no file)" if it is not there.
disk_hash() {
    local out
    out=$(docker exec "$1" sha256sum "/data/chunks/$2.chunk" 2>/dev/null) || { echo "(no file)"; return 0; }
    echo "${out%% *}"
}

# Overwrite 17 bytes in the middle of a chunk file, directly on the worker's volume.
# Prints the file's SHA-256 measured in the same command, before anything can repair it.
corrupt_chunk() {
    local out
    out=$(docker exec "$1" sh -c "printf 'CORRUPTED-BY-DEMO' | dd of=/data/chunks/$2.chunk bs=1 seek=4096 conv=notrunc 2>/dev/null && sha256sum /data/chunks/$2.chunk") || return 1
    echo "${out%% *}"
}

# The copies of one chunk as the controller records them, as "worker:STATE" words.
chunk_copies() {
    api "/objects/$obj/metadata" "' '.join(r['worker_id'] + ':' + r['replica_state'] for c in d['chunks'] if c['chunk_id'] == '$1' for r in c['replicas'])"
}

two_valid_copies() {
    local copies entry w state n=0 good=0
    [ "$(replication corrupt_replicas)" = "0" ] || return 1
    [ "$(replication fully_replicated)" = "True" ] || return 1
    copies=$(chunk_copies "$cid")
    for entry in $copies; do
        n=$((n + 1)); w=${entry%%:*}; state=${entry#*:}
        if [ "$state" = "HEALTHY" ] && [ "$(disk_hash "$w" "$cid")" = "$expected" ]; then good=$((good + 1)); fi
    done
    [ "$n" = 2 ] && [ "$good" = 2 ]
}

step "6.1  Upload a 3.5 MB object"
client gen 3.5MB demo6.bin
json=$(client_json upload demo6.bin) || fail "upload failed"
obj=$(printf '%s' "$json" | json_field "d['object_id']")
read -r cid worker other expected <<< "$(printf '%s' "$json" | json_field "' '.join([d['chunks'][1]['chunk_id'], d['chunks'][1]['replicas'][0]['worker_id'], d['chunks'][1]['replicas'][1]['worker_id'], d['chunks'][1]['checksum']])")"
first=$(printf '%s' "$json" | json_field "d['chunks'][0]['chunk_id']")
note "Object ID: $obj"
client info "$obj"
pause_demo

step "6.2  Intentionally corrupt chunk 1 on $worker (its other copy is on $other)"
echo "expected SHA-256 (controller metadata) : $expected"
echo "on $worker before corruption           : $(disk_hash "$worker" "$cid")"
damaged=$(corrupt_chunk "$worker" "$cid") || fail "could not corrupt $cid on $worker"
printf '%son %s after corruption            : %s%s\n' "$RED" "$worker" "$damaged" "$RESET"
[ "$damaged" != "$expected" ] || fail "the file was not modified"
pause_demo

step "6.3  Download the object: corruption must be detected and the valid copy served"
note "Detected by whichever comes first: a download reading the damaged copy, or the background scan (every 30 s)."
# Each download reads one of the two copies at random, so keep downloading until the
# corruption is detected. Every single download must still match the original.
detected=0
downloads=0
deadline=$(( $(date +%s) + 120 ))
while [ "$detected" = 0 ] && [ "$(date +%s)" -lt "$deadline" ]; do
    client verify "$obj" demo6.bin || fail "a download returned data that does not match the original"
    downloads=$((downloads + 1))
    if docker logs controller 2>&1 | grep -qE "CORRUPTION_DETECTED.*$cid"; then detected=1; fi
done
[ "$detected" = 1 ] || fail "corruption was not detected within 120 s"
show_controller_log "CORRUPTION_DETECTED.*$cid" 2
pass "corruption detected by checksum; all $downloads download(s) still matched the original"
pause_demo

step "6.4  Restoration: the damaged copy is replaced from the valid copy"
wait_until "chunk 1 to have two valid copies again" 120 two_valid_copies
show_controller_log "REPAIRED.*$cid|SURPLUS_COPY_REMOVED.*$cid" 4
now=$(chunk_copies "$cid")
for entry in $now; do
    w=${entry%%:*}
    printf '%scopy on %-8s: %s%s\n' "$GREEN" "$w" "$(disk_hash "$w" "$cid")" "$RESET"
done
echo "expected        : $expected"
case " $now " in
    *" $worker:"*) note "The damaged file on $worker was overwritten with a valid copy." ;;
    *) note "The damaged file on $worker was deleted, and a fresh valid copy was written to another worker." ;;
esac
client info "$obj"
client cluster
pass "damaged copy replaced; chunk 1 again has two copies that pass the checksum"

if [ "$BOTH_COPIES" = 1 ]; then
    pause_demo
    step "6.5  Extra: corrupt BOTH copies of chunk 0 - the system must refuse rather than return bad data"
    for entry in $(chunk_copies "$first"); do
        w=${entry%%:*}
        h=$(corrupt_chunk "$w" "$first") || fail "could not corrupt $first on $w"
        printf '%scorrupted chunk 0 on %s: %s%s\n' "$RED" "$w" "$h" "$RESET"
    done
    status=0
    for i in 1 2 3; do
        status=$(curl -s -w '\n%{http_code}' "$API/objects/$obj" | tail -n 1)
        echo "GET /api/v1/objects/$obj -> HTTP $status"
    done
    curl -s "$API/objects/$obj"
    echo
    if [ "$status" = "503" ]; then pass "no valid copy left: 503 returned, corrupt data never served"; else fail "expected 503, got $status"; fi
    note "Cleaning up the unrecoverable object."
    client delete "$obj"
fi

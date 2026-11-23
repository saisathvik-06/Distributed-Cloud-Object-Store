#!/usr/bin/env bash
# Demonstration 1 - Object Storage
# Upload and retrieve an object; verify that the retrieved object matches the original.
#   bash demo/01_object_storage.sh [--no-pause]
. "$(dirname "$0")/_common.sh"
assert_cluster_ready

step "1.1  Create a test file (300 KB of random bytes)"
client gen 300KB demo1.bin
pause_demo

step "1.2  Upload it  (POST /api/v1/objects -> 201)"
json=$(client_json upload demo1.bin) || fail "upload failed"
obj=$(printf '%s' "$json" | json_field "d['object_id']")
stored=$(printf '%s' "$json" | json_field "d['checksum']")
note "Object ID assigned by the system: $obj"
client info "$obj"
pause_demo

step "1.3  Download it  (GET /api/v1/objects/{id} -> 200)"
client download "$obj" -o demo1.downloaded.bin
pause_demo

step "1.4  Verify the retrieved object matches the original (SHA-256)"
original=$(file_sha256 demo/work/demo1.bin)
retrieved=$(file_sha256 demo/work/demo1.downloaded.bin)
echo "original   : $original"
echo "downloaded : $retrieved"
echo "stored     : $stored"
if [ "$original" = "$retrieved" ] && [ "$original" = "$stored" ]; then
    pass "retrieved object is identical to the original"
else
    fail "checksums differ"
fi

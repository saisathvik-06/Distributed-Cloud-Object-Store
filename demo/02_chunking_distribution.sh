#!/usr/bin/env bash
# Demonstration 2 - Chunking and Distribution
# Upload an object larger than 1 MB; show it is cut into 1 MB chunks spread across the workers.
#   bash demo/02_chunking_distribution.sh [--no-pause]
. "$(dirname "$0")/_common.sh"
assert_cluster_ready

step "2.1  Create and upload a 5.5 MB file"
client gen 5.5MB demo2.bin
json=$(client_json upload demo2.bin) || fail "upload failed"
obj=$(printf '%s' "$json" | json_field "d['object_id']")
note "Object ID: $obj"
pause_demo

step "2.2  The object was divided into ordered 1 MB chunks (last chunk smaller)"
client info "$obj"
sizes=$(printf '%s' "$json" | json_field "', '.join(str(c['size']) for c in d['chunks'])")
echo "chunk sizes in order: $sizes"
if [ "$sizes" = "1048576, 1048576, 1048576, 1048576, 1048576, 524288" ]; then
    pass "6 chunks: 5 x 1,048,576 bytes + 1 x 524,288 bytes"
else
    fail "unexpected chunking: $sizes"
fi
pause_demo

step "2.3  Chunk files of this object on each worker's persistent volume"
holding=0
for w in $WORKERS; do
    heading "--- $w : /data/chunks"
    files=$(docker exec "$w" sh -c "ls -l /data/chunks | grep $obj | grep '\.chunk'")
    if [ -n "$files" ]; then holding=$((holding + 1)); echo "$files"; else echo "(none)"; fi
done
if [ "$holding" -ge 3 ]; then
    pass "chunks are distributed across $holding of the 4 workers"
else
    fail "chunks are on only $holding workers"
fi

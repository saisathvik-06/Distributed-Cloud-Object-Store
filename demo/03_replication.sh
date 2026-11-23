#!/usr/bin/env bash
# Demonstration 3 - Replication
# Show that each chunk has two copies and where the copies are stored.
#   bash demo/03_replication.sh [--no-pause]
. "$(dirname "$0")/_common.sh"
assert_cluster_ready

step "3.1  Upload a 3.5 MB object"
client gen 3.5MB demo3.bin
json=$(client_json upload demo3.bin) || fail "upload failed"
obj=$(printf '%s' "$json" | json_field "d['object_id']")
note "Object ID: $obj"
pause_demo

step "3.2  Controller metadata: where each chunk's copies are stored"
client info "$obj"
pause_demo

step "3.3  Cross-check against what each worker really has on disk"
for w in $WORKERS; do
    port=$((8000 + ${w#worker}))
    listing=$(curl -s "http://localhost:$port/internal/workers/$w/chunks" | json_field "'\n'.join('    %s  %s bytes  sha256 %s...' % (c['chunk_id'], c['size'], c['checksum'][:12]) for c in d['chunks'] if c['chunk_id'].startswith('$obj'))")
    count=0
    if [ -n "$listing" ]; then count=$(printf '%s\n' "$listing" | wc -l | tr -d ' '); fi
    heading "$w: $count chunk(s) of this object"
    [ -n "$listing" ] && echo "$listing"
done
pause_demo

step "3.4  Result: every chunk has exactly two copies on two different workers"
# Compare the controller's records with each worker's own list of the files on its disk.
if docker exec controller python -c "
import json, sys, urllib.request as u
obj = sys.argv[1]
meta = json.load(u.urlopen('http://127.0.0.1:8000/api/v1/objects/' + obj + '/metadata'))
disk = {}
for i in range(1, 5):
    w = 'worker%d' % i
    for c in json.load(u.urlopen('http://%s:8000/internal/workers/%s/chunks' % (w, w)))['chunks']:
        if c['chunk_id'].startswith(obj):
            disk.setdefault(c['chunk_id'], []).append(w)
ok = True
for c in meta['chunks']:
    recorded = sorted(r['worker_id'] for r in c['replicas'])
    actual = sorted(disk.get(c['chunk_id'], []))
    print('chunk %d: metadata says [%s]  disk says [%s]' % (c['index'], ', '.join(recorded), ', '.join(actual)))
    ok = ok and len(actual) == 2 and recorded == actual
sys.exit(0 if ok else 1)
" "$obj"; then
    pass "replication factor 2 holds for every chunk, and metadata matches the disks"
else
    fail "replication check failed"
fi
client cluster

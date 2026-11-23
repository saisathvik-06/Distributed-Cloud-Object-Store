#!/usr/bin/env bash
# Demonstration 4 - Metadata
# Upload custom metadata; retrieve and modify it without re-uploading the object.
#   bash demo/04_metadata.sh [--no-pause]
. "$(dirname "$0")/_common.sh"
assert_cluster_ready

step "4.1  Upload an object with custom metadata  (owner=sai, course=cloud-computing)"
client gen 2MB demo4.bin
before=$(client_json upload demo4.bin --meta owner=sai --meta course=cloud-computing) || fail "upload failed"
obj=$(printf '%s' "$before" | json_field "d['object_id']")
note "Object ID: $obj"
pause_demo

step "4.2  Retrieve metadata without downloading data  (GET /api/v1/objects/{id}/metadata)"
client info "$obj"
pause_demo

step "4.3  Modify metadata  (PATCH /api/v1/objects/{id}/metadata): update owner, add status, remove course"
sleep 1
client set-meta "$obj" owner=sathvik status=reviewed --unset course
pause_demo

step "4.4  Result"
if printf '%s' "$before" | docker exec -i controller python -c "
import json, sys, urllib.request as u
b = json.load(sys.stdin)
a = json.load(u.urlopen('http://127.0.0.1:8000/api/v1/objects/' + b['object_id'] + '/metadata'))
print('custom metadata before :', json.dumps(b['custom_metadata']))
print('custom metadata after  :', json.dumps(a['custom_metadata']))
print('created_at   :', b['created_at'], ' -> ', a['created_at'])
print('modified_at  :', b['modified_at'], ' -> ', a['modified_at'])
print('checksum     : unchanged =', a['checksum'] == b['checksum'])
m = a['custom_metadata']
ok = (m.get('owner') == 'sathvik' and m.get('status') == 'reviewed' and 'course' not in m
      and a['checksum'] == b['checksum'] and a['created_at'] == b['created_at']
      and a['modified_at'] != b['modified_at']
      and [c['chunk_id'] for c in a['chunks']] == [c['chunk_id'] for c in b['chunks']])
sys.exit(0 if ok else 1)
"; then
    pass "metadata changed; data, checksum, chunks and creation time untouched (no re-upload)"
else
    fail "metadata check failed"
fi

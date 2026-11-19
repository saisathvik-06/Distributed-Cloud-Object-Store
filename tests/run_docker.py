#!/usr/bin/env python3
"""Build and extensively test an isolated five-container cluster. Uses only host stdlib.

Usage: python3 tests/run_docker.py [--keep]
Never operates on the ordinary objstore project, its containers, or its volumes.
"""
import argparse
import concurrent.futures
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.error
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parents[1]
CLUSTER = None


def request(url, method='GET', data=None, headers=None):
    req=urllib.request.Request(url,data=data,method=method,headers=headers or {})
    try:
        response=urllib.request.urlopen(req,timeout=90)
    except urllib.error.HTTPError as exc:
        response=exc
    with response:
        return response.status,response.read(),dict(response.headers.items())


def decode(result):
    status,body,headers=result
    return status,json.loads(body) if body else None,headers


def multipart(data, name='file.bin', metadata=None, content_type='application/octet-stream'):
    boundary='test-'+uuid.uuid4().hex
    body=(f'--{boundary}\r\nContent-Disposition: form-data; name="file"; filename="{name}"\r\n'
          f'Content-Type: {content_type}\r\n\r\n').encode()+data+b'\r\n'
    if metadata is not None:
        body+=(f'--{boundary}\r\nContent-Disposition: form-data; name="metadata"\r\n\r\n'
               f'{json.dumps(metadata)}\r\n').encode()
    body+=f'--{boundary}--\r\n'.encode()
    return body,{'Content-Type':f'multipart/form-data; boundary={boundary}'}


def eventually(fn, timeout=90):
    deadline=time.monotonic()+timeout;last=None
    while time.monotonic()<deadline:
        try:
            result=fn()
            if result:return result
        except (OSError,ValueError,KeyError,AssertionError,subprocess.CalledProcessError) as exc:
            last=exc
        time.sleep(.25)
    raise AssertionError(f'Condition did not become true in {timeout}s; last error: {last}')


class Cluster:
    def __init__(self):
        self.project='objstore-test-'+uuid.uuid4().hex[:10]
        self.temp=tempfile.TemporaryDirectory(prefix='objstore-compose-')
        self.file=Path(self.temp.name)/'compose.json'
        self.results=ROOT/'test-results'/self.project
        self.results.mkdir(parents=True,exist_ok=True)
        health={'test':['CMD','python','-c',"import urllib.request; urllib.request.urlopen('http://localhost:8000/health',timeout=2)"],
                'interval':'1s','timeout':'3s','retries':60,'start_period':'2s'}
        settings={'HEARTBEAT_INTERVAL_S':'1','WORKER_TIMEOUT_S':'4','REPAIR_GRACE_S':'2','REPAIR_INTERVAL_S':'1',
                  'STARTUP_GRACE_S':'2','SCRUB_INTERVAL_S':'3','RECONCILE_INTERVAL_S':'3',
                  'DELETE_RETRY_INTERVAL_S':'1','ORPHAN_MIN_AGE_S':'2'}
        services={'controller':{'build':str(ROOT/'controller'),'ports':['127.0.0.1::8000'],
                                'volumes':['controller-data:/data'],'environment':settings,'healthcheck':health}}
        volumes={'controller-data':{}}
        for i in range(1,5):
            w=f'worker{i}';volumes[w+'-data']={}
            services[w]={'build':str(ROOT/'worker'),'ports':['127.0.0.1::8000'],
                         'volumes':[w+'-data:/data'],'environment':{'WORKER_ID':w,'CONTROLLER_URL':'http://controller:8000'},
                         'depends_on':{'controller':{'condition':'service_healthy'}},'healthcheck':health}
        self.file.write_text(json.dumps({'services':services,'volumes':volumes}))
        self.urls={}

    def compose(self,*args, capture=True):
        return subprocess.run(['docker','compose','-p',self.project,'-f',str(self.file),*args],
                              check=True,text=True,stdout=subprocess.PIPE if capture else None,
                              stderr=subprocess.PIPE if capture else None).stdout

    def refresh_ports(self):
        for service in ['controller','worker1','worker2','worker3','worker4']:
            address=self.compose('port',service,'8000').strip().splitlines()[0]
            self.urls[service]='http://'+address

    def ready(self):
        self.refresh_ports()
        eventually(lambda:all(request(url+'/health')[0]==200 for url in self.urls.values()))
        eventually(lambda:decode(request(self.urls['controller']+'/api/v1/admin/cluster'))[1]['alive_workers']==4)

    def python(self,service,code,*args):
        return self.compose('exec','-T',service,'python','-c',code,*args).strip()

    def inventory(self):
        result={}
        for w in ['worker1','worker2','worker3','worker4']:
            status,body,_=decode(request(self.urls[w]+f'/internal/workers/{w}/chunks'))
            if status!=200:raise AssertionError((status,body))
            result[w]=body['chunks']
        return result

    def physical(self,cids):
        return [(w,c['chunk_id']) for w,chunks in self.inventory().items() for c in chunks if c['chunk_id'] in cids]

    def corrupt(self,w,cid):
        self.python(w,"from pathlib import Path; import sys; Path('/data/chunks/'+sys.argv[1]+'.chunk').write_bytes(b'CORRUPTED')",cid)


class DockerTests(unittest.TestCase):
    def setUp(self):
        self.c=CLUSTER;self.c.ready();self.ids=[]

    @property
    def base(self):
        return self.c.urls['controller']

    def tearDown(self):
        # Restart any workers left stopped by an assertion so later tests remain useful.
        self.c.compose('start','controller','worker1','worker2','worker3','worker4')
        self.c.ready()
        for oid in self.ids:request(self.base+'/api/v1/objects/'+oid,'DELETE')

    def upload(self,data=b'hello',name='file.bin',metadata=None,ct='application/octet-stream'):
        body,headers=multipart(data,name,metadata,ct)
        status,obj,_=decode(request(self.base+'/api/v1/objects','POST',body,headers))
        self.assertEqual(status,201,obj);self.ids.append(obj['object_id']);return obj

    def get(self,oid):return request(self.base+'/api/v1/objects/'+oid)

    def metadata(self,oid):return decode(request(self.base+'/api/v1/objects/'+oid+'/metadata'))[1]

    def healthy(self,oid):
        obj=self.metadata(oid)
        return all(c['status']=='healthy' and len(c['replicas'])==2 for c in obj['chunks'])

    def test_01_boundaries_binary_unicode_and_all_endpoints(self):
        for size in [0,1,1048575,1048576,1048577,3145741]:
            with self.subTest(size=size):
                data=(bytes(range(256))*(size//256+1))[:size]
                obj=self.upload(data,'测试.bin',{'owner':'aj','nested':{'list':[1,None,True]}});oid=obj['object_id']
                self.assertEqual(obj['size'],size);self.assertEqual(obj['chunk_count'],(size+1048575)//1048576)
                self.assertEqual(obj['checksum'],hashlib.sha256(data).hexdigest())
                status,body,h=self.get(oid);self.assertEqual(status,200);self.assertEqual(body,data)
                self.assertEqual(h.get('Content-Type',h.get('content-type')),'application/octet-stream')
                cids={c['chunk_id'] for c in obj['chunks']}
                self.assertEqual(len(self.c.physical(cids)),2*len(cids))
                status,patched,_=decode(request(self.base+f'/api/v1/objects/{oid}/metadata','PATCH',
                    b'{"owner":null,"new":42}',{'Content-Type':'application/json'}))
                self.assertEqual(status,200);self.assertNotIn('owner',patched['custom_metadata'])
                self.assertEqual(patched['created_at'],obj['created_at'])
                self.assertEqual(self.get(oid)[1],data)
                self.assertEqual(request(self.base+'/api/v1/objects/'+oid,'DELETE')[0],204)
                eventually(lambda:not self.c.physical(cids))
                self.assertEqual(self.get(oid)[0],404)

    def test_02_content_types(self):
        for ct in ['application/pdf','image/png','video/mp4','audio/mpeg','application/zip','application/octet-stream']:
            obj=self.upload(b'\x00\xffarbitrary','file',ct=ct)
            status,data,headers=self.get(obj['object_id']);self.assertEqual(status,200)
            self.assertEqual(headers.get('Content-Type',headers.get('content-type')),ct)

    def test_03_invalid_requests_and_pagination(self):
        self.assertEqual(request(self.base+'/api/v1/objects','POST')[0],400)
        for q in ['limit=0','limit=-1','limit=x','offset=-1','offset=x']:
            self.assertEqual(request(self.base+'/api/v1/objects?'+q)[0],400)
        obj=self.upload();oid=obj['object_id']
        for value in [b'[]',b'null',b'{',b'{"x":NaN}',b'{"x":1e999}']:
            status,body,_=decode(request(self.base+f'/api/v1/objects/{oid}/metadata','PATCH',value,{'Content-Type':'application/json'}))
            self.assertEqual(status,400);self.assertIn('message',body['error'])
        for method,suffix in [('GET',''),('GET','/metadata'),('PATCH','/metadata'),('DELETE','')]:
            data=b'{}' if method=='PATCH' else None
            self.assertEqual(request(self.base+'/api/v1/objects/'+str(uuid.uuid4())+suffix,method,data,{'Content-Type':'application/json'})[0],404)
        self.upload();status,listing,_=decode(request(self.base+'/api/v1/objects?limit=1&offset=1'))
        self.assertEqual(status,200);self.assertEqual(len(listing['objects']),1)

    def test_04_concurrent_upload_patch_download_delete(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
            objs=list(pool.map(lambda i:self.upload(bytes([i])*(1048576+11)),range(12)))
            for i,obj in enumerate(objs):self.assertEqual(self.get(obj['object_id'])[1],bytes([i])*(1048576+11))
            oid=objs[0]['object_id'];url=self.base+f'/api/v1/objects/{oid}/metadata'
            results=list(pool.map(lambda i:request(url,'PATCH',json.dumps({str(i):i}).encode(),{'Content-Type':'application/json'})[0],range(40)))
            self.assertEqual(results,[200]*40);self.assertEqual(len(self.metadata(oid)['custom_metadata']),40)
            results=list(pool.map(lambda _:request(self.base+f'/api/v1/objects/{oid}','DELETE')[0],range(5)))
            self.assertEqual(sorted(results),[204,404,404,404,404])

    def test_05_stop_and_restart_every_worker(self):
        data=b'z'*(4*1048576+17);obj=self.upload(data);oid=obj['object_id']
        for w in ['worker1','worker2','worker3','worker4']:
            with self.subTest(worker=w):
                self.c.compose('stop','-t','1',w)
                eventually(lambda:any(x['worker_id']==w and x['state']=='DEAD' for x in decode(request(self.base+'/api/v1/admin/cluster'))[1]['workers']))
                self.assertEqual(self.get(oid)[1],data)
                eventually(lambda:all(sum(r['worker_state']=='ALIVE' and r['replica_state']=='HEALTHY' for r in c['replicas'])==2 for c in self.metadata(oid)['chunks']))
                self.c.compose('start',w);self.c.ready();eventually(lambda:self.healthy(oid))
                self.assertEqual(self.get(oid)[1],data)
                self.assertEqual(len(self.c.physical({c['chunk_id'] for c in obj['chunks']})),2*len(obj['chunks']))

    def test_06_dormant_corruption_scrub(self):
        obj=self.upload();chunk=obj['chunks'][0];cid=chunk['chunk_id'];w=chunk['replicas'][0]['worker_id']
        self.c.corrupt(w,cid)
        # No controller download is issued: background verification must find it.
        def repaired():
            copies=self.c.physical({cid})
            if len(copies)!=2:return False
            return all(request(self.c.urls[worker]+'/internal/chunks/'+cid)[1]==b'hello' for worker,_ in copies)
        eventually(repaired)
        self.assertEqual(self.get(obj['object_id'])[1],b'hello')

    def test_07_all_copies_of_later_chunk_corrupt_clean_503(self):
        obj=self.upload(b'x'*(1048576+9));chunk=obj['chunks'][1]
        self.c.compose('stop','-t','1','controller')
        for rep in chunk['replicas']:self.c.corrupt(rep['worker_id'],chunk['chunk_id'])
        self.c.compose('start','controller');self.c.ready()
        status,body,_=decode(self.get(obj['object_id']));self.assertEqual(status,503);self.assertIn('message',body['error'])

    def test_08_controller_and_worker_volume_persistence(self):
        obj=self.upload(b'persistent'*180000);oid=obj['object_id']
        self.c.compose('kill','-s','SIGKILL','controller');self.c.compose('start','controller');self.c.ready()
        self.assertEqual(self.get(oid)[1],b'persistent'*180000)
        self.c.compose('restart','worker1','worker2','worker3','worker4');self.c.ready()
        eventually(lambda:self.get(oid)[0]==200)
        self.assertEqual(self.get(oid)[1],b'persistent'*180000)

    def test_09_offline_delete_survives_controller_restart(self):
        obj=self.upload();chunk=obj['chunks'][0];w=chunk['replicas'][0]['worker_id'];cid=chunk['chunk_id']
        self.c.compose('stop','-t','1',w)
        eventually(lambda:any(x['worker_id']==w and x['state']=='DEAD' for x in decode(request(self.base+'/api/v1/admin/cluster'))[1]['workers']))
        self.assertEqual(request(self.base+'/api/v1/objects/'+obj['object_id'],'DELETE')[0],204)
        self.c.compose('restart','controller');self.c.compose('start',w);self.c.ready()
        eventually(lambda:not self.c.physical({cid}))
        self.assertEqual(self.get(obj['object_id'])[0],404)

    def test_10_insufficient_workers_reject_upload(self):
        self.c.compose('stop','-t','1','worker2','worker3','worker4')
        eventually(lambda:decode(request(self.base+'/api/v1/admin/cluster'))[1]['alive_workers']==1)
        body,headers=multipart(b'data');status,result,_=decode(request(self.base+'/api/v1/objects','POST',body,headers))
        self.assertEqual(status,503);self.assertIn('message',result['error'])

    def test_11_recovery_of_persisted_interrupted_operations(self):
        for state in ['UPLOADING','DELETING']:
            obj=self.upload();oid=obj['object_id'];cids={c['chunk_id'] for c in obj['chunks']}
            # Deterministically reproduce database state left by an interrupted operation.
            self.c.python('controller',"import sqlite3,sys; c=sqlite3.connect('/data/metadata.db'); c.execute('UPDATE objects SET state=? WHERE object_id=?',(sys.argv[1],sys.argv[2])); c.commit()",state,oid)
            self.c.compose('kill','-s','SIGKILL','controller');self.c.compose('start','controller');self.c.ready()
            eventually(lambda:not self.c.physical(cids));self.assertEqual(self.get(oid)[0],404)

    def test_12_large_object(self):
        data=bytes(range(256))*(32*1024*1024//256)
        obj=self.upload(data);self.assertEqual(obj['chunk_count'],32)
        status,body,_=self.get(obj['object_id']);self.assertEqual(status,200)
        self.assertEqual(hashlib.sha256(body).digest(),hashlib.sha256(data).digest())

    def test_13_worker_internal_api(self):
        w='worker1';base=self.c.urls[w];cid=f'{uuid.uuid4()}-00000';url=base+'/internal/chunks/'+cid
        try:
            data=b'\x00\xffdata';headers={'X-Chunk-Checksum':hashlib.sha256(data).hexdigest(),'Content-Type':'application/octet-stream'}
            self.assertEqual(request(url,'PUT',data,headers)[0],201)
            self.assertEqual(request(url)[1],data)
            self.assertEqual(request(url,'PUT',b'wrong',headers)[0],400)
            self.assertEqual(request(base+'/internal/chunks/bad','PUT',b'x')[0],400)
            self.assertEqual(request(base+'/internal/workers/wrong/chunks')[0],404)
            self.assertEqual(request(url,'DELETE')[0],204);self.assertEqual(request(url,'DELETE')[0],204)
            self.assertEqual(request(url)[0],404)
        finally:request(url,'DELETE')

    def test_14_three_replicas_with_only_one_valid_copy(self):
        obj=self.upload();chunk=obj['chunks'][0];cid=chunk['chunk_id']
        holders={r['worker_id'] for r in chunk['replicas']}
        spare=next(w for w in ['worker1','worker2','worker3','worker4'] if w not in holders)
        self.c.compose('stop','-t','1','controller')
        for w in holders:self.c.corrupt(w,cid)
        headers={'X-Chunk-Checksum':chunk['checksum'],'Content-Type':'application/octet-stream'}
        self.assertEqual(request(self.c.urls[spare]+'/internal/chunks/'+cid,'PUT',b'hello',headers)[0],201)
        code=("import sqlite3,sys; c=sqlite3.connect('/data/metadata.db'); "
              "c.execute(\"INSERT INTO replicas VALUES (?,?,'HEALTHY','test')\",(sys.argv[1],sys.argv[2])); c.commit()")
        self.c.compose('run','--rm','--no-deps','controller','python','-c',code,cid,spare)
        self.c.compose('start','controller');self.c.ready()
        def repaired():
            copies=self.c.physical({cid})
            return len(copies)==2 and all(request(self.c.urls[w]+'/internal/chunks/'+cid)[1]==b'hello' for w,_ in copies)
        eventually(repaired)
        self.assertEqual(self.get(obj['object_id'])[1],b'hello')

    def test_15_orphan_eventually_removed_without_worker_restart(self):
        cid=f'{uuid.uuid4()}-00000';url=self.c.urls['worker1']+'/internal/chunks/'+cid
        try:
            self.assertEqual(request(url,'PUT',b'orphan',{'Content-Type':'application/octet-stream'})[0],201)
            eventually(lambda:request(url)[0]==404)
        finally:request(url,'DELETE')


def main():
    global CLUSTER
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--keep',action='store_true');args=parser.parse_args()
    preflight=subprocess.run(['docker','info'],capture_output=True,text=True)
    if preflight.returncode:
        print('Docker is unavailable; no containers or volumes were changed.\n'+preflight.stderr,file=sys.stderr)
        return 2
    CLUSTER=Cluster();passed=False
    print('Isolated project:',CLUSTER.project,flush=True)
    try:
        CLUSTER.compose('build',capture=False)
        # Run core + actual ASGI API tests inside the pinned dependency image.
        CLUSTER.compose('run','--rm','--no-deps','-v',str(ROOT)+':/project:ro','-w','/project',
                        'controller','python','-m','unittest','discover','-s','tests','-v',capture=False)
        CLUSTER.compose('up','-d','--wait',capture=False);CLUSTER.ready()
        result=unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(DockerTests))
        passed=result.wasSuccessful()
        (CLUSTER.results/'summary.json').write_text(json.dumps({'passed':passed,'tests':result.testsRun,
            'failures':[(str(t),trace) for t,trace in result.failures],
            'errors':[(str(t),trace) for t,trace in result.errors]},indent=2))
    finally:
        try:(CLUSTER.results/'containers.log').write_text(CLUSTER.compose('logs','--no-color'))
        except subprocess.CalledProcessError:pass
        if args.keep:
            # Retain the compose file too, so the user can inspect/clean this exact project.
            destination=CLUSTER.results/'compose.json';destination.write_text(CLUSTER.file.read_text())
            print(f'Kept test cluster. Cleanup: docker compose -p {CLUSTER.project} -f {destination} down -v --rmi local')
        else:CLUSTER.compose('down','-v','--remove-orphans','--rmi','local',capture=False)
        CLUSTER.temp.cleanup()
        print('Results:',CLUSTER.results)
    return 0 if passed else 1


if __name__=='__main__':sys.exit(main())

"""Actual FastAPI/ASGI requests; requires the pinned controller dependencies."""
import asyncio
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
import uuid
from unittest.mock import patch

from support import ROOT, make_context
from app import config

HAS_HTTP = all(importlib.util.find_spec(name) is not None for name in ('fastapi','httpx','python_multipart'))


@unittest.skipUnless(HAS_HTTP, 'HTTP tests require controller/requirements.txt')
class APITests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        import httpx
        from app.main import app
        self.temp = tempfile.TemporaryDirectory()
        self.ctx = make_context(Path(self.temp.name))
        app.state.ctx = self.ctx
        self.client = httpx.AsyncClient(transport=httpx.ASGITransport(app=app, raise_app_exceptions=False),
                                       base_url='http://controller')

    async def asyncTearDown(self):
        await self.client.aclose()
        tasks = list(self.ctx.background)
        for task in tasks: task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self.ctx.repo.close()
        self.temp.cleanup()

    async def upload(self, data=b'hello', name='测试.bin', metadata=None, content_type='application/octet-stream'):
        fields = {} if metadata is None else {'metadata':json.dumps(metadata)}
        response = await self.client.post('/api/v1/objects', files={'file':(name, data, content_type)}, data=fields)
        self.assertEqual(response.status_code,201,response.text)
        return response.json()

    async def test_all_six_endpoints_and_system_metadata(self):
        data = bytes(range(256))*5000
        obj = await self.upload(data, metadata={'owner':'aj','nested':{'a':[1,True,None]}})
        oid = obj['object_id'];uuid.UUID(oid)
        for key in ['name','size','content_type','created_at','modified_at','checksum','chunk_count','chunks','custom_metadata']:
            self.assertIn(key,obj)
        self.assertEqual(obj['size'],len(data));self.assertEqual(obj['chunk_count'],2)
        self.assertEqual(obj['checksum'],hashlib.sha256(data).hexdigest())
        self.assertEqual([c['index'] for c in obj['chunks']],[0,1])
        self.assertTrue(all(len(c['replicas'])==2 for c in obj['chunks']))
        r=await self.client.get(f'/api/v1/objects/{oid}');self.assertEqual(r.status_code,200)
        self.assertEqual(r.content,data);self.assertEqual(r.headers['content-type'],'application/octet-stream')
        self.assertIn("filename*=UTF-8''",r.headers['content-disposition'])
        self.assertEqual(int(r.headers['content-length']),len(data))
        self.assertEqual(r.headers['x-object-checksum'],obj['checksum'])
        r=await self.client.get(f'/api/v1/objects/{oid}/metadata');self.assertEqual(r.status_code,200)
        self.assertEqual(r.json()['custom_metadata']['owner'],'aj')
        r=await self.client.patch(f'/api/v1/objects/{oid}/metadata',json={'owner':None,'new':42})
        self.assertEqual(r.status_code,200);self.assertNotIn('owner',r.json()['custom_metadata'])
        self.assertEqual(r.json()['created_at'],obj['created_at'])
        self.assertGreaterEqual(r.json()['modified_at'],obj['modified_at'])
        self.assertEqual((await self.client.get(f'/api/v1/objects/{oid}')).content,data)
        r=await self.client.get('/api/v1/objects');self.assertEqual(r.status_code,200)
        self.assertEqual(r.json()['objects'][0]['object_id'],oid)
        r=await self.client.delete(f'/api/v1/objects/{oid}');self.assertEqual(r.status_code,204);self.assertEqual(r.content,b'')
        self.assertEqual(self.ctx.workers.copies(),[])
        self.assertEqual((await self.client.delete(f'/api/v1/objects/{oid}')).status_code,404)
        self.assertEqual((await self.client.get(f'/api/v1/objects/{oid}')).status_code,404)

    async def test_missing_file_and_invalid_metadata_are_400(self):
        self.assertEqual((await self.client.post('/api/v1/objects')).status_code,400)
        for meta in ['','oops','[]','null','{"x":NaN}','{"x":1e999}']:
            r=await self.client.post('/api/v1/objects', files={'file':('x',b'x')},data={'metadata':meta})
            self.assertEqual(r.status_code,400,(meta,r.text));self.assertIn('message',r.json()['error'])
        self.assertEqual(self.ctx.repo.count_objects(),0)

    async def test_invalid_patch_and_missing_object(self):
        obj=await self.upload();url=f"/api/v1/objects/{obj['object_id']}/metadata"
        for body in ['[]','null','"text"','{','', '{"x":NaN}', '{"x":1e999}']:
            r=await self.client.patch(url,content=body,headers={'Content-Type':'application/json'})
            self.assertEqual(r.status_code,400,(body,r.text))
        for oid in ['bad-id',str(uuid.uuid4())]:
            for method,suffix in [('get',''),('get','/metadata'),('patch','/metadata'),('delete','')]:
                kw={'json':{}} if method=='patch' else {}
                r=await getattr(self.client,method)(f'/api/v1/objects/{oid}{suffix}',**kw)
                self.assertEqual(r.status_code,404,r.text)

    async def test_pagination_and_validation(self):
        for _ in range(3):await self.upload()
        r=await self.client.get('/api/v1/objects?limit=1&offset=1')
        self.assertEqual(r.status_code,200);self.assertEqual(len(r.json()['objects']),1);self.assertEqual(r.json()['total'],3)
        for query in ['limit=0','limit=-1','limit=1001','offset=-1','limit=abc','offset=abc']:
            r=await self.client.get('/api/v1/objects?'+query);self.assertEqual(r.status_code,400,(query,r.text))

    async def test_empty_object(self):
        obj=await self.upload(b'')
        self.assertEqual(obj['chunk_count'],0)
        r=await self.client.get(f"/api/v1/objects/{obj['object_id']}")
        self.assertEqual(r.status_code,200);self.assertEqual(r.content,b'')

    async def test_content_types_preserved(self):
        for ct in ['application/octet-stream','application/pdf','image/png','audio/mpeg','video/mp4','application/zip']:
            obj=await self.upload(content_type=ct)
            r=await self.client.get(f"/api/v1/objects/{obj['object_id']}")
            self.assertEqual(r.headers['content-type'],ct)

    async def test_unavailable_later_chunk_is_clean_json_503(self):
        obj=await self.upload(b'x'*(1048576+1));chunk=obj['chunks'][1]
        for rep in chunk['replicas']:self.ctx.workers.corrupt(rep['worker_id'],chunk['chunk_id'])
        r=await self.client.get(f"/api/v1/objects/{obj['object_id']}")
        self.assertEqual(r.status_code,503);self.assertIn('message',r.json()['error'])

    async def test_insufficient_storage(self):
        for w in ['w2','w3','w4']:self.ctx.membership.get(w).state='DEAD'
        r=await self.client.post('/api/v1/objects',files={'file':('x',b'x')})
        self.assertEqual(r.status_code,503)

    async def test_concurrent_patch_and_delete(self):
        obj=await self.upload();oid=obj['object_id'];url=f'/api/v1/objects/{oid}/metadata'
        responses=await asyncio.gather(*(self.client.patch(url,json={str(i):i}) for i in range(25)))
        self.assertTrue(all(r.status_code==200 for r in responses))
        self.assertEqual(len((await self.client.get(url)).json()['custom_metadata']),25)
        responses=await asyncio.gather(*(self.client.delete(f'/api/v1/objects/{oid}') for _ in range(5)))
        self.assertEqual(sorted(r.status_code for r in responses),[204,404,404,404,404])
        self.assertEqual(self.ctx.workers.copies(),[])

    async def test_metadata_limits_apply_after_merge(self):
        obj=await self.upload(metadata={'a':1})
        with patch.object(config,'MAX_METADATA_KEYS',1):
            r=await self.client.patch(f"/api/v1/objects/{obj['object_id']}/metadata",json={'b':2})
        self.assertEqual(r.status_code,400)
        self.assertEqual(self.ctx.repo.get_object(obj['object_id'])['custom_metadata'],{'a':1})

    async def test_internal_registration_heartbeat_and_errors(self):
        r=await self.client.post('/internal/workers/register',json={'worker_id':'w1','address':'http://w1'})
        self.assertEqual(r.status_code,200)
        r=await self.client.post('/internal/workers/heartbeat',json={'worker_id':'w1'})
        self.assertEqual(r.status_code,200)
        r=await self.client.post('/internal/workers/heartbeat',json={'worker_id':'missing'})
        self.assertEqual(r.status_code,404)
        for body in [{},{'worker_id':'../bad','address':'http://w1'},{'worker_id':'w1','address':'invalid'}]:
            r=await self.client.post('/internal/workers/register',json=body);self.assertEqual(r.status_code,400)
        r=await self.client.get('/not-found');self.assertEqual(r.status_code,404);self.assertIn('message',r.json()['error'])

    async def test_conflict_and_unexpected_error_json(self):
        obj=await self.upload();oid=obj['object_id'];self.ctx.repo.begin_delete(oid)
        r=await self.client.patch(f'/api/v1/objects/{oid}/metadata',json={});self.assertEqual(r.status_code,409)
        r=await self.client.delete(f'/api/v1/objects/{oid}');self.assertEqual(r.status_code,409)
        with patch.object(self.ctx.repo,'list_objects',side_effect=RuntimeError('test failure')):
            r=await self.client.get('/api/v1/objects')
        self.assertEqual(r.status_code,500);self.assertIn('message',r.json()['error'])


@unittest.skipUnless(HAS_HTTP, 'HTTP tests require controller/requirements.txt')
class WorkerAPITests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        import httpx
        import sys
        spec=importlib.util.spec_from_file_location('worker_test_app',ROOT/'worker/app/__init__.py',
                                                  submodule_search_locations=[str(ROOT/'worker/app')])
        package=importlib.util.module_from_spec(spec);sys.modules['worker_test_app']=package;spec.loader.exec_module(package)
        from worker_test_app.main import app
        from worker_test_app.storage import ChunkStore
        self.temp=tempfile.TemporaryDirectory();app.state.store=ChunkStore(self.temp.name,1048576)
        self.client=httpx.AsyncClient(transport=httpx.ASGITransport(app=app,raise_app_exceptions=False),base_url='http://worker1')
        self.store=app.state.store

    async def asyncTearDown(self):
        await self.client.aclose();self.temp.cleanup()

    async def test_chunk_put_get_inventory_delete_and_corruption(self):
        cid=f'{uuid.uuid4()}-00000';url='/internal/chunks/'+cid;data=b'\x00\xffbinary'
        checksum=hashlib.sha256(data).hexdigest()
        r=await self.client.put(url,content=data,headers={'X-Chunk-Checksum':checksum});self.assertEqual(r.status_code,201)
        r=await self.client.put(url,content=data,headers={'X-Chunk-Checksum':checksum});self.assertEqual(r.status_code,200)
        self.assertEqual((await self.client.get(url)).content,data)
        r=await self.client.get('/internal/workers/worker1/chunks');self.assertEqual(r.status_code,200);self.assertEqual(r.json()['chunk_count'],1)
        Path(self.store._paths(cid)[0]).write_bytes(b'bad')
        r=await self.client.get(url);self.assertEqual(r.status_code,500);self.assertEqual(r.json()['error']['code'],'CHUNK_CORRUPT')
        for _ in range(2):self.assertEqual((await self.client.delete(url)).status_code,204)
        self.assertEqual((await self.client.get(url)).status_code,404)

    async def test_worker_invalid_requests(self):
        url='/internal/chunks/'+str(uuid.uuid4())+'-00000'
        r=await self.client.put(url,content=b'x',headers={'X-Chunk-Checksum':'bad'});self.assertEqual(r.status_code,400)
        r=await self.client.put(url,content=b'x'*1048577);self.assertEqual(r.status_code,400)
        r=await self.client.put('/internal/chunks/bad',content=b'x');self.assertEqual(r.status_code,400)
        r=await self.client.get('/internal/workers/not-this-worker/chunks');self.assertEqual(r.status_code,404)

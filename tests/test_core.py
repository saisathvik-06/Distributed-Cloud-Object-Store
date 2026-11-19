import asyncio
import hashlib
import json
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch
import uuid

from support import File, make_context, consume, storage
from app import config
from app.deletion import purge_object, retry_deletions
from app.download import open_download
from app.errors import ApiError
from app.metadata import parse_json, validate_custom_metadata
from app.reconcile import reconcile_worker
from app.repository import Repository
from app.db import connect
from app.upload import store_object
from app.worker_errors import WorkerError


class CoreTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name)
        self.ctx = make_context(self.path)

    async def asyncTearDown(self):
        tasks = list(self.ctx.background)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self.ctx.repo.close()
        self.temp.cleanup()

    async def upload(self, data=b'hello', metadata=None):
        stored = await store_object(self.ctx, File(data), '测试.bin', 'application/octet-stream', metadata or {})
        oid = stored.object_id
        return oid, self.ctx.repo.get_chunks(oid)

    async def download(self, oid):
        return await consume(await open_download(self.ctx, self.ctx.repo.get_object(oid)))

    async def test_chunk_boundaries(self):
        for size in [0, 1, 1048575, 1048576, 1048577, 3*1048576+13]:
            with self.subTest(size=size):
                data = (bytes(range(256)) * (size//256+1))[:size]
                oid, chunks = await self.upload(data)
                self.assertEqual(await self.download(oid), data)
                self.assertEqual(len(chunks), (size+1048575)//1048576)
                self.assertEqual(self.ctx.repo.get_object(oid)['checksum'], hashlib.sha256(data).hexdigest())
                for i, c in enumerate(chunks):
                    self.assertEqual(c['chunk_index'], i)
                    self.assertEqual(c['size'], min(1048576, size-i*1048576))
                    self.assertEqual(len({r['worker_id'] for r in c['replicas']}), 2)
                    self.assertEqual(len(self.ctx.workers.copies(c['chunk_id'])), 2)

    async def test_short_reads_are_combined(self):
        data = b'x' * (1048576+5)
        oid = (await store_object(self.ctx, File(data, True), 'x', 'x/test', {})).object_id
        self.assertEqual([c['size'] for c in self.ctx.repo.get_chunks(oid)], [1048576, 5])
        self.assertEqual(await self.download(oid), data)

    async def test_unique_ids_same_name_and_data(self):
        first, _ = await self.upload()
        second, _ = await self.upload()
        self.assertNotEqual(first, second)

    async def test_upload_creation_snapshot_survives_immediate_delete(self):
        stored = await store_object(self.ctx, File(b'hello'), 'x', 'x/test', {})
        await purge_object(self.ctx, stored.object_id)
        self.assertIsNone(self.ctx.repo.get_object(stored.object_id))
        self.assertEqual(stored.metadata['size'], 5)
        self.assertEqual(len(stored.chunks), 1)

    async def test_distribution(self):
        await self.upload(b'x' * 8 * 1048576)
        counts = self.ctx.repo.replica_counts_by_worker()
        self.assertEqual(len(counts), 4)
        self.assertLessEqual(max(counts.values()) - min(counts.values()), 1)

    async def test_failed_upload_rolls_back_confirmed_and_unrecorded_copies(self):
        self.ctx.workers.fail_put = {'w2','w3','w4'}
        with self.assertRaises(ApiError) as e:
            await self.upload()
        self.assertEqual(e.exception.status, 503)
        self.assertEqual(self.ctx.workers.copies(), [])
        self.assertEqual(self.ctx.repo.object_ids_in_state('UPLOADING'), [])
        self.assertEqual(self.ctx.repo.pending_deletions(), [])

    async def test_put_ack_lost_still_cleaned_up(self):
        async def lose_ack(w, cid):
            raise WorkerError('PUT stored, acknowledgement lost')
        self.ctx.workers.after_put = lose_ack
        with self.assertRaises(ApiError):
            await self.upload()
        self.assertEqual(self.ctx.workers.copies(), [])

    async def test_upload_falls_back_to_other_workers(self):
        self.ctx.workers.fail_put = {'w1','w2'}
        oid, chunks = await self.upload()
        self.assertEqual({r['worker_id'] for r in chunks[0]['replicas']}, {'w3','w4'})
        self.assertEqual(await self.download(oid), b'hello')

    async def test_insufficient_workers_503(self):
        for w in ['w2','w3','w4']:
            self.ctx.membership.get(w).state = 'DEAD'
        with self.assertRaises(ApiError) as e:
            await self.upload()
        self.assertEqual(e.exception.status, 503)
        self.assertEqual(self.ctx.repo.count_objects(), 0)

    async def test_upload_size_limit_rolls_back(self):
        with patch.object(config, 'MAX_OBJECT_SIZE', 3):
            with self.assertRaises(ApiError) as e:
                await self.upload(b'hello')
        self.assertEqual(e.exception.status, 400)
        self.assertEqual(self.ctx.workers.copies(), [])

    async def test_corrupt_read_falls_back(self):
        oid, chunks = await self.upload()
        self.ctx.workers.corrupt(chunks[0]['replicas'][0]['worker_id'], chunks[0]['chunk_id'])
        self.assertEqual(await self.download(oid), b'hello')

    async def test_scrub_detects_dormant_corruption(self):
        oid, chunks = await self.upload()
        cid = chunks[0]['chunk_id']
        self.ctx.workers.corrupt(chunks[0]['replicas'][0]['worker_id'], cid)
        await self.ctx.repair.run_once(scrub=True)
        copies = self.ctx.workers.copies(cid)
        self.assertEqual(len(copies), 2)
        for w, c in copies:
            self.assertEqual(self.ctx.workers.stores[w].get(c)[0], b'hello')

    async def test_prune_never_deletes_only_valid_copy(self):
        oid, chunks = await self.upload()
        cid = chunks[0]['chunk_id']
        for r in chunks[0]['replicas']:
            self.ctx.workers.corrupt(r['worker_id'], cid)
        extra = next(w for w in self.ctx.workers.stores if w not in {r['worker_id'] for r in chunks[0]['replicas']})
        self.ctx.workers.stores[extra].put(cid, b'hello', hashlib.sha256(b'hello').hexdigest())
        self.ctx.repo.add_replica(cid, extra)
        await self.ctx.repair.run_once(scrub=True)
        self.assertEqual(await self.download(oid), b'hello')
        self.assertEqual(len(self.ctx.workers.copies(cid)), 2)
        for w, c in self.ctx.workers.copies(cid):
            self.assertEqual(self.ctx.workers.stores[w].get(c)[0], b'hello')

    async def test_all_corrupt_retained_and_503(self):
        oid, chunks = await self.upload()
        for r in chunks[0]['replicas']:
            self.ctx.workers.corrupt(r['worker_id'], chunks[0]['chunk_id'])
        await self.ctx.repair.run_once(scrub=True)
        with self.assertRaises(ApiError) as e:
            await self.download(oid)
        self.assertEqual(e.exception.status, 503)
        self.assertEqual(len(self.ctx.workers.copies()), 2)

    async def test_later_chunk_unavailable_fails_before_stream(self):
        oid, chunks = await self.upload(b'x' * (1048576+1))
        for r in chunks[1]['replicas']:
            self.ctx.workers.corrupt(r['worker_id'], chunks[1]['chunk_id'])
        with self.assertRaises(ApiError) as e:
            await open_download(self.ctx, self.ctx.repo.get_object(oid))
        self.assertEqual(e.exception.status, 503)

    async def test_whole_object_checksum_verified(self):
        oid, _ = await self.upload()
        self.ctx.repo._conn.execute("UPDATE objects SET checksum='bad' WHERE object_id=?", (oid,))
        with self.assertRaises(ApiError):
            await self.download(oid)

    async def test_missing_chunk_row_detected(self):
        oid, chunks = await self.upload(b'x'*(1048576+1))
        self.ctx.repo._conn.execute('DELETE FROM chunks WHERE chunk_id=?', (chunks[1]['chunk_id'],))
        with self.assertRaises(ApiError):
            await self.download(oid)

    async def test_worker_failure_repair_and_return(self):
        oid, chunks = await self.upload()
        wid = chunks[0]['replicas'][0]['worker_id']
        self.ctx.workers.offline.add(wid)
        info = self.ctx.membership.get(wid)
        info.last_heartbeat = time.monotonic()-100
        self.ctx.membership.detect_failures()
        info.died_at = time.monotonic()-100
        self.assertEqual(await self.download(oid), b'hello')
        await self.ctx.repair.run_once(scrub=True)
        self.assertEqual(len([w for w,c in self.ctx.workers.copies() if w!=wid]), 2)
        self.ctx.workers.offline.clear()
        self.ctx.membership.register(wid, wid, {})
        await reconcile_worker(self.ctx, wid)
        await self.ctx.repair.run_once(scrub=True)
        self.assertEqual(len(self.ctx.workers.copies()), 2)
        self.assertEqual(await self.download(oid), b'hello')

    async def test_no_repair_during_worker_grace(self):
        _, chunks = await self.upload()
        wid = chunks[0]['replicas'][0]['worker_id']
        self.ctx.membership.get(wid).last_heartbeat = time.monotonic()-100
        self.ctx.membership.detect_failures()
        await self.ctx.repair.run_once(scrub=True)
        self.assertEqual(len(self.ctx.workers.copies()), 2)

    async def test_missing_file_recreated(self):
        oid, chunks = await self.upload()
        c = chunks[0]
        self.ctx.workers.stores[c['replicas'][0]['worker_id']].delete(c['chunk_id'])
        await self.ctx.repair.run_once(scrub=True)
        self.assertEqual(len(self.ctx.workers.copies()), 2)
        self.assertEqual(await self.download(oid), b'hello')

    async def test_delete_physically_removes_all_copies(self):
        oid, _ = await self.upload()
        self.ctx.repo.begin_delete(oid)
        await purge_object(self.ctx, oid)
        self.assertIsNone(self.ctx.repo.get_object(oid))
        self.assertEqual(self.ctx.workers.copies(), [])
        self.assertEqual(self.ctx.repo.pending_deletions(), [])

    async def test_failed_delete_retries_without_restart(self):
        oid, chunks = await self.upload()
        wid = chunks[0]['replicas'][0]['worker_id']
        self.ctx.workers.fail_delete.add(wid)
        await purge_object(self.ctx, oid)
        self.assertTrue(self.ctx.repo.pending_deletions())
        self.ctx.workers.fail_delete.clear()
        await retry_deletions(self.ctx)
        self.assertEqual(self.ctx.workers.copies(), [])
        self.assertEqual(self.ctx.repo.pending_deletions(), [])

    async def test_delete_queue_survives_controller_restart(self):
        oid, chunks = await self.upload()
        wid = chunks[0]['replicas'][0]['worker_id']
        self.ctx.workers.offline.add(wid)
        await purge_object(self.ctx, oid)
        self.ctx.repo.close()
        self.ctx.repo = Repository(connect(str(self.path/'metadata.db')))
        self.assertTrue(self.ctx.repo.pending_deletions())
        self.ctx.workers.offline.clear()
        await retry_deletions(self.ctx)
        self.assertEqual(self.ctx.workers.copies(), [])

    async def test_delete_waits_for_inflight_repair(self):
        oid, chunks = await self.upload()
        r = chunks[0]['replicas'][0]
        self.ctx.workers.stores[r['worker_id']].delete(chunks[0]['chunk_id'])
        entered, release = asyncio.Event(), asyncio.Event()
        async def pause(w, c):
            entered.set()
            await release.wait()
        self.ctx.workers.after_put = pause
        repair = asyncio.create_task(self.ctx.repair.run_once(scrub=True))
        await asyncio.wait_for(entered.wait(), 2)
        deletion = asyncio.create_task(purge_object(self.ctx, oid))
        await asyncio.sleep(.02)
        self.assertFalse(deletion.done())
        release.set()
        await asyncio.wait_for(asyncio.gather(repair, deletion), 3)
        self.assertEqual(self.ctx.workers.copies(), [])
        self.assertIsNone(self.ctx.repo.get_object(oid))

    async def test_repair_cannot_register_deleting_object(self):
        oid, chunks = await self.upload()
        self.ctx.repo.begin_delete(oid)
        self.assertFalse(self.ctx.repo.add_replica(chunks[0]['chunk_id'], 'w4'))
        await self.ctx.repair.run_once(scrub=True)
        self.assertEqual(len(self.ctx.workers.copies()), 2)

    async def test_download_snapshot_survives_delete(self):
        oid, _ = await self.upload(b'x'*(1048576+1))
        prepared = await open_download(self.ctx, self.ctx.repo.get_object(oid))
        await purge_object(self.ctx, oid)
        self.assertEqual(await consume(prepared), b'x'*(1048576+1))
        self.assertTrue(prepared.spool.closed)

    async def test_orphans_age_out_without_reregistration(self):
        cid = f'{uuid.uuid4()}-00000'
        self.ctx.workers.stores['w1'].put(cid, b'x', None)
        await reconcile_worker(self.ctx, 'w1')
        self.assertEqual(len(self.ctx.workers.copies()), 1)
        with patch.object(config, 'ORPHAN_MIN_AGE_S', 0):
            await reconcile_worker(self.ctx, 'w1')
        self.assertEqual(self.ctx.workers.copies(), [])

    async def test_reconciliation_does_not_trust_sidecar(self):
        _, chunks = await self.upload()
        cid = chunks[0]['chunk_id']
        wid = chunks[0]['replicas'][0]['worker_id']
        self.ctx.workers.corrupt(wid, cid)
        await reconcile_worker(self.ctx, wid)
        self.assertEqual(self.ctx.repo.replicas_for_worker(wid)[cid], 'CORRUPT')

    async def test_reconcile_waits_for_upload_and_preserves_copy(self):
        entered, release = asyncio.Event(), asyncio.Event()
        written = []
        async def pause(w, cid):
            written.append((w, cid)); entered.set(); await release.wait()
        self.ctx.workers.after_put = pause
        upload = asyncio.create_task(self.upload())
        await asyncio.wait_for(entered.wait(), 2)
        rec = asyncio.create_task(reconcile_worker(self.ctx, written[0][0]))
        await asyncio.sleep(.02)
        release.set()
        oid, _ = await upload
        await rec
        self.assertEqual(await self.download(oid), b'hello')
        self.assertEqual(len(self.ctx.workers.copies()), 2)

    async def test_concurrent_metadata_merge_and_null_delete(self):
        oid, _ = await self.upload(metadata={'old':1})
        await asyncio.gather(*(asyncio.to_thread(self.ctx.repo.patch_metadata, oid, {str(i):i}) for i in range(40)))
        self.ctx.repo.patch_metadata(oid, {'old':None})
        self.assertEqual(len(self.ctx.repo.get_object(oid)['custom_metadata']), 40)

    async def test_merged_metadata_limit_is_transactional(self):
        oid, _ = await self.upload(metadata={'a':1})
        with patch.object(config, 'MAX_METADATA_KEYS', 1):
            with self.assertRaises(ApiError):
                self.ctx.repo.patch_metadata(oid, {'b':2})
        self.assertEqual(self.ctx.repo.get_object(oid)['custom_metadata'], {'a':1})

    async def test_restart_interrupted_upload_cleanup(self):
        oid = str(uuid.uuid4());cid = oid+'-00000'
        self.ctx.repo.create_object(oid, 'x', 'x/test', {})
        self.ctx.repo.add_chunk(cid, oid, 0, 1, hashlib.sha256(b'x').hexdigest())
        self.ctx.workers.stores['w1'].put(cid, b'x', None)
        self.ctx.repo.close();self.ctx.repo=Repository(connect(str(self.path/'metadata.db')))
        self.assertIn(oid, self.ctx.repo.object_ids_in_state('UPLOADING'))
        await purge_object(self.ctx, oid)
        self.assertEqual(self.ctx.workers.copies(), [])
        self.assertEqual(self.ctx.repo.object_ids_in_state('UPLOADING'), [])

    async def test_concurrent_uploads_are_independent(self):
        results = await asyncio.gather(*(self.upload(bytes([i])*1200) for i in range(12)))
        self.assertEqual(len({o for o,c in results}), 12)
        for i, (oid, chunks) in enumerate(results):
            self.assertEqual(await self.download(oid), bytes([i])*1200)
            self.assertEqual(len(chunks[0]['replicas']), 2)

    async def test_foreign_keys_reject_dangling_replica(self):
        self.assertFalse(self.ctx.repo.add_replica('missing', 'w1'))
        self.assertEqual(self.ctx.repo.replicas_for_worker('w1'), {})

    async def test_cancelled_upload_is_cleaned(self):
        entered = asyncio.Event()
        async def pause(w, cid):
            entered.set()
            await asyncio.Event().wait()
        self.ctx.workers.after_put = pause
        upload = asyncio.create_task(self.upload())
        await asyncio.wait_for(entered.wait(), 2)
        upload.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await upload
        self.assertEqual(self.ctx.workers.copies(), [])
        self.assertEqual(self.ctx.repo.object_ids_in_state('UPLOADING'), [])

    async def test_reconciliation_adopts_unrecorded_valid_copy(self):
        oid, chunks = await self.upload()
        cid = chunks[0]['chunk_id']
        wid = chunks[0]['replicas'][0]['worker_id']
        self.ctx.repo.remove_replica(cid, wid)
        await reconcile_worker(self.ctx, wid)
        self.assertEqual(self.ctx.repo.replicas_for_worker(wid)[cid], 'HEALTHY')
        self.assertEqual(await self.download(oid), b'hello')

    async def test_cancelled_db_write_finishes_before_lock_released(self):
        import threading
        from app.repository import run_db
        started, release = threading.Event(), threading.Event()
        finished = []
        def delayed_write():
            started.set()
            release.wait(2)
            finished.append(True)
        async def operation():
            async with self.ctx.object_lock('test'):
                await run_db(delayed_write)
        task = asyncio.create_task(operation())
        await asyncio.to_thread(started.wait, 2)
        task.cancel()
        await asyncio.sleep(.02)
        self.assertTrue(self.ctx.object_lock('test').locked())
        release.set()
        with self.assertRaises(asyncio.CancelledError): await task
        self.assertEqual(finished, [True])

    async def test_idle_lifecycle_locks_are_reclaimed(self):
        import gc
        for i in range(100):
            async with self.ctx.object_lock(str(i)): pass
        gc.collect()
        self.assertEqual(len(self.ctx._object_locks), 0)

    async def test_prepared_download_closes_when_consumer_stops(self):
        oid, _ = await self.upload(b'x'*(1048576+1))
        prepared = await open_download(self.ctx, self.ctx.repo.get_object(oid))
        iterator = prepared.body()
        await anext(iterator)
        await iterator.aclose()
        self.assertTrue(prepared.spool.closed)

    async def test_repair_tries_next_target_after_put_failure(self):
        oid, chunks = await self.upload()
        bad = chunks[0]['replicas'][0]['worker_id']
        self.ctx.workers.stores[bad].delete(chunks[0]['chunk_id'])
        self.ctx.workers.fail_put.add(bad)
        await self.ctx.repair.run_once(scrub=True)
        self.assertEqual(await self.download(oid), b'hello')
        self.assertEqual(len(self.ctx.workers.copies()), 2)

    async def wait_until(self, condition):
        async def poll():
            while not condition(): await asyncio.sleep(.01)
        await asyncio.wait_for(poll(), 3)

    async def test_automatic_scrubber_without_download_or_manual_pass(self):
        _, chunks = await self.upload()
        cid = chunks[0]['chunk_id']
        self.ctx.workers.corrupt(chunks[0]['replicas'][0]['worker_id'], cid)
        def correct():
            copies = self.ctx.workers.copies(cid)
            if len(copies) != 2: return False
            try: return all(self.ctx.workers.stores[w].get(c)[0] == b'hello' for w,c in copies)
            except storage.ChunkCorrupt: return False
        with patch.object(config, 'REPAIR_INTERVAL_S', .01), patch.object(config, 'STARTUP_GRACE_S', 0):
            task = asyncio.create_task(self.ctx.repair.run())
            try: await self.wait_until(correct)
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    async def test_automatic_orphan_retry_after_age_threshold(self):
        from app.reconcile import run_reconciliation
        cid=f'{uuid.uuid4()}-00000'
        self.ctx.workers.stores['w1'].put(cid,b'x',None)
        with patch.object(config,'RECONCILE_INTERVAL_S',.01), patch.object(config,'ORPHAN_MIN_AGE_S',.05):
            task=asyncio.create_task(run_reconciliation(self.ctx))
            try: await self.wait_until(lambda:not self.ctx.workers.copies())
            finally:
                task.cancel()
                await asyncio.gather(task,return_exceptions=True)

    async def test_automatic_cleanup_retries_failed_delete(self):
        from app.deletion import run_cleanup
        oid,chunks=await self.upload()
        wid=chunks[0]['replicas'][0]['worker_id']
        self.ctx.workers.fail_delete.add(wid)
        await purge_object(self.ctx,oid)
        with patch.object(config,'DELETE_RETRY_INTERVAL_S',.01):
            task=asyncio.create_task(run_cleanup(self.ctx))
            try:
                self.ctx.workers.fail_delete.clear()
                await self.wait_until(lambda:not self.ctx.repo.pending_deletions())
                self.assertEqual(self.ctx.workers.copies(),[])
            finally:
                task.cancel()
                await asyncio.gather(task,return_exceptions=True)

    async def test_listing_hides_uncommitted_and_deleted(self):
        oid, _ = await self.upload()
        self.ctx.repo.create_object(str(uuid.uuid4()), 'pending', 'x/test', {})
        rows,total=self.ctx.repo.list_objects(100,0)
        self.assertEqual(total,1);self.assertEqual(rows[0]['object_id'],oid)
        self.ctx.repo.begin_delete(oid)
        self.assertEqual(self.ctx.repo.list_objects(100,0)[1],0)


class ValidationAndStorageTests(unittest.TestCase):
    def test_strict_json_and_nested_metadata(self):
        self.assertEqual(validate_custom_metadata(parse_json('{"a":{"nested":[1,null,true]}}')),
                         {'a':{'nested':[1,None,True]}})
        for text in ['{"x":NaN}', '{"x":Infinity}', '{"x":-Infinity}', '']:
            with self.subTest(text=text), self.assertRaises(ValueError):parse_json(text)
        for value in [[], None, 'x', {'a':float('inf')}, {'a':'\ud800'}]:
            with self.subTest(value=value), self.assertRaises(ApiError):validate_custom_metadata(value)

    def test_worker_size_checksum_and_id_validation(self):
        with tempfile.TemporaryDirectory() as tmp:
            st=storage.ChunkStore(tmp,3);cid=f'{uuid.uuid4()}-00000'
            for bad in ['../escape','/tmp/escape','x','..',cid+'\n']:
                with self.subTest(bad=bad), self.assertRaises(storage.InvalidChunkId):st.put(bad,b'x',None)
            with self.assertRaises(storage.ChunkTooLarge):st.put(cid,b'four',None)
            with self.assertRaises(storage.ChecksumMismatch):st.put(cid,b'abc','bad')
            st.put(cid,b'abc',hashlib.sha256(b'abc').hexdigest())
            self.assertEqual(st.get(cid)[0],b'abc')
            self.assertTrue(st.delete(cid));self.assertFalse(st.delete(cid))

    def test_worker_persistence(self):
        with tempfile.TemporaryDirectory() as tmp:
            cid=f'{uuid.uuid4()}-00000';st=storage.ChunkStore(tmp,1024)
            st.put(cid,b'abc',None)
            self.assertEqual(storage.ChunkStore(tmp,1024).get(cid)[0],b'abc')

    def test_worker_large_chunk_index(self):
        with tempfile.TemporaryDirectory() as tmp:
            cid=f'{uuid.uuid4()}-100000';st=storage.ChunkStore(tmp,1024)
            st.put(cid,b'x',None);self.assertEqual(st.get(cid)[0],b'x')

    def test_worker_concurrent_writes_keep_data_and_sidecar_consistent(self):
        import concurrent.futures
        with tempfile.TemporaryDirectory() as tmp:
            cid=f'{uuid.uuid4()}-00000';st=storage.ChunkStore(tmp,1024)
            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
                list(pool.map(lambda i:st.put(cid,bytes([i])*100,None),range(100)))
            data, checksum=st.get(cid)
            self.assertEqual(checksum,hashlib.sha256(data).hexdigest())


if __name__ == '__main__':
    unittest.main()

import asyncio
import tempfile
import unittest
from contextvars import ContextVar
from pathlib import Path
from unittest.mock import patch

from cloudreve import AsyncCloudreveV4


async def until(predicate):
    async def wait():
        while not predicate():
            await asyncio.sleep(0.001)
    await asyncio.wait_for(wait(), 2)


def token():
    return {'access_token': 'new', 'refresh_token': 'rotated',
            'access_expires': '2099-01-01T00:00:00Z'}


class QueueTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.client = AsyncCloudreveV4(trust_env=False, max_concurrency=2)
        self.addAsyncCleanup(self.client.aclose)
        self.tasks = []
        self.addAsyncCleanup(self.finish_tasks)

    def start(self, awaitable):
        task = asyncio.create_task(awaitable)
        self.tasks.append(task)
        return task

    async def finish_tasks(self):
        for task in self.tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)

    async def test_fifo_bound_backpressure_and_caller_context(self):
        gate = asyncio.Event()
        trace = ContextVar('trace', default=None)
        started, contexts = [], []
        active = peak = 0
        async def request(method, url, **kwargs):
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            index = kwargs['params']['uri'].rsplit('/', 1)[-1]
            started.append(index)
            contexts.append(trace.get())
            try:
                await gate.wait()
                return index
            finally:
                active -= 1
        self.client._request = request
        for index in range(6):
            trace.set(str(index))
            self.start(self.client.get_info('/'+str(index)))
        await until(lambda: len(self.client._operations._putters) == 2)
        self.assertEqual(started, ['0', '1'])
        self.assertEqual(self.client._operations._queue.qsize(), 2)
        gate.set()
        results = await asyncio.wait_for(asyncio.gather(*self.tasks), 2)
        self.assertEqual(results, [str(i) for i in range(6)])
        self.assertEqual(started, results)
        self.assertEqual(contexts, results)
        self.assertEqual(peak, 2)

    async def test_single_worker_upload_is_one_job_and_refreshes_mid_operation(self):
        async with AsyncCloudreveV4(trust_env=False, max_concurrency=1) as client:
            client._set_token(token())
            calls = []
            async def request(method, url, **kwargs):
                calls.append(url)
                if url == '/file':
                    client._access_expires = 0
                    return {'storage_policy': {'id': 'p', 'type': 'local'}}
                if url.endswith('/refresh'):
                    self.assertEqual(client._active_requests, 0)
                    return token()
                if method == 'put':
                    return {'session_id': 's', 'chunk_size': 0}
                if method == 'post':
                    body = b''.join([chunk async for chunk in kwargs['content']])
                    self.assertEqual(body, b'abc')
                    return None
                return {'size': 3}
            client._request = request
            with tempfile.TemporaryDirectory() as directory:
                source = Path(directory)/'source'
                source.write_bytes(b'abc')
                with patch.object(client._operations._queue, 'put_nowait', wraps=client._operations._queue.put_nowait) as submit:
                    await asyncio.wait_for(client.upload(source, '/file'), 2)
                    self.assertEqual(submit.call_count, 1)
            self.assertEqual(calls, ['/file', '/session/token/refresh', '/file/upload', '/file/upload/s/0', '/file/info'])

    async def test_refresh_barrier_drains_inflight_and_blocks_new_requests(self):
        old_gate, refresh_gate = asyncio.Event(), asyncio.Event()
        old_started, refresh_started = asyncio.Event(), asyncio.Event()
        self.client._set_token(token())
        calls = []
        async def request(method, url, **kwargs):
            calls.append(url)
            if url == '/old':
                old_started.set()
                await old_gate.wait()
                calls.append('old-finished')
            elif url.endswith('/refresh'):
                self.assertEqual(self.client._active_requests, 0)
                refresh_started.set()
                await refresh_gate.wait()
                return token()
            return url
        self.client._request = request
        self.start(self.client.request('get', '/old'))
        await old_started.wait()
        self.client._access_expires = 0
        self.start(self.client.request('get', '/next'))
        await until(lambda: self.client._auth_pending)
        self.start(self.client.request('get', '/last'))
        self.assertFalse(refresh_started.is_set())
        old_gate.set()
        await asyncio.wait_for(refresh_started.wait(), 2)
        self.assertEqual(calls, ['/old', 'old-finished', '/session/token/refresh'])
        refresh_gate.set()
        await asyncio.wait_for(asyncio.gather(*self.tasks), 2)
        self.assertEqual(calls[3:], ['/next', '/last'])
        self.assertEqual(self.client._active_requests, 0)

    async def test_cancel_while_barrier_drains_does_not_poison_token(self):
        old_gate, old_started = asyncio.Event(), asyncio.Event()
        self.client._set_token(token())
        calls = []
        async def request(method, url, **kwargs):
            calls.append(url)
            if url == '/old':
                old_started.set()
                await old_gate.wait()
            if url.endswith('/refresh'):
                return token()
            return url
        self.client._request = request
        old = self.start(self.client.request('get', '/old'))
        await old_started.wait()
        self.client._access_expires = 0
        waiting = self.start(self.client.request('get', '/cancelled'))
        await until(lambda: self.client._auth_pending)
        waiting.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await asyncio.wait_for(waiting, 2)
        self.assertFalse(self.client._auth_pending)
        self.assertIsNone(self.client._refresh_error)
        old_gate.set()
        await old
        await asyncio.wait_for(self.client.request('get', '/after'), 2)
        self.assertEqual(calls, ['/old', '/session/token/refresh', '/after'])

    async def test_cancel_queued_and_backpressured_operations_sends_nothing(self):
        async with AsyncCloudreveV4(trust_env=False, max_concurrency=1) as client:
            gate, started = asyncio.Event(), asyncio.Event()
            calls = []
            async def request(method, url, **kwargs):
                calls.append(url)
                started.set()
                await gate.wait()
            client._request = request
            first = self.start(client.request('get', '/first'))
            await started.wait()
            queued = self.start(client.request('get', '/queued'))
            backpressured = self.start(client.request('get', '/backpressured'))
            await until(lambda: len(client._operations._putters) == 1)
            queued.cancel()
            backpressured.cancel()
            results = await asyncio.wait_for(asyncio.gather(queued, backpressured, return_exceptions=True), 2)
            self.assertTrue(all(isinstance(result, asyncio.CancelledError) for result in results))
            gate.set()
            await first
            await asyncio.wait_for(client._operations._queue.join(), 2)
            self.assertEqual(calls, ['/first'])

    async def test_close_cancels_backlog_and_waits_for_upload_cleanup(self):
        async with AsyncCloudreveV4(trust_env=False, max_concurrency=1) as client:
            uploading, cleaning, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
            calls = []
            async def request(method, url, **kwargs):
                calls.append((method, url))
                if method == 'delete':
                    cleaning.set()
                    await release.wait()
                elif url == '/file':
                    return {'storage_policy': {'id': 'p', 'type': 'local'}}
                elif method == 'put':
                    return {'session_id': 's', 'chunk_size': 0}
                else:
                    uploading.set()
                    await asyncio.Event().wait()
            client._request = request
            with tempfile.TemporaryDirectory() as directory:
                source = Path(directory)/'source'
                source.write_bytes(b'abc')
                upload = self.start(client.upload(source, '/file'))
                await uploading.wait()
                backlog = [self.start(client.list()) for _ in range(4)]
                await until(lambda: len(client._operations._putters) == 3)
                close = self.start(client.aclose())
                await asyncio.wait_for(cleaning.wait(), 2)
                self.assertFalse(close.done())
                self.assertFalse(client.session.is_closed)
                with self.assertRaises(RuntimeError):
                    await client.list()
                close.cancel()
                release.set()
                with self.assertRaises(asyncio.CancelledError):
                    await asyncio.wait_for(close, 2)
                results = await asyncio.wait_for(asyncio.gather(upload, *backlog, return_exceptions=True), 2)
                self.assertTrue(all(isinstance(result, asyncio.CancelledError) for result in results))
            self.assertTrue(client.session.is_closed)
            self.assertEqual(calls[-1], ('delete', '/file/upload'))
            self.assertEqual(client._active_requests, 0)
            self.assertEqual(client._operations._queue.qsize(), 0)
            self.assertFalse(client._operations._workers)
            self.assertFalse(client._operations._putters)

    async def test_invalid_concurrency(self):
        for value in (0, -1, True, 1.5):
            with self.assertRaises(ValueError):
                AsyncCloudreveV4(max_concurrency=value)

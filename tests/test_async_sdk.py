import asyncio
import base64
import inspect
import json
import ssl
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx

from cloudreve import AsyncCloudreveV4, CloudreveError, CloudreveProtocolError, CloudreveV4
from cloudreve.async_v4 import _io
from test_sdk import Server


class AsyncHTTPTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        cls.server, cls.external = Server(), Server()
        cls.server.other_url = cls.external.url

    @classmethod
    def tearDownClass(cls):
        cls.server.close()
        cls.external.close()

    async def asyncSetUp(self):
        for server in (self.server, self.external):
            server.counts.clear()
            server.records.clear()
            server.peak = 0
        self.client = AsyncCloudreveV4(self.server.url, trust_env=False)
        self.addAsyncCleanup(self.client.aclose)

    async def test_read_retries_writes_and_401_not_replayed(self):
        self.assertEqual(await self.client.request('get', '/retry'), {'ok': True})
        self.assertEqual(self.server.counts['/api/v4/retry'], 3)
        with self.assertRaises(httpx.HTTPStatusError):
            await self.client.request('get', '/always503')
        self.assertEqual(self.server.counts['/api/v4/always503'], 3)
        for method in ('POST', 'PUT', 'PATCH', 'DELETE'):
            self.server.counts.clear()
            with self.assertRaises(httpx.HTTPStatusError):
                await self.client.request(method, '/always503')
            self.assertEqual(self.server.counts['/api/v4/always503'], 1)
        with self.assertRaises(httpx.HTTPStatusError):
            await self.client.request('get', '/unauthorized')
        self.assertEqual(self.server.counts['/api/v4/unauthorized'], 1)

    async def test_protocol_business_and_no_redirect_or_foreign_api(self):
        for path in ('/malformed', '/invalid-json'):
            with self.assertRaises(CloudreveProtocolError):
                await self.client.request('get', path)
        with self.assertRaises(CloudreveError) as raised:
            await self.client.request('get', '/business')
        self.assertEqual(raised.exception.code, 40076)
        self.assertEqual(raised.exception.data, {'expected': 'old'})
        with self.assertRaises(httpx.HTTPStatusError):
            await self.client.request('get', '/redirect')
        with self.assertRaises(ValueError):
            await self.client.request('get', self.external.url + '/capture')
        with self.assertRaises(ValueError):
            await self.client.request('get', '/redirect', follow_redirects=True)
        self.assertEqual(self.external.records, [])

    async def test_shared_pool_concurrency_and_single_refresh(self):
        await self.client.login('user', 'secret')
        await asyncio.gather(*(self.client.request('get', '/parallel') for _ in range(8)))
        self.assertGreater(self.server.peak, 1)
        self.assertEqual(self.server.counts['/api/v4/session/token/refresh'], 1)
        headers = [r[2] for r in self.server.records if '/parallel' in r[1]]
        self.assertTrue(all(h['Authorization'] == 'Bearer new' for h in headers))
        await asyncio.gather(*(self.client.refresh_access_token() for _ in range(8)))
        self.assertEqual(self.server.counts['/api/v4/session/token/refresh'], 2)

    async def test_failed_and_cancelled_refresh_not_replayed(self):
        await self.client.login('user', 'secret')
        failure = httpx.ReadTimeout('refresh response lost')
        original = self.client._request
        async def fail(method, url, **kwargs):
            if url.endswith('/refresh'):
                raise failure
            return await original(method, url, **kwargs)
        with patch.object(self.client, '_request', side_effect=fail) as mock:
            results = await asyncio.gather(*(self.client.list() for _ in range(8)), return_exceptions=True)
            self.assertTrue(all(e is failure for e in results))
            self.assertEqual(mock.call_count, 1)
        await self.client.login('user', 'secret')
        started = asyncio.Event()
        async def wait(*args, **kwargs):
            started.set()
            await asyncio.Event().wait()
        with patch.object(self.client, '_request', side_effect=wait) as mock:
            task = asyncio.create_task(self.client.list())
            await started.wait()
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            with self.assertRaisesRegex(RuntimeError, 'log in again'):
                await self.client.list()
            self.assertEqual(mock.call_count, 1)

    async def test_network_timeout_and_retry_cancellation(self):
        with self.assertRaises(httpx.RemoteProtocolError):
            await self.client.request('get', '/drop')
        self.assertEqual(self.server.counts['/api/v4/drop'], 3)
        self.client.read_retries = 0
        with self.assertRaises(httpx.ReadTimeout):
            await self.client.request('get', '/slow', timeout=0.01)
        self.assertEqual(self.server.counts['/api/v4/slow'], 1)
        self.client.read_retries = 2
        started = asyncio.Event()
        async def delay(*args):
            started.set()
            await asyncio.Event().wait()
        with patch('cloudreve.async_v4.asyncio.sleep', side_effect=delay):
            task = asyncio.create_task(self.client.request('get', '/always503'))
            await started.wait()
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertEqual(self.server.counts['/api/v4/always503'], 1)

    async def test_tls_failure_never_retried(self):
        async def fail(*args, **kwargs):
            raise httpx.ConnectError('certificate') from ssl.SSLCertVerificationError('certificate')
        with patch.object(self.client.session, 'send', side_effect=fail) as mock:
            with self.assertRaises(httpx.ConnectError):
                await self.client.list()
            self.assertEqual(mock.call_count, 1)

    async def test_download_isolation_strict_origin_and_atomic_failure(self):
        self.client.session.headers.update({'Authorization': 'Bearer private', 'X-Secret': 'private'})
        self.client.session.cookies.set('cloudreve-session', 'secret')
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory)/'out'
            await self.client.download('/file', target)
            self.assertEqual(target.read_bytes(), b'file contents')
            headers = {k.lower(): v for k, v in self.external.records[-1][2].items()}
            self.assertFalse(set(headers) & {'authorization', 'cookie', 'x-secret'})
            self.client.allow_external_downloads = False
            with self.assertRaises(ValueError):
                await self.client.download('/file', target)
            for suffix, exception in (('/partial', httpx.RemoteProtocolError), ('/range', httpx.HTTPStatusError)):
                with self.assertRaises(exception):
                    await self.client._download(self.server.url + suffix, target)
                self.assertEqual(target.read_bytes(), b'file contents')
                self.assertEqual(list(Path(directory).iterdir()), [target])

    async def test_closed_client(self):
        await self.client.aclose()
        with self.assertRaises(RuntimeError):
            await self.client.list()
        with self.assertRaises(RuntimeError):
            await self.client.__aenter__()


class AsyncTransferTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.client = AsyncCloudreveV4('https://cloudreve.test', trust_env=False)
        self.addAsyncCleanup(self.client.aclose)
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.source = Path(self.directory.name)/'source'
        self.source.write_bytes(b'abcdefghij')
        self.policy = {'id': 'policy', 'type': 'local'}
        self.client.list = AsyncMock(return_value={'storage_policy': self.policy})
        self.client.get_info = AsyncMock(return_value={'size': 10})
        self.records = []
        async def handler(request):
            body = await request.aread()
            self.records.append((request.method, request.url, dict(request.headers), body))
            data = {'session_id': 'session', 'chunk_size': 4}
            return httpx.Response(200, json={'code': 0, 'data': data}, request=request)
        await self.client.session.aclose()
        self.client.session = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        self.client._external = httpx.AsyncClient(transport=httpx.MockTransport(handler), trust_env=False)

    async def test_upload_stream_chunks_empty_and_version(self):
        await self.client.upload(self.source, '/dir/file', previous='old')
        self.assertEqual(json.loads(self.records[0][3])['previous'], 'old')
        self.assertEqual(json.loads(self.records[0][3])['entity_type'], 'version')
        chunks = self.records[1:]
        self.assertEqual([r[3] for r in chunks], [b'abcd', b'efgh', b'ij'])
        self.assertEqual([r[2]['content-length'] for r in chunks], ['4', '4', '2'])
        self.records.clear()
        self.source.write_bytes(b'')
        await self.client._upload_chunks(self.source, 'session', 0)
        self.assertEqual(len(self.records), 1)
        self.assertEqual(self.records[0][3], b'')
        self.assertEqual(self.records[0][2]['content-length'], '0')

    async def test_stream_buffer_bounded(self):
        self.source.write_bytes(b'x' * (1024 * 1024 + 17))
        lengths = []
        async def consume(request, **kwargs):
            async for chunk in request.stream:
                lengths.append(len(chunk))
            return httpx.Response(200, json={'code': 0}, request=request)
        # MockTransport eagerly buffers the body; intercept at send for this assertion.
        with patch.object(self.client.session, 'send', side_effect=consume):
            await self.client._upload_chunks(self.source, 'session', 0)
        self.assertEqual(sum(lengths), self.source.stat().st_size)
        self.assertLessEqual(max(lengths), 65536)

    async def test_remote_and_onedrive_headers(self):
        self.client.session.headers['Authorization'] = 'Bearer login'
        self.client._external.cookies.set('cloudreve-session', 'foreign-cookie')
        await self.client._upload_chunks(self.source, 's', 4, mode='remote',
                                          upload_urls=['https://remote.test/upload'], credential='upload-only')
        self.assertEqual([r[1].params['chunk'] for r in self.records], ['0', '1', '2'])
        self.assertTrue(all(r[2]['authorization'] == 'upload-only' and 'cookie' not in r[2] for r in self.records))
        self.records.clear()
        await self.client._upload_chunks(self.source, 's', 4, mode='onedrive',
                                          upload_urls=['https://onedrive.test/upload'])
        self.assertEqual([r[2]['content-range'] for r in self.records], ['bytes 0-3/10', 'bytes 4-7/10', 'bytes 8-9/10'])
        self.assertTrue(all('authorization' not in r[2] for r in self.records))

    async def test_upload_failure_cleanup_and_creation_unknown(self):
        error = httpx.WriteError('lost write')
        with patch.object(self.client, '_upload_chunks', side_effect=error):
            with self.assertRaises(httpx.WriteError) as raised:
                await self.client.upload(self.source, '/dir/file')
        self.assertIs(raised.exception, error)
        self.assertEqual(error.upload_session_id, 'session')
        self.assertEqual(self.records[-1][0], 'DELETE')
        self.records.clear()
        with patch.object(self.client, 'request', side_effect=httpx.ReadTimeout('session response lost')) as mock:
            with self.assertRaises(httpx.ReadTimeout):
                await self.client.upload(self.source, '/dir/file')
            self.assertEqual(mock.call_count, 1)
        self.assertFalse(self.records)

    async def test_upload_repeated_cancellation_drains_cleanup(self):
        uploading, cleaning, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
        async def upload(*args, **kwargs):
            uploading.set()
            await asyncio.Event().wait()
        original = self.client._request
        async def request(method, url, **kwargs):
            if method == 'delete':
                cleaning.set()
                await release.wait()
            return await original(method, url, **kwargs)
        observed = []
        async def observe():
            try:
                await self.client.upload(self.source, '/dir/file')
            except asyncio.CancelledError as error:
                observed.append(error)
                raise
        with patch.object(self.client, '_upload_chunks', side_effect=upload), patch.object(self.client, '_request', side_effect=request):
            task = asyncio.create_task(observe())
            await uploading.wait()
            task.cancel()
            await cleaning.wait()
            task.cancel()
            await asyncio.sleep(0)
            self.assertFalse(task.done())
            release.set()
            with self.assertRaises(asyncio.CancelledError) as raised:
                await task
        self.assertEqual(observed[0].upload_session_id, 'session')
        self.assertFalse(hasattr(observed[0], 'cleanup_error'))
        error = raised.exception if sys.version_info >= (3, 9) else observed[0]
        # Python 3.10 Task copies CancelledError and retains the original in __context__.
        while not hasattr(error, 'upload_session_id') and error.__context__ is not None:
            error = error.__context__
        self.assertEqual(error.upload_session_id, 'session')
        self.assertFalse(hasattr(error, 'cleanup_error'))
        self.assertEqual(self.records[-1][0], 'DELETE')

    async def test_cleanup_failure_preserves_original(self):
        error = httpx.WriteError('upload failed')
        original = self.client._request
        async def request(method, url, **kwargs):
            if method == 'delete':
                raise httpx.ReadTimeout('cleanup response lost')
            return await original(method, url, **kwargs)
        with patch.object(self.client, '_upload_chunks', side_effect=error), patch.object(self.client, '_request', side_effect=request):
            with self.assertRaises(httpx.WriteError) as raised:
                await self.client.upload(self.source, '/dir/file')
        self.assertIs(raised.exception, error)
        self.assertIsInstance(error.cleanup_error, httpx.ReadTimeout)

    async def test_size_mismatch_unsupported_policy_and_invalid_version(self):
        self.client.get_info.return_value = {'size': 9}
        with self.assertRaisesRegex(ValueError, 'size differs'):
            await self.client.upload(self.source, '/dir/file')
        self.assertFalse(any(r[0] == 'DELETE' for r in self.records))
        for policy in ({'id': 'p', 'type': 'oss'}, {'id': 'p', 'type': 'onedrive'}):
            self.client.list.return_value = {'storage_policy': policy}
            self.source.write_bytes(b'')
            self.records.clear()
            with self.assertRaises(ValueError):
                await self.client.upload(self.source, '/dir/file')
            self.assertFalse(self.records)
        self.client.list.return_value = {'storage_policy': self.policy}
        with self.assertRaises(ValueError):
            await self.client.upload(self.source, '/dir/file', previous='')
        with self.assertRaises(ValueError):
            await self.client.upload(self.source, '/')

    async def test_onedrive_failure_has_no_callback(self):
        self.client.list.return_value = {'storage_policy': {'id': 'p', 'type': 'onedrive'}}
        async def handler(request):
            self.records.append((request.method, request.url.path))
            if request.url.path == '/api/v4/file/upload':
                return httpx.Response(200, json={'code': 0, 'data': dict(session_id='s', chunk_size=4,
                    upload_urls=['https://onedrive.test/upload'], callback_secret='secret')})
            return httpx.Response(503)
        self.client.session._transport = httpx.MockTransport(handler)
        self.client._external._transport = httpx.MockTransport(handler)
        with self.assertRaises(httpx.HTTPStatusError):
            await self.client.upload(self.source, '/dir/file')
        self.assertFalse(any('/callback/' in r[1] for r in self.records))
        self.assertEqual(sum(r[0] == 'PUT' and r[1] == '/upload' for r in self.records), 1)
        self.assertEqual(self.records[-1][0], 'DELETE')

    async def test_download_cancellation_removes_temporary_and_closes(self):
        started = asyncio.Event()
        class Stream(httpx.AsyncByteStream):
            closed = False
            async def __aiter__(self):
                yield b'x' * 65536
                started.set()
                await asyncio.Event().wait()
            async def aclose(self):
                self.closed = True
        stream = Stream()
        async def handler(request):
            return httpx.Response(200, stream=stream, request=request)
        self.client._external._transport = httpx.MockTransport(handler)
        target = Path(self.directory.name)/'target'
        target.write_bytes(b'old')
        task = asyncio.create_task(self.client._download('https://cloudreve.test/file', target))
        await started.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(target.read_bytes(), b'old')
        self.assertEqual(set(Path(self.directory.name).iterdir()), {self.source, target})
        self.assertTrue(stream.closed)

    async def test_cancelled_disk_io_is_drained_before_return(self):
        started, release = threading.Event(), threading.Event()
        def write():
            started.set()
            release.wait(2)
            return 42
        task = asyncio.create_task(_io(write))
        while not started.is_set():
            await asyncio.sleep(0.001)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        self.assertFalse(task.done())
        release.set()
        with self.assertRaises(asyncio.CancelledError):
            await task


class AsyncContractTests(unittest.IsolatedAsyncioTestCase):
    async def test_all_v4_operations_are_coroutines(self):
        names = [name for name, value in CloudreveV4.__dict__.items()
                 if not name.startswith('_') and callable(value)]
        for name in names:
            self.assertTrue(inspect.iscoroutinefunction(getattr(AsyncCloudreveV4, name)), name)

    async def test_payloads_aliases_cursor_and_utf8(self):
        async with AsyncCloudreveV4(trust_env=False) as client:
            cursor = base64.b64encode(json.dumps({'start_with_file': True, 'id': 'folder'}).encode()).decode()
            client.request = AsyncMock(return_value={'files': [{'id': 'folder', 'type': 1}],
                'pagination': {'is_cursor': True, 'next_token': cursor}})
            result = await client.list('/目录/a #?', order='desc')
            params = client.request.call_args.kwargs['params']
            self.assertEqual(params['uri'], 'cloudreve://my/%E7%9B%AE%E5%BD%95/a%20%23%3F')
            self.assertEqual(params['order_direction'], 'desc')
            self.assertFalse(json.loads(base64.b64decode(result['pagination']['next_token']))['start_with_file'])
            await client.create_directory('/folder', True)
            self.assertEqual(client.request.call_args.kwargs['json']['type'], 'folder')
            await client.update_file_content('/f', '你好', previous='old')
            self.assertEqual(client.request.call_args.kwargs['content'], '你好'.encode())
            self.assertEqual(client.request.call_args.kwargs['params']['previous'], 'old')
            await client.copy('/f', '/d')
            self.assertTrue(client.request.call_args.kwargs['json']['copy'])
            await client.move('/f', '/d')
            self.assertFalse(client.request.call_args.kwargs['json']['copy'])
            await client.remove('/f', trash_bin=False)
            self.assertTrue(client.request.call_args.kwargs['json']['skip_soft_delete'])
            client.request.return_value = {'urls': [{'url': '/api/v4/file/download'}]}
            self.assertEqual(await client.get_download_url('/f'), 'http://127.0.0.1:5212/api/v4/file/download')
            client.request.return_value = [{'link': 'source'}]
            self.assertEqual(await client.get_source_url('/f'), 'source')
            self.assertEqual(await client.get_source_url(['/f']), [{'link': 'source'}])

    async def test_constructor_validation_proxy_and_env_isolation(self):
        for options in ({'timeout': None}, {'timeout': (1, 2, 3)}, {'timeout': float('nan')},
                        {'read_retries': -1}, {'base_url': 'https://u:p@host'},
                        {'base_url': 'https://host?token=x'}):
            with self.assertRaises(ValueError):
                AsyncCloudreveV4(**options)
        with patch.dict('os.environ', {'HTTPS_PROXY': 'http://127.0.0.1:1', 'NO_PROXY': ''}):
            async with AsyncCloudreveV4(trust_env=True) as client:
                self.assertTrue(client.session._trust_env)
                self.assertFalse(client._external_session()._trust_env)
            async with AsyncCloudreveV4(proxy={'http': 'http://127.0.0.1:2', 'https': 'http://127.0.0.1:3'}, trust_env=False) as client:
                self.assertEqual(len(client.session._mounts), 2)
                self.assertEqual(len(client._external_session()._mounts), 2)


if __name__ == '__main__':
    unittest.main()

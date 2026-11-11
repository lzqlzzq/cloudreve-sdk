"""Fault matrix: explicit outcomes, attempt budgets and preservation of caller data."""
import asyncio
import errno
import gzip
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx
from cloudreve import AsyncCloudreveV4, CloudreveError, CloudreveProtocolError
import test_async_sdk as fixtures


class RetryMatrixTests(unittest.IsolatedAsyncioTestCase):
    async def test_status_method_retry_matrix(self):
        for status in (200, 301, 401, 403, 404, 408, 409, 429, 500, 502, 503, 504):
            for method in ('GET', 'HEAD', 'POST', 'PUT', 'PATCH', 'DELETE'):
                with self.subTest(status=status, method=method):
                    calls = []
                    async def handler(request):
                        calls.append(request)
                        return httpx.Response(status, json={'code': 0, 'data': 'ok'}, headers={'Retry-After': '0'})
                    async with AsyncCloudreveV4(trust_env=False) as client:
                        await client.session.aclose()
                        client.session = httpx.AsyncClient(transport=httpx.MockTransport(handler))
                        response = await client._send(client.session, method, client.base_url + '/matrix')
                        await response.aclose()
                        expected = 3 if method in ('GET', 'HEAD') and status in (429, 502, 503, 504) else 1
                        self.assertEqual(len(calls), expected)

    async def test_retry_after_date_invalid_bounds_and_backoff(self):
        cases = [('2', 2), ('999999', 30), ('-5', 0),
                 ('Thu, 01 Jan 1970 00:16:45 GMT', 5),
                 ('Thu, 01 Jan 1970 00:00:00 GMT', 0), ('invalid', .25), (None, .25)]
        for header, delay in cases:
            with self.subTest(header=header):
                attempts = 0
                async def handler(request):
                    nonlocal attempts
                    attempts += 1
                    return httpx.Response(503 if attempts == 1 else 200,
                        headers={'Retry-After': header} if header is not None else {})
                async with AsyncCloudreveV4(trust_env=False) as client:
                    await client.session.aclose()
                    client.session = httpx.AsyncClient(transport=httpx.MockTransport(handler))
                    with patch('cloudreve.async_v4.asyncio.sleep', new_callable=AsyncMock) as sleep, \
                         patch('cloudreve.async_v4.time.time', return_value=1000), \
                         patch('cloudreve.async_v4.random.uniform', return_value=0):
                        response = await client._send(client.session, 'GET', client.base_url)
                        await response.aclose()
                        sleep.assert_awaited_once_with(delay)
                self.assertEqual(attempts, 2)

    async def test_network_exception_matrix(self):
        exceptions = (httpx.ConnectTimeout, httpx.ReadTimeout, httpx.WriteTimeout,
                      httpx.PoolTimeout, httpx.ConnectError, httpx.ReadError, httpx.WriteError,
                      httpx.RemoteProtocolError, httpx.LocalProtocolError, httpx.UnsupportedProtocol)
        for exception in exceptions:
            for method in ('GET', 'POST'):
                with self.subTest(exception=exception.__name__, method=method):
                    async with AsyncCloudreveV4(trust_env=False) as client:
                        with patch.object(client.session, 'send', side_effect=exception('injected')) as send, \
                             patch('cloudreve.async_v4.asyncio.sleep', new_callable=AsyncMock):
                            with self.assertRaises(exception):
                                await client.request(method, '/fault')
                            expected = 3 if method == 'GET' and exception not in (httpx.LocalProtocolError, httpx.UnsupportedProtocol) else 1
                            self.assertEqual(send.call_count, expected)

    async def test_invalid_envelopes_do_not_retry(self):
        for payload in ([], None, {'code': True}, {'code': '0'}, {'code': 203, 'data': {'failed': ['a']}}):
            async with AsyncCloudreveV4(trust_env=False) as client:
                async def handler(request):
                    return httpx.Response(200, json=payload)
                await client.session.aclose()
                client.session = httpx.AsyncClient(transport=httpx.MockTransport(handler))
                with patch.object(client.session, 'send', wraps=client.session.send) as send:
                    with self.assertRaises((CloudreveProtocolError, CloudreveError)):
                        await client.list()
                    self.assertEqual(send.call_count, 1)

    async def test_real_connection_pool_timeout_recovers(self):
        entered, release = asyncio.Event(), asyncio.Event()
        writers = set()
        async def server(reader, writer):
            writers.add(writer)
            try:
                await reader.readuntil(b'\r\n\r\n')
                entered.set()
                await release.wait()
                body = b'{"code":0,"data":true}'
                writer.write(b'HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: '+str(len(body)).encode()+b'\r\n\r\n'+body)
                await writer.drain()
            finally:
                writer.close()
                await writer.wait_closed()
                writers.discard(writer)
        listener = await asyncio.start_server(server, '127.0.0.1', 0)
        try:
            port = listener.sockets[0].getsockname()[1]
            async with AsyncCloudreveV4(f'http://127.0.0.1:{port}', read_retries=0, trust_env=False) as client:
                await client.session.aclose()
                client.session = httpx.AsyncClient(limits=httpx.Limits(max_connections=1), timeout=.05)
                first = asyncio.create_task(client.request('GET', '/first', timeout=2))
                try:
                    await asyncio.wait_for(entered.wait(), 2)
                    with self.assertRaises(httpx.PoolTimeout):
                        await client.request('GET', '/second')
                finally:
                    release.set()
                    await first
                self.assertTrue(await client.request('GET', '/after'))
        finally:
            release.set()
            listener.close()
            await listener.wait_closed()
            for writer in list(writers):
                writer.close()


class FileFaultTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = fixtures.AsyncTransferTests.asyncSetUp

    async def test_disk_full_and_permission_failures_preserve_destination(self):
        target = Path(self.directory.name)/'target'
        target.write_bytes(b'old')
        for failure in (OSError(errno.ENOSPC, 'disk full'), PermissionError(errno.EACCES, 'denied')):
            with self.subTest(failure=type(failure).__name__):
                with patch('cloudreve.async_v4._io', side_effect=failure):
                    with self.assertRaises(type(failure)):
                        await self.client._download('https://cloudreve.test/file', target)
                self.assertEqual(target.read_bytes(), b'old')
                self.assertEqual(set(Path(self.directory.name).iterdir()), {self.source, target})
        with patch('cloudreve.async_v4.NamedTemporaryFile', side_effect=PermissionError('cannot create')):
            with self.assertRaises(PermissionError):
                await self.client._download('https://cloudreve.test/file', target)
        with patch('cloudreve.async_v4.os.replace', side_effect=PermissionError('cannot replace')):
            with self.assertRaises(PermissionError):
                await self.client._download('https://cloudreve.test/file', target)
        self.assertEqual(target.read_bytes(), b'old')
        self.assertEqual(set(Path(self.directory.name).iterdir()), {self.source, target})

    async def test_upload_read_error_cancels_known_session(self):
        with patch('cloudreve.async_v4._io', side_effect=OSError(errno.EIO, 'read failed')):
            with self.assertRaises(OSError) as caught:
                await self.client.upload(self.source, '/file')
        self.assertEqual(caught.exception.errno, errno.EIO)
        self.assertEqual(self.records[-1][0], 'DELETE')

    async def test_source_truncation_growth_replacement_and_same_size_edit(self):
        for change in ('truncate', 'grow', 'replace', 'edit'):
            with self.subTest(change=change):
                self.source.write_bytes(b'abcdefghij')
                self.records.clear()
                original = self.client._upload_chunks
                async def changed(*args, **kwargs):
                    if change == 'truncate':
                        # Mutate between two real chunk reads.
                        original_io = __import__('cloudreve.async_v4', fromlist=['_io'])._io
                        reads = 0
                        async def io(function, *args):
                            nonlocal reads
                            result = await original_io(function, *args)
                            reads += 1
                            if reads == 1:
                                self.source.write_bytes(b'')
                            return result
                        with patch('cloudreve.async_v4._io', side_effect=io):
                            await original(*args, **kwargs)
                    else:
                        await original(*args, **kwargs)
                        if change == 'grow':
                            with self.source.open('ab') as file:
                                file.write(b'extra')
                        elif change == 'replace':
                            replacement = Path(self.directory.name)/'replacement'
                            replacement.write_bytes(b'0123456789')
                            replacement.replace(self.source)
                        else:
                            stamp = self.source.stat().st_mtime_ns
                            self.source.write_bytes(b'0123456789')
                            os.utime(self.source, ns=(stamp, stamp + 1_000_000))
                with patch.object(self.client, '_upload_chunks', side_effect=changed):
                    with self.assertRaises(ValueError):
                        await self.client.upload(self.source, '/file')
                self.assertEqual(self.records[-1][0], 'DELETE')

    async def test_compressed_download_checks_decoded_content(self):
        class Stream(httpx.AsyncByteStream):
            async def __aiter__(self):
                yield gzip.compress(b'x' * 200000)
        async def handler(request):
            payload = gzip.compress(b'x' * 200000)
            return httpx.Response(200, headers={'Content-Encoding': 'gzip', 'Content-Length': str(len(payload))}, stream=Stream())
        self.client._external._transport = httpx.MockTransport(handler)
        target = Path(self.directory.name)/'target'
        await self.client._download('https://cloudreve.test/file', target)
        self.assertEqual(target.read_bytes(), b'x' * 200000)

    async def test_onedrive_success_callback_and_remote_empty(self):
        self.client.list.return_value = {'storage_policy': {'id': 'p', 'type': 'onedrive'}}
        original = self.client._request
        async def request(method, url, **kwargs):
            if method == 'put' and url == '/file/upload':
                return {'session_id': 's', 'chunk_size': 4, 'upload_urls': ['https://remote.test/file'], 'callback_secret': 'c'}
            return await original(method, url, **kwargs)
        with patch.object(self.client, '_request', side_effect=request):
            await self.client.upload(self.source, '/file')
        self.assertEqual([r[2].get('content-range') for r in self.records[:3]], ['bytes 0-3/10', 'bytes 4-7/10', 'bytes 8-9/10'])
        self.assertEqual(self.records[-1][1].path, '/api/v4/callback/onedrive/s/c')
        self.records.clear()
        self.source.write_bytes(b'')
        await self.client._upload_chunks(self.source, 's', 0, mode='remote', upload_urls=['https://remote.test/file'], credential='upload')
        self.assertEqual(len(self.records), 1)
        self.assertEqual(self.records[0][3], b'')


class SyncSourceMutationTests(unittest.TestCase):
    def test_sync_upload_reports_source_mutation_and_cleans_session(self):
        from cloudreve import CloudreveV4
        from unittest.mock import Mock
        with tempfile.TemporaryDirectory() as directory, CloudreveV4(trust_env=False) as client:
            source = Path(directory)/'file'
            source.write_bytes(b'old')
            client.list = Mock(return_value={'storage_policy': {'id': 'p', 'type': 'local'}})
            client.request = Mock(return_value={'session_id': 's', 'chunk_size': 0})
            client._upload_to_local = Mock(side_effect=lambda **kw: source.write_bytes(b'changed'))
            with self.assertRaisesRegex(ValueError, 'Source file changed') as caught:
                client.upload(source, '/file')
            self.assertEqual(caught.exception.upload_session_id, 's')
            self.assertEqual(client.request.call_args.args, ('delete', '/file/upload'))


class BoundaryTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = fixtures.AsyncTransferTests.asyncSetUp

    async def test_bad_session_policy_chunk_sizes_and_eof_cleanup(self):
        with self.assertRaises(FileNotFoundError):
            await self.client.upload(self.source.with_name('missing'), '/file')
        self.assertFalse(self.records)
        for chunk_size in (-1, True, '4'):
            with self.assertRaises(ValueError):
                await self.client._upload_chunks(self.source, 's', chunk_size)
        with self.assertRaises(ValueError):
            await self.client._upload_chunks(self.source, 's', 0, mode='onedrive', upload_urls=['https://drive.test/u'])
        original = self.client.request
        async def request(method, url, **kwargs):
            if method == 'put' and url == '/file/upload':
                return {'session_id': 's', 'chunk_size': 4, 'storage_policy': {'type': 'unexpected'}}
            return await original(method, url, **kwargs)
        with patch.object(self.client, 'request', side_effect=request):
            with self.assertRaises(ValueError):
                await self.client.upload(self.source, '/file')
        self.assertEqual(self.records[-1][0], 'DELETE')
        with patch('cloudreve.async_v4._io', return_value=b''):
            with self.assertRaisesRegex(ValueError, 'truncated'):
                await self.client.upload(self.source, '/file')
        self.assertEqual(self.records[-1][0], 'DELETE')

    async def test_decode_and_declared_length_failure_are_atomic(self):
        class Stream(httpx.AsyncByteStream):
            closed = False
            async def __aiter__(self):
                yield b'bad bytes'
            async def aclose(self):
                self.closed = True
        target = Path(self.directory.name)/'target'
        target.write_bytes(b'old')
        for headers, expected in (({'Content-Length': '100'}, httpx.RemoteProtocolError),
                                  ({'Content-Encoding': 'gzip'}, httpx.DecodingError)):
            stream = Stream()
            async def handler(request):
                return httpx.Response(200, headers=headers, stream=stream)
            self.client._external._transport = httpx.MockTransport(handler)
            with self.assertRaises(expected):
                await self.client._download('https://cloudreve.test/file', target)
            self.assertTrue(stream.closed)
            self.assertEqual(target.read_bytes(), b'old')
            self.assertEqual(set(Path(self.directory.name).iterdir()), {self.source, target})

    async def test_remaining_operation_payloads_and_auth_boundaries(self):
        async with AsyncCloudreveV4('https://cloudreve.test/api/v4', trust_env=False, cloudreve_session='cookie') as client:
            self.assertEqual(client.session.cookies['cloudreve-session'], 'cookie')
            self.assertEqual(client.base_url, 'https://cloudreve.test/api/v4')
            with self.assertRaisesRegex(ValueError, 'log in first'):
                await client.refresh_access_token()
            with patch.object(client, '_request', new_callable=AsyncMock) as request:
                await client.create_file('/file', True)
                self.assertEqual(request.call_args.kwargs['json'], {'uri': 'cloudreve://my/file', 'type': 'file', 'err_on_conflict': True})
                await client.rename('/file', 'new')
                self.assertEqual(request.call_args.kwargs['json']['new_name'], 'new')
                request.return_value = 'https://cloudreve.test/share/example'
                self.assertEqual(await client.get_share_url('/file', downloads=2, expire=60, password='secret'), request.return_value)
                self.assertEqual(request.call_args.kwargs['json']['is_private'], True)
                await client.update_file_content('/file', 'text')
                self.assertEqual(request.call_args.kwargs['params'], {'uri': 'cloudreve://my/file'})
                for previous in ('', 1):
                    with self.assertRaises(ValueError):
                        await client.update_file_content('/file', 'text', previous=previous)
        with self.assertRaises(RuntimeError):
            client._external_session()
        with self.assertRaises(RuntimeError):
            await client._send(client.session, 'GET', client.base_url)

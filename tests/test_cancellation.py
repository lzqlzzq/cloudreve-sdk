"""Cancellation is an acknowledgement, not a rollback or proof of OSS cleanup."""
import asyncio
import unittest
from unittest.mock import AsyncMock, Mock, patch

import httpx
from requests.exceptions import ReadTimeout
from cloudreve import CloudreveError
from cloudreve.transport import envelope_data
import test_sdk
import test_async_sdk


CANCEL_ERROR = {'code': -1, 'msg': '', 'correlation_id': 'cancel-request',
                'error': 'AbortMultipartUpload failed: private signed URL',
                'aggregated_error': {'item': {'code': 404}}}


class ErrorTests(unittest.TestCase):
    def test_server_diagnostics_preserved_without_implicit_logging(self):
        with self.assertRaises(CloudreveError) as caught:
            envelope_data(CANCEL_ERROR)
        error = caught.exception
        self.assertEqual(error.code, -1)
        self.assertEqual(error.correlation_id, 'cancel-request')
        self.assertEqual(error.detail, CANCEL_ERROR['error'])
        self.assertEqual(error.aggregated_error, CANCEL_ERROR['aggregated_error'])
        self.assertNotIn('private signed URL', str(error))


class SyncCancellationTests(unittest.TestCase):
    setUp = test_sdk.UploadTests.setUp

    def test_cleanup_outcome_matrix_never_swallows_or_retries(self):
        for code in (0, -1, 40016, 40011):
            with self.subTest(code=code):
                failure = ReadTimeout('upload response lost')
                calls = []
                def request(method, url, **kwargs):
                    calls.append(method)
                    if method == 'put':
                        return {'session_id': 's', 'chunk_size': 0, 'expires': 123}
                    if method == 'post':
                        raise failure
                    return envelope_data(dict(CANCEL_ERROR, code=code))
                self.client.request = request
                with self.assertRaises(ReadTimeout) as caught:
                    self.client.upload(self.file, '/file')
                self.assertIs(caught.exception, failure)
                self.assertFalse(failure.upload_acknowledged)
                self.assertEqual(failure.upload_session_expires, 123)
                self.assertEqual(failure.cleanup_acknowledged, code == 0)
                self.assertEqual(calls, ['put', 'post', 'delete'])
                if code:
                    self.assertEqual(failure.cleanup_error.code, code)
                    self.assertEqual(failure.cleanup_error.correlation_id, 'cancel-request')
                    self.assertIs(failure.__cause__, failure.cleanup_error)

    def test_post_upload_read_failure_keeps_completed_file(self):
        failure = ReadTimeout('verification unavailable')
        self.client.get_info.side_effect = failure
        self.client.request = Mock(side_effect=[{'session_id':'s', 'chunk_size':0}, None])
        with self.assertRaises(ReadTimeout) as caught:
            self.client.upload(self.file, '/file')
        self.assertIs(caught.exception, failure)
        self.assertTrue(failure.upload_acknowledged)
        self.assertIsNone(failure.cleanup_acknowledged)
        self.assertEqual(self.client.request.call_count, 2)


class AsyncCancellationTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = test_async_sdk.AsyncTransferTests.asyncSetUp

    async def test_cancel_during_cleanup_does_not_invent_a_cleanup_failure(self):
        started, release = asyncio.Event(), asyncio.Event()
        failure = httpx.WriteError('upload failed')
        original = self.client._request
        async def request(method, url, **kwargs):
            if method == 'delete':
                started.set()
                await release.wait()
                return None
            return await original(method, url, **kwargs)
        with patch.object(self.client, '_request', side_effect=request), \
             patch.object(self.client, '_upload_chunks', side_effect=failure):
            observed = []
            async def operation():
                try:
                    await self.client.upload(self.source, '/file')
                except asyncio.CancelledError as error:
                    observed.append(error)
                    raise
            task = asyncio.create_task(operation())
            await asyncio.wait_for(started.wait(), 2)
            task.cancel()
            release.set()
            with self.assertRaises(asyncio.CancelledError):
                await asyncio.wait_for(task, 2)
        self.assertIs(observed[0].__cause__, failure)
        self.assertTrue(failure.cleanup_acknowledged)
        self.assertFalse(hasattr(failure, 'cleanup_error'))

    async def test_cleanup_outcome_matrix_preserves_cancel_and_diagnostics(self):
        for code in (0, -1, 40016, 40011):
            with self.subTest(code=code):
                self.records.clear()
                original = self.client._request
                async def request(method, url, **kwargs):
                    if method == 'delete':
                        self.records.append(('DELETE',))
                        return envelope_data(dict(CANCEL_ERROR, code=code))
                    return await original(method, url, **kwargs)
                with patch.object(self.client, '_request', side_effect=request), \
                     patch.object(self.client, '_upload_chunks', side_effect=asyncio.CancelledError()):
                    # Catch inside the directly awaiting coroutine for Python 3.8.
                    observed = []
                    async def operation():
                        try:
                            await self.client.upload(self.source, '/file')
                        except asyncio.CancelledError as error:
                            observed.append(error)
                    await operation()
                error = observed[0]
                self.assertFalse(error.upload_acknowledged)
                self.assertEqual(error.cleanup_acknowledged, code == 0)
                self.assertEqual(sum(r[0] == 'DELETE' for r in self.records), 1)
                if code:
                    self.assertEqual(error.cleanup_error.code, code)
                    self.assertEqual(error.cleanup_error.correlation_id, 'cancel-request')

    async def test_verification_cancel_and_timeout_do_not_cancel_completed_upload(self):
        for failure in (asyncio.CancelledError(), httpx.ReadTimeout('verification response lost')):
            with self.subTest(failure=type(failure).__name__):
                self.records.clear()
                with patch.object(self.client, 'get_info', side_effect=failure):
                    observed = []
                    async def operation():
                        try:
                            await self.client.upload(self.source, '/file')
                        except BaseException as error:
                            observed.append(error)
                    await operation()
                error = observed[0]
                self.assertIs(error, failure)
                self.assertTrue(error.upload_acknowledged)
                self.assertIsNone(error.cleanup_acknowledged)
                self.assertFalse(any(r[0] == 'DELETE' for r in self.records))

    async def test_onedrive_needs_callback_acknowledgement(self):
        self.client.list.return_value = {'storage_policy': {'id':'p', 'type':'onedrive'}}
        for callback_fails in (True, False):
            calls = []
            async def request(method, url, **kwargs):
                calls.append((method, url))
                if method == 'put':
                    return {'session_id':'s', 'chunk_size':4, 'callback_secret':'c'}
                if url.startswith('/callback/') and callback_fails:
                    raise httpx.ReadTimeout('callback outcome unknown')
            with patch.object(self.client, '_request', side_effect=request), \
                 patch.object(self.client, '_upload_chunks', new_callable=AsyncMock), \
                 patch.object(self.client, 'get_info', side_effect=httpx.ReadTimeout('verification unavailable')):
                with self.assertRaises(httpx.ReadTimeout) as caught:
                    await self.client.upload(self.source, '/file')
            self.assertEqual(caught.exception.upload_acknowledged, not callback_fails)
            self.assertEqual(any(method == 'delete' for method, _ in calls), callback_fails)

import base64
import json
import socket
import tempfile
import threading
import time
import unittest
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import Mock, patch

from requests import HTTPError
from requests.exceptions import ChunkedEncodingError, ConnectionError, ReadTimeout

from cloudreve import CloudreveError, CloudreveProtocolError, CloudreveV3, CloudreveV4
from cloudreve.utils import download_file
from cloudreve.v4 import revise_file_path


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        self.handle_request()

    do_POST = do_PUT = do_DELETE = do_PATCH = do_GET

    def handle_request(self):
        size = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(size) if size else b''
        path = self.path.split('?')[0]
        with self.server.lock:
            self.server.counts[path] += 1
            count = self.server.counts[path]
            self.server.records.append((self.command, self.path, dict(self.headers), body))
        status, headers = 200, {}
        payload = {'code': 0, 'data': {'ok': True}}
        if path.endswith('/retry') and count < 3:
            status, headers = 503, {'Retry-After': '0'}
        if path.endswith('/always503'):
            status, headers = 503, {'Retry-After': '0'}
        if path.endswith('/business'):
            payload = {'code': 40076, 'msg': 'version conflict', 'data': {'expected': 'old'}}
        if path.endswith('/malformed'):
            payload = {'message': 'bad gateway'}
        if path.endswith('/invalid-json'):
            payload = b'<html>bad gateway</html>'
        if path.endswith('/unauthorized'):
            status = 401
        if path.endswith('/redirect'):
            status, headers = 302, {'Location': self.server.other_url + '/capture'}
        if path.endswith('/drop'):
            self.connection.shutdown(socket.SHUT_RDWR)
            self.connection.close()
            return
        if path.endswith('/slow'):
            threading.Event().wait(0.06)
        if path.endswith('/parallel'):
            with self.server.lock:
                self.server.active += 1
                self.server.peak = max(self.server.peak, self.server.active)
            time.sleep(0.05)
            with self.server.lock:
                self.server.active -= 1
        if path.endswith('/session/token'):
            payload = {'code': 0, 'data': {'user': {'id': 'test'}, 'token': {
                'access_token': 'old', 'refresh_token': 'refresh',
                'access_expires': '2020-01-01T00:00:00+00:00'}}}
        if path.endswith('/session/token/refresh'):
            time.sleep(0.02)
            payload = {'code': 0, 'data': {'access_token': 'new', 'refresh_token': 'rotated',
                                        'access_expires': '2099-01-01T00:00:00.123456789+00:00'}}
        if path.endswith('/file/url'):
            payload = {'code': 0, 'data': {'urls': [{'url': self.server.other_url + '/capture'}]}}
        if path.endswith('/capture'):
            payload = b'file contents'
        if path.endswith('/partial'):
            payload = b'partial'
            headers['Content-Length'] = '1000'
        if path.endswith('/range'):
            status, payload = 206, b'partial'
        if not isinstance(payload, bytes):
            payload = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header('Content-Length', headers.pop('Content-Length', str(len(payload))))
        for key, value in headers.items():
            self.send_header(key, value)
        self.end_headers()
        try:
            self.wfile.write(payload)
        except BrokenPipeError:
            pass


class Server(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self):
        super().__init__(('127.0.0.1', 0), Handler)
        self.lock = threading.Lock()
        self.counts, self.records = Counter(), []
        self.active = self.peak = 0
        self.url = f'http://127.0.0.1:{self.server_port}'
        self.other_url = self.url
        self.thread = threading.Thread(target=self.serve_forever, daemon=True)
        self.thread.start()

    def close(self):
        self.shutdown()
        self.server_close()
        self.thread.join()


class HTTPTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server, cls.external = Server(), Server()
        cls.server.other_url = cls.external.url

    @classmethod
    def tearDownClass(cls):
        cls.server.close()
        cls.external.close()

    def setUp(self):
        for server in (self.server, self.external):
            server.counts.clear()
            server.records.clear()
            server.peak = 0
        self.client = CloudreveV4(self.server.url, trust_env=False)
        self.addCleanup(self.client.close)

    def test_read_retry_and_exhaustion(self):
        self.assertEqual(self.client.request('get', '/retry'), {'ok': True})
        self.assertEqual(self.server.counts['/api/v4/retry'], 3)
        with self.assertRaises(HTTPError):
            self.client.request('get', '/always503')
        self.assertEqual(self.server.counts['/api/v4/always503'], 3)

    def test_writes_never_retry(self):
        for method in ('post', 'put', 'delete', 'patch'):
            with self.subTest(method=method), self.assertRaises(HTTPError):
                self.client.request(method, '/always503')
        self.assertEqual(self.server.counts['/api/v4/always503'], 4)

    def test_connection_loss_only_retries_reads(self):
        with patch('cloudreve.transport.time.sleep'):
            for method, expected in [('get', 3), ('post', 1)]:
                self.server.counts.clear()
                with self.assertRaises(ConnectionError):
                    self.client.request(method, '/drop')
                self.assertEqual(self.server.counts['/api/v4/drop'], expected)

    def test_read_timeout_budget(self):
        with patch('cloudreve.transport.time.sleep'):
            with self.assertRaises(ReadTimeout):
                self.client.request('get', '/slow', timeout=(1, 0.005))
        self.assertEqual(self.server.counts['/api/v4/slow'], 3)

    def test_business_error_and_protocol(self):
        with self.assertRaises(CloudreveError) as caught:
            self.client.request('get', '/business')
        self.assertEqual(caught.exception.code, 40076)
        self.assertEqual(caught.exception.message, 'version conflict')
        self.assertEqual(caught.exception.data, {'expected': 'old'})
        self.assertEqual(self.server.counts['/api/v4/business'], 1)
        for path in ('/malformed', '/invalid-json'):
            with self.assertRaises(CloudreveProtocolError):
                self.client.request('get', path)

    def test_no_implicit_auth_replay(self):
        with self.assertRaises(HTTPError):
            self.client.request('post', '/unauthorized')
        self.assertEqual(self.server.counts['/api/v4/unauthorized'], 1)

    def test_redirect_and_api_origin_blocked(self):
        with self.assertRaises(HTTPError):
            self.client.request('get', '/redirect')
        with self.assertRaises(ValueError):
            self.client.request('get', self.external.url + '/capture')
        self.assertEqual(len(self.external.records), 0)

    def test_parallel_thread_sessions_and_refresh_once(self):
        self.client.login('a', 'b')
        barrier = threading.Barrier(6)
        def work(_):
            barrier.wait()
            session = self.client.session
            self.client.request('get', '/parallel')
            return id(session)
        with ThreadPoolExecutor(6) as pool:
            ids = list(pool.map(work, range(6)))
        self.assertEqual(len(set(ids)), 6)
        self.assertGreaterEqual(self.server.peak, 2)
        self.assertEqual(self.server.counts['/api/v4/session/token/refresh'], 1)
        records = [r for r in self.server.records if r[1].endswith('/parallel')]
        self.assertTrue(all(r[2]['Authorization'] == 'Bearer new' for r in records))
        self.assertEqual(self.client.refresh_token, 'rotated')

    def test_failed_refresh_not_replayed_by_waiting_workers(self):
        self.client.login('a', 'b')
        with patch.object(self.client, '_send', side_effect=ReadTimeout('refresh result unknown')) as send:
            def read(_):
                try:
                    self.client.list()
                except ReadTimeout:
                    return True
            with ThreadPoolExecutor(4) as pool:
                self.assertTrue(all(pool.map(read, range(4))))
            self.assertEqual(send.call_count, 1)
        self.client.login('a', 'b')
        self.assertEqual(self.client.request('get', '/ok'), {'ok': True})

    def test_retry_can_be_disabled(self):
        self.client.read_retries = 0
        with self.assertRaises(HTTPError):
            self.client.request('get', '/retry')
        self.assertEqual(self.server.counts['/api/v4/retry'], 1)

    def test_download_does_not_leak_credentials(self):
        self.client.session.headers['Authorization'] = 'Bearer secret'
        self.client.session.headers['X-Secret'] = 'private'
        self.client.session.cookies.set('cloudreve-session', 'secret')
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory)/'out'
            self.client.download('/a', target)
            self.assertEqual(target.read_bytes(), b'file contents')
        headers = self.external.records[-1][2]
        for key in ('Authorization', 'Cookie', 'X-Secret'):
            self.assertNotIn(key, headers)

    def test_download_same_origin_guard(self):
        self.client.allow_external_downloads = False
        with tempfile.TemporaryDirectory() as directory, self.assertRaises(ValueError):
            self.client.download('/a', Path(directory)/'out')
        self.assertEqual(len(self.external.records), 0)

    def test_download_failure_preserves_existing_file(self):
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory)/'out'
            target.write_bytes(b'old')
            for path in ('/partial', '/redirect', '/range'):
                with self.subTest(path=path), self.assertRaises((HTTPError, ChunkedEncodingError)):
                    self.client._download(self.server.url + path, target)
                self.assertEqual(target.read_bytes(), b'old')
                self.assertEqual(list(Path(directory).iterdir()), [target])

    def test_download_retries_http_status(self):
        with tempfile.TemporaryDirectory() as directory:
            self.client._download(self.server.url + '/retry', Path(directory)/'out')
        self.assertEqual(self.server.counts['/retry'], 3)

    def test_remote_upload_isolates_credentials_and_handles_empty(self):
        self.client.session.headers['Authorization'] = 'Bearer cloudreve'
        self.client.session.cookies.set('cloudreve-session', 'secret')
        with tempfile.TemporaryDirectory() as directory:
            file = Path(directory)/'file'
            for data, chunk_size, count in [(b'', 0, 1), (b'abcd', 0, 1), (b'abcde', 2, 3)]:
                self.external.records.clear()
                file.write_bytes(data)
                self.client._upload_to_remote_direct(file, 'id', chunk_size,
                                                     [self.external.url+'/remote'], 'Bearer remote')
                self.assertEqual(len(self.external.records), count)
                self.assertEqual(b''.join(r[3] for r in self.external.records), data)
                for record in self.external.records:
                    self.assertEqual(record[2]['Authorization'], 'Bearer remote')
                    self.assertNotIn('Cookie', record[2])

    def test_onedrive_status_checked_before_callback(self):
        with tempfile.TemporaryDirectory() as directory:
            file = Path(directory)/'file'; file.write_bytes(b'abc')
            with self.assertRaises(HTTPError):
                self.client._upload_to_onedrive(file, 'id', 2,
                                                [self.external.url+'/always503'], 'callback')
        self.assertEqual(self.external.counts['/always503'], 1)
        self.assertEqual(len(self.server.records), 0)

    def test_v3_uses_same_error_and_retry_policy(self):
        with CloudreveV3(self.server.url, trust_env=False) as client:
            self.assertEqual(client.request('get', '/retry'), {'ok': True})
            with self.assertRaises(CloudreveError):
                client.request('get', '/business')

    def test_utf8_content_length(self):
        self.client.update_file_content('/a', '中文内容')
        self.assertEqual(self.server.records[-1][3], '中文内容'.encode())

    def test_close_is_final(self):
        self.client.close()
        with self.assertRaises(RuntimeError):
            self.client.list()


class UploadTests(unittest.TestCase):
    def setUp(self):
        self.client = CloudreveV4('https://cr.invalid')
        self.addCleanup(self.client.close)
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.file = Path(self.tmp.name)/'file'
        self.file.write_bytes(b'abcdefg')
        self.policy = {'type': 'oss', 'relay': True, 'id': 'policy'}
        self.client.list = Mock(return_value={'storage_policy': self.policy})
        self.client.get_info = Mock(return_value={'size': 7})

    def test_relay_variants(self):
        for data, chunk_size in [(b'', 0), (b'', 2), (b'abcdefg', 0), (b'abcdefg', 2)]:
            calls, bodies = [], []
            self.file.write_bytes(data)
            self.client.get_info.return_value = {'size': len(data)}
            def request(method, url, **kw):
                calls.append((method, url, kw))
                if method == 'put':
                    return {'session_id': 'session', 'chunk_size': chunk_size, 'storage_policy': self.policy}
                body = kw['data']
                bodies.append(body.read() if hasattr(body, 'read') else body)
            self.client.request = request
            self.client.upload(self.file, '/file', previous='old')
            self.assertEqual(b''.join(bodies), data)
            self.assertGreaterEqual(len(bodies), 1)
            self.assertEqual(calls[0][2]['json']['previous'], 'old')
            self.assertEqual(calls[0][2]['json']['entity_type'], 'version')

    def test_upload_failure_and_cleanup_failure_preserve_original(self):
        error, cleanup = ReadTimeout('ambiguous upload'), CloudreveError(50001, 'cancel failed')
        self.client.request = Mock(side_effect=[{'session_id': 'id', 'chunk_size': 0}, error, cleanup])
        with self.assertRaises(ReadTimeout) as caught:
            self.client.upload(self.file, '/file')
        self.assertIs(caught.exception, error)
        self.assertIs(error.cleanup_error, cleanup)
        self.assertEqual(error.upload_session_id, 'id')
        self.assertEqual(self.client.request.call_count, 3)

    def test_cancel_on_keyboard_interrupt(self):
        self.client.request = Mock(side_effect=[{'session_id': 'id', 'chunk_size': 0}, KeyboardInterrupt(), None])
        with self.assertRaises(KeyboardInterrupt):
            self.client.upload(self.file, '/file')
        self.assertEqual(self.client.request.call_args.args[0], 'delete')

    def test_session_creation_timeout_cannot_cancel_unknown_id(self):
        self.client.request = Mock(side_effect=ReadTimeout())
        with self.assertRaises(ReadTimeout):
            self.client.upload(self.file, '/file')
        self.assertEqual(self.client.request.call_count, 1)

    def test_size_mismatch_cancels(self):
        self.client.get_info.return_value = {'size': 8}
        self.client.request = Mock(side_effect=[{'session_id': 'id', 'chunk_size': 0}, None, None])
        with self.assertRaises(ValueError):
            self.client.upload(self.file, '/file')
        self.assertEqual(self.client.request.call_args.args[0], 'delete')

    def test_unsupported_policy_before_session(self):
        self.policy['relay'] = False
        self.client.request = Mock()
        with self.assertRaises(ValueError):
            self.client.upload(self.file, '/file')
        self.client.request.assert_not_called()

    def test_delete_contract_and_aliases(self):
        self.client.request = Mock()
        self.client.delete('/a')
        self.assertFalse(self.client.request.call_args.kwargs['json']['skip_soft_delete'])
        self.client.remove('/a', trash_bin=False)
        self.assertTrue(self.client.request.call_args.kwargs['json']['skip_soft_delete'])

    def test_uri_paths(self):
        self.assertEqual(revise_file_path('/中文 #?%.txt'), 'cloudreve://my/%E4%B8%AD%E6%96%87%20%23%3F%25.txt')
        self.assertEqual(revise_file_path('cloudreve://my/a%20b'), 'cloudreve://my/a%20b')
        self.assertEqual(revise_file_path('/'), 'cloudreve://my/')
        for value in ('', 'cloudreve:///a', 'cloudreve://my/a#b'):
            with self.assertRaises(ValueError):
                revise_file_path(value)

    def test_text_update_version_precondition(self):
        self.client.request = Mock()
        self.client.update_file_content('/a', '新内容', previous='version')
        self.assertEqual(self.client.request.call_args.kwargs['params']['previous'], 'version')
        self.assertEqual(self.client.request.call_args.kwargs['data'], '新内容'.encode())
        with self.assertRaises(ValueError):
            self.client.update_file_content('/a', 'x', previous='')

    def test_search_uri_preserved(self):
        self.assertEqual(revise_file_path('cloudreve://my/?name=abc'), 'cloudreve://my/?name=abc')

    def test_folder_boundary_cursor_and_sort_direction(self):
        for last_type, mixed, start_with_file, expected in [(1, False, True, False),
                (1, False, False, False), (0, False, True, True), (1, True, True, True)]:
            token = base64.b64encode(json.dumps({'id': 'last', 'start_with_file': start_with_file}).encode()).decode()
            client = CloudreveV4('https://cr.invalid')
            self.addCleanup(client.close)
            client.request = Mock(return_value={'files': [{'id': 'last', 'type': last_type}],
                    'mixed_type': mixed, 'pagination': {'is_cursor': True, 'next_token': token}})
            result = client.list('/', order='desc')
            cursor = json.loads(base64.b64decode(result['pagination']['next_token']))
            self.assertEqual(cursor['start_with_file'], expected)
            self.assertEqual(client.request.call_args.kwargs['params']['order_direction'], 'desc')

    def test_token_fraction_compatibility(self):
        for fraction in ('1', '12', '1234', '12345', '123456789'):
            self.client._set_token({'access_token': 'a', 'refresh_token': 'r',
                                   'access_expires': '2099-01-01T00:00:00.'+fraction+'Z'})
            self.assertGreater(self.client._access_expires, 0)

    def test_v3_local_streaming_and_empty(self):
        with CloudreveV3('https://cr.invalid') as client:
            for data, size in [(b'', 0), (b'abcde', 0), (b'abcde', 2)]:
                self.file.write_bytes(data)
                bodies = []
                def request(method, url, **kw):
                    body = kw['data']
                    bodies.append(body.read() if hasattr(body, 'read') else body)
                client.request = request
                client.upload_to_local(self.file, 'id', size, None)
                self.assertGreaterEqual(len(bodies), 1)
                self.assertEqual(b''.join(bodies), data)

    def test_invalid_config(self):
        for kw in ({'read_retries': -1}, {'timeout': None}, {'timeout': 0}, {'timeout': float('inf')}, {'timeout': (10,)}):
            with self.assertRaises(ValueError):
                CloudreveV4(**kw)


if __name__ == '__main__':
    unittest.main()

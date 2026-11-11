"""Shared HTTP policy for V3/V4; each calling thread owns its connection pool."""
import random
import time
from email.utils import parsedate_to_datetime
from threading import RLock, local
from urllib.parse import urlsplit

from requests import ConnectionError, HTTPError, Session, Timeout
from requests.exceptions import SSLError
from requests.structures import CaseInsensitiveDict


class CloudreveError(Exception):
    def __init__(self, code, message='', data=None):
        self.code, self.message, self.data = code, message, data
        super().__init__(f'Cloudreve error code={code}' + (f': {message}' if message else ''))


class CloudreveProtocolError(ValueError):
    """A successful HTTP response did not contain a Cloudreve envelope."""


def origin(url):
    parsed = urlsplit(url)
    if parsed.scheme not in ('http', 'https') or not parsed.hostname or parsed.username or parsed.password:
        raise ValueError('Expected an HTTP(S) URL without credentials')
    return parsed.scheme, parsed.hostname.lower(), parsed.port or (443 if parsed.scheme == 'https' else 80)


def response_data(response):
    if not 200 <= response.status_code < 300:
        raise HTTPError(f'Cloudreve HTTP {response.status_code}', response=response)
    try:
        body = response.json()
    except ValueError as error:
        raise CloudreveProtocolError('Cloudreve returned invalid JSON') from error
    return envelope_data(body)


def envelope_data(body):
    if not isinstance(body, dict) or type(body.get('code')) is not int:
        raise CloudreveProtocolError('Cloudreve response requires an integer code')
    if body['code'] != 0:
        raise CloudreveError(body['code'], body.get('msg', ''), body.get('data'))
    return body.get('data')


class Client:
    api_version = ''

    def __init__(self, base_url='http://127.0.0.1:5212', proxy=None, verify=True,
                 headers=None, cloudreve_session=None, *, timeout=(10, 120),
                 read_retries=2, trust_env=True, allow_external_downloads=True):
        origin(base_url)
        if urlsplit(base_url).query or urlsplit(base_url).fragment:
            raise ValueError('Base URL must not contain a query or fragment')
        if type(read_retries) is not int or read_retries < 0:
            raise ValueError('read_retries must be a nonnegative integer')
        values = timeout if isinstance(timeout, tuple) else (timeout,)
        if (isinstance(timeout, tuple) and len(timeout) != 2) or any(not isinstance(v, (int, float)) or v <= 0 for v in values):
            raise ValueError('timeout must contain positive finite seconds')
        if any(v == float('inf') or v != v for v in values):
            raise ValueError('timeout must contain positive finite seconds')
        self.base_url = base_url.rstrip('/')
        if not self.base_url.endswith('/api/' + self.api_version):
            self.base_url += '/api/' + self.api_version
        self.timeout, self.read_retries = timeout, read_retries
        self.allow_external_downloads = allow_external_downloads
        self._lock, self._local = RLock(), local()
        self._closed = False
        self._template = Session()
        self._template.verify, self._template.trust_env = verify, trust_env
        if isinstance(proxy, str):
            proxy = {'http': proxy, 'https': proxy}
        if proxy is not None:
            self._template.proxies.update(proxy)
        if headers is not None:
            self._template.headers.update(headers)
        if cloudreve_session is not None:
            self._template.cookies.set('cloudreve-session', cloudreve_session)
        self._local.session = self._template
        self._sessions = [self._template]

    @property
    def session(self):
        with self._lock:
            if self._closed:
                raise RuntimeError('Cloudreve client is closed')
            if not hasattr(self._local, 'session'):
                session = Session()
                for name in ('headers', 'cookies', 'proxies', 'params'):
                    setattr(session, name, getattr(self._template, name).copy())
                for name in ('verify', 'cert', 'trust_env', 'auth'):
                    setattr(session, name, getattr(self._template, name))
                self._local.session = session
                self._sessions.append(session)
            if 'Authorization' in self._template.headers:
                self._local.session.headers['Authorization'] = self._template.headers['Authorization']
            return self._local.session

    def close(self):
        """Close all thread pools after workers have joined; does not revoke tokens."""
        with self._lock:
            for session in self._sessions:
                session.close()
            self._sessions.clear()
            self._closed = True

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def _external_session(self):
        # Signed URLs must not inherit CR cookies, headers, auth or netrc credentials.
        with self._lock:
            if not hasattr(self._local, 'external'):
                session = Session()
                session.trust_env = False
                session.verify = self.session.verify
                session.proxies.update(self.session.proxies)
                self._local.external = session
                self._sessions.append(session)
            return self._local.external

    def _send(self, session, method, target, **kwargs):
        kwargs.setdefault('timeout', self.timeout)
        kwargs.setdefault('allow_redirects', False)
        attempts = self.read_retries if method.upper() in ('GET', 'HEAD') else 0
        for attempt in range(attempts + 1):
            delay = min(0.25 * 2 ** attempt + random.uniform(0, 0.1), 30)
            try:
                response = session.request(method, target, **kwargs)
            except SSLError:
                raise
            except (ConnectionError, Timeout):
                if attempt == attempts:
                    raise
            else:
                if response.status_code not in (429, 502, 503, 504) or attempt == attempts:
                    return response
                retry_after = response.headers.get('Retry-After')
                if retry_after:
                    try:
                        delay = float(retry_after)
                    except ValueError:
                        try:
                            delay = parsedate_to_datetime(retry_after).timestamp() - time.time()
                        except (ValueError, TypeError, OverflowError):
                            pass
                response.close()
            time.sleep(max(0, min(delay, 30)))

    def request(self, method, url, **kwargs):
        target = url if urlsplit(url).scheme else self.base_url + url
        if origin(target) != origin(self.base_url):
            raise ValueError('API requests must use the configured Cloudreve origin')
        # Configuration/login occurs before worker startup; refresh updates auth atomically.
        with self._lock:
            headers = CaseInsensitiveDict(kwargs.pop('headers', {}) or {})
            if 'Authorization' in self._template.headers:
                headers.setdefault('Authorization', self._template.headers['Authorization'])
        with self._send(self.session, method, target, headers=headers, **kwargs) as response:
            return response_data(response)

    def _download(self, url, save_path):
        from .utils import save_response
        same_origin = origin(url) == origin(self.base_url)
        if not same_origin and not self.allow_external_downloads:
            raise ValueError('External download origin is disabled')
        # Download URLs are signed; even same-origin transfers need no login credentials.
        session = self._external_session()
        with self._send(session, 'get', url, stream=True) as response:
            save_response(response, save_path)

    def _onedrive_chunks(self, local_file, chunk_size, url):
        origin(url)
        size = local_file.stat().st_size
        if type(chunk_size) is not int or chunk_size <= 0 or size == 0:
            raise ValueError('OneDrive requires positive chunk size and nonempty file')
        with local_file.open('rb') as file:
            for start in range(0, size, chunk_size):
                end = min(start + chunk_size, size) - 1
                with self._send(self._external_session(), 'put', url,
                                headers={'Content-Type': 'application/octet-stream',
                                         'Content-Range': f'bytes {start}-{end}/{size}'},
                                data=file.read(chunk_size)) as response:
                    if not 200 <= response.status_code < 300:
                        raise HTTPError(f'OneDrive HTTP {response.status_code}', response=response)

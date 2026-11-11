"""Asyncio V4 client. Share one instance within one event loop."""
import asyncio
import os
import random
import ssl
import time
from email.utils import parsedate_to_datetime
from functools import wraps
from mimetypes import guess_type
from pathlib import Path
from tempfile import NamedTemporaryFile
from urllib.parse import urljoin, urlsplit
from typing import List, Literal, Union

import httpx

from ._async_queue import OperationQueue, complete as _complete
from .transport import CloudreveProtocolError, envelope_data, origin
from .v4 import check_source_unchanged, fix_list_cursor, revise_file_path, token_values, uris_to_list


def _operation(method):
    @wraps(method)
    async def queued(self, *args, **kwargs):
        return await self._operations.submit(lambda: method(self, *args, **kwargs))
    return queued


async def _io(function, *args):
    return await _complete(asyncio.get_running_loop().run_in_executor(None, function, *args))


def _response_data(response):
    response.raise_for_status()
    try:
        body = response.json()
    except ValueError as error:
        raise CloudreveProtocolError('Cloudreve returned invalid JSON') from error
    return envelope_data(body)


class AsyncCloudreveV4:
    api_version = 'v4'

    def __init__(self, base_url='http://127.0.0.1:5212', proxy=None, verify=True,
                 headers=None, cloudreve_session=None, *, timeout=(10, 120),
                 read_retries=2, trust_env=True, allow_external_downloads=True,
                 max_concurrency=4):
        origin(base_url)
        if urlsplit(base_url).query or urlsplit(base_url).fragment:
            raise ValueError('Base URL must not contain a query or fragment')
        if type(read_retries) is not int or read_retries < 0:
            raise ValueError('read_retries must be a nonnegative integer')
        values = timeout if isinstance(timeout, tuple) else (timeout,)
        if ((isinstance(timeout, tuple) and len(timeout) != 2)
                or any(not isinstance(v, (int, float)) or not 0 < v < float('inf') for v in values)):
            raise ValueError('timeout must contain positive finite seconds')
        connect, read = timeout if isinstance(timeout, tuple) else (timeout, timeout)
        self.timeout = httpx.Timeout(read, connect=connect, pool=connect)
        self.base_url = base_url.rstrip('/')
        if not self.base_url.endswith('/api/v4'):
            self.base_url += '/api/v4'
        self.read_retries = read_retries
        self.allow_external_downloads = allow_external_downloads
        self._operations = OperationQueue(max_concurrency)
        self._auth_condition = asyncio.Condition()
        self._auth_pending = False
        self._active_requests = 0
        self._close_task = None
        self.refresh_token = self._access_expires = self._refresh_error = None
        self._closed = False
        self._token_generation = 0
        self._verify, self._proxy = verify, proxy
        self._external = None
        self.session = self._new_session(trust_env, headers)
        if cloudreve_session is not None:
            self.session.cookies.set('cloudreve-session', cloudreve_session)

    def _new_session(self, trust_env, headers=None):
        options = dict(verify=self._verify, trust_env=trust_env, timeout=self.timeout,
                       follow_redirects=False, headers=headers)
        if isinstance(self._proxy, dict):
            options['mounts'] = {
                scheme.rstrip(':/') + '://': httpx.AsyncHTTPTransport(
                    proxy=proxy, verify=self._verify, trust_env=trust_env)
                for scheme, proxy in self._proxy.items()
            }
        else:
            options['proxy'] = self._proxy
        return httpx.AsyncClient(**options)

    async def aclose(self):
        """Stop admission, cancel queued/running operations, drain cleanup, then close pools."""
        if self._close_task is None:
            self._operations.closed = True
            self._close_task = asyncio.create_task(self._close())
        await _complete(self._close_task)

    async def _close(self):
        await self._operations.aclose()
        self._closed = True
        try:
            await self.session.aclose()
        finally:
            if self._external is not None:
                await self._external.aclose()

    async def __aenter__(self):
        if self._close_task is not None:
            raise RuntimeError('Cloudreve client is closed')
        return self

    async def __aexit__(self, *args):
        await self.aclose()

    def _external_session(self):
        if self._closed:
            raise RuntimeError('Cloudreve client is closed')
        if self._external is None:
            # Signed transfers never inherit login headers, cookies or ambient proxies.
            self._external = self._new_session(False)
        return self._external

    async def _send(self, session, method, target, *, stream=False, **kwargs):
        if self._closed:
            raise RuntimeError('Cloudreve client is closed')
        if kwargs.pop('follow_redirects', False):
            raise ValueError('Redirects are disabled')
        if isinstance(kwargs.get('params'), dict):
            kwargs['params'] = {k: v for k, v in kwargs['params'].items() if v is not None}
        attempts = self.read_retries if method.upper() in ('GET', 'HEAD') else 0
        for attempt in range(attempts + 1):
            delay = min(0.25 * 2 ** min(attempt, 7) + random.uniform(0, 0.1), 30)
            request = session.build_request(method, target, **kwargs)
            if session is self._external:
                request.headers.pop('Cookie', None)
            try:
                response = await session.send(request, stream=stream, follow_redirects=False)
            except (httpx.NetworkError, httpx.TimeoutException, httpx.RemoteProtocolError) as error:
                cause = error
                while cause is not None and not isinstance(cause, ssl.SSLError):
                    cause = cause.__cause__ or cause.__context__
                if cause is not None or attempt == attempts:
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
                await _complete(response.aclose())
            await asyncio.sleep(max(0, min(delay, 30)))

    def _set_token(self, token):
        access, refresh, deadline = token_values(token)
        self.session.headers['Authorization'] = 'Bearer ' + access
        self._token_generation += 1
        self.refresh_token, self._access_expires = refresh, deadline
        self._refresh_error = None

    @_operation
    async def login(self, email, password):
        async def authenticate():
            result = await self._request('post', '/session/token',
                                         json={'email': email, 'password': password})
            self._set_token(result['token'])
            self.user = result['user']
        async with self._auth_condition:
            await self._auth_condition.wait_for(lambda: not self._auth_pending)
            await self._authenticate(authenticate)

    async def _authenticate(self, authenticate):
        # Caller owns the condition; waiting drains requests, not whole multi-step operations.
        self._auth_pending = True
        try:
            await self._auth_condition.wait_for(lambda: self._active_requests == 0)
            return await authenticate()
        finally:
            self._auth_pending = False
            self._auth_condition.notify_all()

    async def _refresh(self):
        if not self.refresh_token:
            raise ValueError('No refresh token; log in first')
        if self._refresh_error is not None:
            raise self._refresh_error
        try:
            result = await self._request('post', '/session/token/refresh',
                                         json={'refresh_token': self.refresh_token})
            self._set_token(result)
        except BaseException as error:
            # A lost/cancelled response may already have rotated the refresh token.
            self._refresh_error = (RuntimeError('Token refresh cancelled; log in again')
                                   if isinstance(error, asyncio.CancelledError) else error)
            raise

    async def refresh_access_token(self):
        generation = self._token_generation
        async def refresh():
            async with self._auth_condition:
                await self._auth_condition.wait_for(lambda: not self._auth_pending)
                if generation == self._token_generation:
                    await self._authenticate(self._refresh)
        return await self._operations.submit(refresh)

    @_operation
    async def request(self, method, url, **kwargs):
        return await self._api_request(method, url, **kwargs)

    async def _api_request(self, method, url, *, skip_refresh=False, **kwargs):
        async with self._auth_condition:
            await self._auth_condition.wait_for(lambda: not self._auth_pending)
            if (not skip_refresh and self.refresh_token and self._access_expires is not None
                    and time.time() >= self._access_expires - 30):
                await self._authenticate(self._refresh)
            self._active_requests += 1
        try:
            return await self._request(method, url, **kwargs)
        finally:
            await _complete(self._release_request())

    async def _release_request(self):
        async with self._auth_condition:
            self._active_requests -= 1
            self._auth_condition.notify_all()

    async def _request(self, method, url, **kwargs):
        target = url if urlsplit(url).scheme else self.base_url + url
        if origin(target) != origin(self.base_url):
            raise ValueError('API requests must use the configured Cloudreve origin')
        response = await self._send(self.session, method, target, **kwargs)
        try:
            return _response_data(response)
        finally:
            await _complete(response.aclose())

    async def _download(self, url, save_path):
        if origin(url) != origin(self.base_url) and not self.allow_external_downloads:
            raise ValueError('External download origin is disabled')
        response = await self._send(self._external_session(), 'get', url, stream=True)
        temporary = None
        try:
            if response.status_code != 200:
                raise httpx.HTTPStatusError('Download requires HTTP 200',
                                            request=response.request, response=response)
            destination = Path(save_path)
            with NamedTemporaryFile(dir=destination.parent, delete=False) as file:
                temporary = Path(file.name)
                size = 0
                async for chunk in response.aiter_bytes(64 * 1024):
                    await _io(file.write, chunk)
                    size += len(chunk)
            expected = response.headers.get('Content-Length')
            if expected is not None and not response.headers.get('Content-Encoding') and size != int(expected):
                raise httpx.RemoteProtocolError('Downloaded size differs from Content-Length')
            os.replace(temporary, destination)
        finally:
            if temporary is not None and temporary.exists():
                temporary.unlink()
            await _complete(response.aclose())

    @_operation
    async def list(self,
             uri='/',
             page=0,
             page_size=100,
             order_by: Literal['name', 'size', 'created_at',
                               'updated_at'] = 'created_at',
             order: Literal['asc', 'desc'] = 'asc',
             next_page_token=None):
        '''
        列出目录下的文件
        @param path: 目录路径
        @return: 文件列表
            - parent: 目录文件夹信息
            - files: 文件列表
            - storage_policy: 文件存储策略
        '''
        result = await self.request('get',
                            '/file',
                            params={
                                'uri': revise_file_path(uri),
                                'page': page,
                                'page_size': page_size,
                                'order_by': order_by,
                                'order_direction': order,
                                'next_page_token': next_page_token
                            })

        return fix_list_cursor(result)

    @_operation
    async def get_info(self, file_uri):
        '''
        获取文件信息
        @param file_uri: 文件URI
        @return: 文件信息
        '''
        return await self.request('get',
                            '/file/info',
                            params={
                                'uri': revise_file_path(file_uri),
                                'extended': True
                            })

    get_property = get_info

    @_operation
    async def get_download_url(self, file_uri) -> str:
        '''
        获取文件临时下载链接
        @param file_uri: 文件URI
        @return: 下载链接
        '''
        r = await self.request('post',
                         '/file/url',
                         json={
                             'download': True,
                             'uris': [revise_file_path(file_uri)],
                         })
        url = r['urls'][0]['url']
        return urljoin(self.base_url + '/', url)

    @_operation
    async def download(self, file_uri, save_path):
        '''
        下载文件至本地
        @param file_uri: 文件URI
        @param save_path: 保存路径
        '''
        await self._download(await self.get_download_url(file_uri), save_path)

    @_operation
    async def get_source_url(self, uris):
        '''
        获取文件直链
        @param uris(str|list): 文件URL，可传递列表
        @return: 单个 URI 返回链接字符串；列表返回服务端的无序链接信息列表。
        '''
        is_list = type(uris) is list
        r = await self.request('put',
                         '/file/source',
                         json={'uris': uris_to_list(uris)})
        if not is_list:
            return r[0]['link']
        return r

    @_operation
    async def get_share_url(self,
                      file_uri,
                      downloads=None,
                      expire=None,
                      password=None,
                      share_view=None,
                      show_readme=None) -> str:
        '''
        获取文件分享链接
        @param file_uri: 文件或文件夹URI
        @param downloads(int|None): 下载次数限制（默认不限制）
        @param expire(int|None): 过期时间（自现在开始的秒数）
        @param password(str|None): 密码
        @param share_view(bool|None): 是否允许预览分享的文件夹（仅对文件夹有效，默认False）
        @param show_readme(bool|None): 是否显示README文件（仅对文件夹有效，默认False）
        @return: 分享链接
        '''
        r = await self.request('put',
                         '/share',
                         json={
                             'uri': revise_file_path(file_uri),
                             'downloads': downloads,
                             'expire': expire,
                             'password': password,
                             'is_private': True if password else False,
                             'share_view': share_view,
                             'show_readme': show_readme,
                         })
        return r

    async def _create(self,
                uri,
                type: Literal['folder', 'file'],
                err_on_conflict=False):
        '''
        创建文件夹或文件
        @param uri: 目标URI
        @param type: 创建类型，folder或file
        @param err_on_conflict: 若目标已存在，是否报错
        '''
        return await self.request('post',
                            '/file/create',
                            json={
                                'uri': revise_file_path(uri),
                                'type': type,
                                'err_on_conflict': err_on_conflict,
                            })

    @_operation
    async def create_file(self, uri, err_on_conflict=False):
        '''
        创建文件
        @param uri: 目标URI
        @param err_on_conflict: 若目标已存在，是否报错
        '''
        return await self._create(uri, 'file', err_on_conflict)

    @_operation
    async def create_folder(self, uri, err_on_conflict=False):
        '''
        创建文件夹
        @param uri: 目标URI
        @param err_on_conflict: 若目标已存在，是否报错
        '''
        return await self._create(uri, 'folder', err_on_conflict)

    create_dir = create_directory = create_folder

    @_operation
    async def update_file_content(self, file_uri, content, previous=None):
        '''
        更新文本文件内容
        @param file_uri: 文件URI
        @param content: 新内容
        @param previous: 可选的当前 primary_entity，用于防止覆盖并发更新
        '''
        params = {'uri': revise_file_path(file_uri)}
        if previous is not None:
            if not isinstance(previous, str) or not previous:
                raise ValueError('Previous version must be a nonempty entity ID')
            params['previous'] = previous
        return await self.request('put', '/file/content', params=params,
                            content=content.encode('utf-8') if isinstance(content, str) else content)

    @_operation
    async def delete(self,
               uris: Union[str, List[str]],
               unlink=False,
               trash_bin=True):
        '''
        删除文件或文件夹
        @param uris(str|list): 文件URI
        @param unlink: 仅解除链接
        @param trash_bin: 默认移入回收站；False 永久删除
        '''
        return await self.request('delete',
                            '/file',
                            json={
                                'uris': uris_to_list(uris),
                                'unlink': unlink,
                                'skip_soft_delete': not trash_bin
                            })

    remove = delete

    @_operation
    async def rename(self, uri: str, new_name: str):
        '''
        重命名文件或文件夹
        @param uri: 文件URI
        @param new_name: 新名称
        '''
        return await self.request('post',
                            '/file/rename',
                            json={
                                'uri': revise_file_path(uri),
                                'new_name': new_name
                            })

    @_operation
    async def copy_or_move(self, uris, dst, copy=False):
        '''
        通过来源文件夹和文件ID复制文件或文件夹
        @param uris: 源文件或文件夹URI列表
        @param dst: 目标目录
        @param copy: 是否为复制操作，默认为False（移动操作）
        '''
        return await self.request('post',
                            '/file/move',
                            json={
                                'uris': uris_to_list(uris),
                                'dst': revise_file_path(dst),
                                'copy': copy,
                            })

    @_operation
    async def copy(self, uris: Union[str, List[str]], dst: str):
        '''
        通过路径复制文件或文件夹
        @param uris: 源文件或文件夹URI或URI列表
        @param dst: 目标目录
        '''
        return await self.copy_or_move(uris, dst, copy=True)

    @_operation
    async def move(self, uris: Union[str, List[str]], dst: str):
        '''
        通过路径移动文件或文件夹
        @param uris: 源文件或文件夹URI或URI列表
        @param dst: 目标目录
        '''
        return await self.copy_or_move(uris, dst, copy=False)

    async def _upload_chunks(self, local_file, session_id, chunk_size, *,
                             mode='local', upload_urls=None, credential=None, **kwargs):
        if type(chunk_size) is not int or chunk_size < 0:
            raise ValueError('Invalid upload chunk size')
        size = local_file.stat().st_size
        if mode == 'onedrive' and (chunk_size == 0 or size == 0):
            raise ValueError('OneDrive requires positive chunk size and nonempty file')
        if mode != 'local':
            origin(upload_urls[0])
        count = max(1, (size + chunk_size - 1) // chunk_size) if chunk_size else 1
        with local_file.open('rb') as file:
            for index in range(count):
                length = min(chunk_size, size - index * chunk_size) if chunk_size else size

                async def content():
                    remaining = length
                    while remaining:
                        block = await _io(file.read, min(64 * 1024, remaining))
                        if not block:
                            raise ValueError('Source file was truncated during upload')
                        remaining -= len(block)
                        yield block

                headers = {'Content-Length': str(length), 'Content-Type': 'application/octet-stream'}
                body = content()
                try:
                    if mode == 'local':
                        await self.request('post', f'/file/upload/{session_id}/{index}',
                                           headers=headers, content=body)
                    else:
                        options = {}
                        if mode == 'remote':
                            headers['Authorization'] = credential
                            options['params'] = {'chunk': index}
                        else:
                            start = index * chunk_size
                            headers['Content-Range'] = f'bytes {start}-{start + length - 1}/{size}'
                        response = await self._send(self._external_session(),
                                                    'post' if mode == 'remote' else 'put',
                                                    upload_urls[0], headers=headers, content=body, **options)
                        try:
                            if mode == 'remote':
                                _response_data(response)
                            else:
                                response.raise_for_status()
                        finally:
                            await _complete(response.aclose())
                finally:
                    await _complete(body.aclose())

    @_operation
    async def upload(self, local_file_path, uri, previous=None):
        local_file = Path(local_file_path)
        if not local_file.is_file():
            raise FileNotFoundError(f'{local_file_path} is not a file')
        uri = revise_file_path(uri)
        if not urlsplit(uri).path.strip('/'):
            raise ValueError('Upload destination must include a file name')
        policy = (await self.list(uri[:uri.rfind('/')]))['storage_policy']
        policy_type = policy['type']
        if not policy.get('relay') and policy_type not in ('local', 'remote', 'onedrive'):
            raise ValueError(f'存储策略 {policy_type} 需要启用中转上传')
        if previous is not None and (not isinstance(previous, str) or not previous):
            raise ValueError('Previous version must be a nonempty entity ID')
        stat = local_file.stat()
        size = stat.st_size
        if policy_type == 'onedrive' and not policy.get('relay') and size == 0:
            raise ValueError('OneDrive direct upload does not support empty files')
        body = dict(uri=uri, size=size, last_modified=int(stat.st_mtime * 1000),
                    policy_id=policy['id'], mime_type=guess_type(local_file.name)[0])
        if previous is not None:
            body.update(previous=previous, entity_type='version')
        session = await self.request('put', '/file/upload', json=body)
        try:
            policy = session.get('storage_policy', policy)
            mode = policy['type']
            if policy.get('relay') or mode == 'local' or (mode == 'remote' and not session.get('upload_urls')):
                mode = 'local'
            if mode not in ('local', 'remote', 'onedrive'):
                raise ValueError(f'存储策略 {mode} 暂时不受支持')
            await self._upload_chunks(local_file, mode=mode, **session)
            check_source_unchanged(local_file, stat)
            if mode == 'onedrive':
                await self.request('post', f"/callback/onedrive/{session['session_id']}/{session['callback_secret']}")
            if (await self.get_info(uri))['size'] != size:
                raise ValueError('Uploaded size differs from source')
        except BaseException as error:
            error.upload_session_id = session.get('session_id')
            error.upload_uri = uri
            try:
                # Cleanup bypasses a failed refresh and is drained before cancellation returns.
                await _complete(
                    self._api_request('delete', '/file/upload', skip_refresh=True,
                                      json={'id': session['session_id'], 'uri': uri}),
                    propagate_cancel=not isinstance(error, asyncio.CancelledError))
            except BaseException as cleanup_error:
                error.cleanup_error = cleanup_error
                raise error from cleanup_error
            raise

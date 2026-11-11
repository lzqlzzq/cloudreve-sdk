import base64
import json
import re
from datetime import datetime, timezone
from mimetypes import guess_type
from pathlib import Path
from typing import List, Literal, Union
from urllib.parse import quote, urljoin, urlsplit

from .transport import Client, CloudreveError, origin, response_data


def revise_file_path(file_path: str) -> str:
    if not isinstance(file_path, str) or not file_path:
        raise ValueError('File path must be a nonempty string')
    if not file_path.startswith('cloudreve://'):
        file_path = 'cloudreve://my/' + quote(file_path.lstrip('/'), safe='/')
    parsed = urlsplit(file_path)
    if not parsed.netloc or parsed.username or parsed.password or parsed.fragment:
        raise ValueError('Cloudreve URI must have an authority and no credentials or fragment')
    return parsed._replace(path=parsed.path.rstrip('/') or '/').geturl()


def uris_to_list(uris: Union[str, List[str]]) -> List[str]:
    _items = uris if type(uris) is list else [uris]
    return [revise_file_path(i) for i in _items]


def check_source_unchanged(path, before):
    after = path.stat()
    if any(getattr(before, field) != getattr(after, field)
           for field in ('st_dev', 'st_ino', 'st_size', 'st_mtime_ns')):
        raise ValueError('Source file changed during upload; verify the remote result')


def token_values(token):
    access, refresh = token['access_token'], token['refresh_token']
    expires = token.get('access_expires')
    # Go emits nanoseconds; Python 3.8–3.10 accepts only microseconds.
    normalized = re.sub(r'\.(\d+)', lambda m: '.' + m[1][:6].ljust(6, '0'), expires).replace('Z', '+00:00') if expires else None
    deadline = datetime.fromisoformat(normalized).timestamp() if normalized else None
    if not isinstance(access, str) or not access or not isinstance(refresh, str) or not refresh:
        raise ValueError('Invalid access or refresh token')
    return access, refresh, deadline


def fix_list_cursor(result):
    pagination = result.get('pagination', {})
    token = pagination.get('next_token')
    files = result.get('files', [])
    if token and pagination.get('is_cursor') and not result.get('mixed_type') and files and files[-1]['type'] == 1:
        # shortcut: V4 folder-boundary cursors skip older files; remove when upstream fixes its flag.
        try:
            cursor = json.loads(base64.b64decode(token, validate=True))
        except (ValueError, UnicodeError):
            cursor = None
        if isinstance(cursor, dict) and cursor.get('start_with_file') and cursor.get('id') == files[-1]['id']:
            cursor['start_with_file'] = False
            pagination['next_token'] = base64.b64encode(json.dumps(cursor).encode()).decode()
    return result


class CloudreveV4(Client):
    api_version = "v4"

    refresh_token = None
    _access_expires = None
    _refresh_error = None

    def _set_token(self, token):
        access, refresh, deadline = token_values(token)
        self._template.headers['Authorization'] = 'Bearer ' + access
        self.refresh_token, self._access_expires = refresh, deadline
        self._refresh_error = None

    def login(self, email, password):
        """Log in before starting workers. Credentials are not retained by the SDK."""
        with self._lock:
            result = super().request('post', '/session/token',
                                     json={'email': email, 'password': password})
            self._set_token(result['token'])
            self.user = result['user']

    def refresh_access_token(self):
        """Refresh once, without retrying a possibly rotated refresh token."""
        with self._lock:
            if not self.refresh_token:
                raise ValueError('No refresh token; log in first')
            try:
                result = super().request('post', '/session/token/refresh',
                                         json={'refresh_token': self.refresh_token})
                self._set_token(result)
            except Exception as error:
                # A lost response may already have rotated the token; workers must not replay it.
                self._refresh_error = error
                raise

    def request(self, method, url, **kwargs):
        with self._lock:
            if (not url.startswith('/session/') and self.refresh_token
                    and self._access_expires is not None
                    and datetime.now(timezone.utc).timestamp() >= self._access_expires - 30):
                if self._refresh_error is not None:
                    raise self._refresh_error
                self.refresh_access_token()
        return super().request(method, url, **kwargs)

    def list(self,
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
        result = self.request('get',
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

    def get_info(self, file_uri):
        '''
        获取文件信息
        @param file_uri: 文件URI
        @return: 文件信息
        '''
        return self.request('get',
                            '/file/info',
                            params={
                                'uri': revise_file_path(file_uri),
                                'extended': True
                            })

    get_property = get_info

    def get_download_url(self, file_uri) -> str:
        '''
        获取文件临时下载链接
        @param file_uri: 文件URI
        @return: 下载链接
        '''
        r = self.request('post',
                         '/file/url',
                         json={
                             'download': True,
                             'uris': [revise_file_path(file_uri)],
                         })
        url = r['urls'][0]['url']
        return urljoin(self.base_url + '/', url)

    def download(self, file_uri, save_path):
        '''
        下载文件至本地
        @param file_uri: 文件URI
        @param save_path: 保存路径
        '''
        self._download(self.get_download_url(file_uri), save_path)

    def get_source_url(self, uris):
        '''
        获取文件直链
        @param uris(str|list): 文件URL，可传递列表
        @return: 单个 URI 返回链接字符串；列表返回服务端的无序链接信息列表。
        '''
        is_list = type(uris) is list
        r = self.request('put',
                         '/file/source',
                         json={'uris': uris_to_list(uris)})
        if not is_list:
            return r[0]['link']
        return r

    def get_share_url(self,
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
        r = self.request('put',
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

    def _create(self,
                uri,
                type: Literal['folder', 'file'],
                err_on_conflict=False):
        '''
        创建文件夹或文件
        @param uri: 目标URI
        @param type: 创建类型，folder或file
        @param err_on_conflict: 若目标已存在，是否报错
        '''
        return self.request('post',
                            '/file/create',
                            json={
                                'uri': revise_file_path(uri),
                                'type': type,
                                'err_on_conflict': err_on_conflict,
                            })

    def create_file(self, uri, err_on_conflict=False):
        '''
        创建文件
        @param uri: 目标URI
        @param err_on_conflict: 若目标已存在，是否报错
        '''
        return self._create(uri, 'file', err_on_conflict)

    def create_folder(self, uri, err_on_conflict=False):
        '''
        创建文件夹
        @param uri: 目标URI
        @param err_on_conflict: 若目标已存在，是否报错
        '''
        return self._create(uri, 'folder', err_on_conflict)

    create_dir = create_directory = create_folder

    def update_file_content(self, file_uri, content, previous=None):
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
        return self.request('put', '/file/content', params=params,
                            data=content.encode('utf-8') if isinstance(content, str) else content)

    def delete(self,
               uris: Union[str, List[str]],
               unlink=False,
               trash_bin=True):
        '''
        删除文件或文件夹
        @param uris(str|list): 文件URI
        @param unlink: 仅解除链接
        @param trash_bin: 默认移入回收站；False 永久删除
        '''
        return self.request('delete',
                            '/file',
                            json={
                                'uris': uris_to_list(uris),
                                'unlink': unlink,
                                'skip_soft_delete': not trash_bin
                            })

    remove = delete

    def rename(self, uri: str, new_name: str):
        '''
        重命名文件或文件夹
        @param uri: 文件URI
        @param new_name: 新名称
        '''
        return self.request('post',
                            '/file/rename',
                            json={
                                'uri': revise_file_path(uri),
                                'new_name': new_name
                            })

    def copy_or_move(self, uris, dst, copy=False):
        '''
        通过来源文件夹和文件ID复制文件或文件夹
        @param uris: 源文件或文件夹URI列表
        @param dst: 目标目录
        @param copy: 是否为复制操作，默认为False（移动操作）
        '''
        return self.request('post',
                            '/file/move',
                            json={
                                'uris': uris_to_list(uris),
                                'dst': revise_file_path(dst),
                                'copy': copy,
                            })

    def copy(self, uris: Union[str, List[str]], dst: str):
        '''
        通过路径复制文件或文件夹
        @param uris: 源文件或文件夹URI或URI列表
        @param dst: 目标目录
        '''
        return self.copy_or_move(uris, dst, copy=True)

    def move(self, uris: Union[str, List[str]], dst: str):
        '''
        通过路径移动文件或文件夹
        @param uris: 源文件或文件夹URI或URI列表
        @param dst: 目标目录
        '''
        return self.copy_or_move(uris, dst, copy=False)

    def _upload_to_local(self, local_file: Path, session_id, chunk_size,
                         **kwards):
        if type(chunk_size) is not int or chunk_size < 0:
            raise ValueError('Invalid upload chunk size')
        size = local_file.stat().st_size
        with open(local_file, 'rb') as file:
            # Relay chunk_size=0 means one streaming request, including empty files.
            count = max(1, (size + chunk_size - 1) // chunk_size) if chunk_size else 1
            for block_id in range(count):
                chunk = file.read(chunk_size) if chunk_size else (file if size else b'')
                self.request(
                    'post',
                    f'/file/upload/{session_id}/{block_id}',
                    headers={
                        'Content-Length': str(len(chunk) if chunk_size else size),
                        'Content-Type': 'application/octet-stream',
                    },
                    data=chunk,
                )

    def _upload_to_remote_direct(self, local_file: Path, session_id,
                                 chunk_size, upload_urls, credential, **kwargs):
        if type(chunk_size) is not int or chunk_size < 0:
            raise ValueError('Invalid upload chunk size')
        size = local_file.stat().st_size
        url = upload_urls[0]
        origin(url)
        with local_file.open('rb') as file:
            count = max(1, (size + chunk_size - 1) // chunk_size) if chunk_size else 1
            for index in range(count):
                chunk = file.read(chunk_size) if chunk_size else (file if size else b'')
                with self._send(self._external_session(), 'post', url,
                                params={'chunk': index},
                                headers={'Authorization': credential,
                                         'Content-Type': 'application/octet-stream',
                                         'Content-Length': str(len(chunk) if chunk_size else size)},
                                data=chunk) as response:
                    response_data(response)

    def _upload_to_onedrive(self, local_file: Path, session_id, chunk_size,
                            upload_urls, callback_secret, **kwargs):
        self._onedrive_chunks(local_file, chunk_size, upload_urls[0])
        self.request('post', f'/callback/onedrive/{session_id}/{callback_secret}')

    def upload(self, local_file_path, uri, previous=None):
        '''
        上传文件
        @param local_file_path: 本地文件路径
        @param uri: 文件目标路径（包含文件名）
        @param previous: 覆盖时传当前文件的 primary_entity；省略则创建新文件
        '''
        local_file = Path(local_file_path)
        if not local_file.is_file():
            raise FileNotFoundError(f'{local_file_path} is not a file')

        uri = revise_file_path(uri)
        if not urlsplit(uri).path.strip('/'):
            raise ValueError('Upload destination must include a file name')
        dir = uri[:uri.rfind('/')]
        policy = self.list(dir)['storage_policy']
        policy_id, policy_type = policy['id'], policy['type']
        if not policy.get('relay') and policy_type not in ('local', 'remote', 'onedrive'):
            raise ValueError(f'存储策略 {policy_type} 需要启用中转上传')
        if previous is not None and (not isinstance(previous, str) or not previous):
            raise ValueError('Previous version must be a nonempty entity ID')

        mime_type, _ = guess_type(local_file.name)
        stat = local_file.stat()
        size = stat.st_size
        if policy_type == 'onedrive' and not policy.get('relay') and size == 0:
            raise ValueError('OneDrive direct upload does not support empty files')
        time = int(stat.st_mtime * 1000)

        body = {
            'uri': uri,
            'size': size,
            'last_modified': time,
            'policy_id': policy_id,
            'mime_type': mime_type,
        }
        if previous is not None:
            body.update(previous=previous, entity_type='version')
        r = self.request('put', '/file/upload', json=body)
        try:
            session_policy = r.get('storage_policy', policy)
            policy_type = session_policy['type']
            if session_policy.get('relay') or policy_type == 'local':
                self._upload_to_local(local_file=local_file, **r)
            elif policy_type == 'remote':
                if r.get('upload_urls'):
                    self._upload_to_remote_direct(local_file=local_file, **r)
                else:
                    self._upload_to_local(local_file=local_file, **r)
            elif policy_type == 'onedrive':
                self._upload_to_onedrive(local_file=local_file, **r)
            else:
                raise ValueError(f'存储策略 {policy_type} 暂时不受支持')
            check_source_unchanged(local_file, stat)
            if self.get_info(uri)['size'] != size:
                raise ValueError('Uploaded size differs from source')
        except BaseException as error:
            error.upload_session_id = r.get('session_id')
            error.upload_uri = uri
            try:
                self.request('delete', '/file/upload', json={'id': r['session_id'], 'uri': uri})
            except Exception as cleanup_error:
                error.cleanup_error = cleanup_error
                raise error from cleanup_error
            raise

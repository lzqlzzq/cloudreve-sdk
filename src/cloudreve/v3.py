from pathlib import Path

from urllib.parse import quote_plus, urljoin

from .transport import Client


def generate_src(file_id, is_dir) -> dict:
    src = {
        'items': [],
        'dirs': [],
    }
    if is_dir:
        src['dirs'].append(file_id)
    else:
        src['items'].append(file_id)
    return src


def revise_file_path(file_path: str) -> str:
    if not file_path:
        raise ValueError('File path must not be empty')
    if not file_path.startswith('/'):
        file_path = '/' + file_path
    return file_path.rstrip('/') or '/'


class Cloudreve(Client):
    api_version = "v3"

    def login(self, email, password):
        '''
        登录（请在执行其他操作前调用此方法）
        @param email: 邮箱
        @param password: 密码
        '''

        r = self.request('POST',
                         '/user/session',
                         json={
                             'userName': email,
                             'Password': password,
                             'captchaCode': ''
                         })
        self.user = r
        with self._lock:
            self._template.cookies.update(self.session.cookies)

    def list(self, path='/'):
        '''
        列出目录下的文件
        @param path: 目录路径
        @return: 文件列表
            - parent: 目录文件夹ID
            - objects: 文件列表
            - policy: 文件存储策略
        '''

        return self.request('get', '/directory' + quote_plus(path, safe=[]))

    def get_id(self, file_path: str, return_type=False):
        '''
        根据文件路径获取文件ID
        @param file_path: 文件路径
        @param return_type: 是否返回文件类型
        @return: 文件ID, (文件类型)
        '''

        file_path = revise_file_path(file_path)

        dir = file_path[:file_path.rfind('/')] or '/'
        name = file_path[file_path.rfind('/') + 1:]

        file_list = self.list(dir)

        for file in file_list['objects']:
            if file['name'] == name:
                if return_type:
                    return file['id'], file['type']
                return file['id']

        raise Exception('File not found')

    def get_property(self, file_id, is_dir=False, trace_root=False):
        '''
        获取文件属性
        @param file_id: 文件ID
        @param is_dir: 是否为文件夹
        @param trace_root: 是否跟踪根目录
        @return: 文件属性
            - created_at: 创建时间
            - updated_at: 修改时间
            - policy: 存储策略名称
            - size: 文件（夹）大小
            - child_folder_num: 子文件夹数
            - child_file_num: 子文件数
            - path: 文件路径（仅在trace_root=True时有效）
        '''

        return self.request('get',
                            f'/object/property/{file_id}',
                            params={
                                'is_folder': is_dir,
                                'trace_root': trace_root,
                            })

    def get_download_url(self, file_id) -> str:
        '''
        获取文件临时下载链接
        @param file_id: 文件ID
        @return: 下载链接
        '''

        url = self.request('put', f'/file/download/{file_id}')
        return urljoin(self.base_url + '/', url)

    def download(self, file_id, save_path):
        '''
        下载文件至本地
        @param file_id: 文件ID
        @param save_path: 保存路径
        '''
        download_url = self.get_download_url(file_id)
        self._download(download_url, save_path)

    def get_source_url(self, file_id, url_only=True):
        '''
        获取文件直链
        @param file_id: 文件ID，可传递列表
        @param url_only: 若为True则只返回直链，若为False则返回包含url和name的字典。
        @return: 直链列表，若url_only=True，则列表项为字符串；否则返回包含url和name的字典列表。
        请注意：由于Cloudreve API设计问题，传入的文件ID为列表时，返回的数据无序且不包含文件ID。因此请勿在同时获取多个直链时启用url_only。
        '''

        is_list = type(file_id) == list

        items = file_id if is_list else [file_id]

        r = self.request('post', '/file/source', json={'items': items})

        res = []

        if url_only:
            assert len(r) == 1, '传入的文件ID为列表时无法启用url_only。'
            for i in r:
                res.append(i['url'])
        else:
            for i in r:
                res.append({'url': i['url'], 'name': i['name']})

        if is_list:
            return res
        else:
            return res[0]

    def get_share_url(self,
                      id,
                      is_dir=False,
                      preview=True,
                      downloads=-1,
                      expire=0,
                      password=''):
        '''
        获取文件分享链接
        @param id: 文件或文件夹ID
        @param is_dir: 是否为文件夹
        @param preview: 是否允许预览
        @param downloads: 下载次数限制（默认不限制）
        @param expire: 过期时间（自现在开始的秒数）
        @param password: 密码
        '''

        assert downloads <= 0 or expire > 0, '设置下载次数限制需同时设置过期时间。'

        r = self.request('post',
                         '/share',
                         json={
                             'id': id,
                             'is_dir': is_dir,
                             'preview': preview,
                             'downloads': downloads,
                             'expire': expire,
                             'password': password,
                         })

        return r

    def delete(self, file_id, is_dir=False, force=False, unlink=False):
        '''
        删除文件或文件夹
        @param file_id: 文件ID
        @param is_dir: 是否为文件夹
        @param force: 强制删除文件
        @param unlink: 仅解除链接
        '''

        body = {
            'force': force,
            'unlink': unlink,
        }
        body.update(generate_src(file_id, is_dir))

        self.request('delete', '/object', json=body)

    def rename(self, file_id, new_name, is_dir=False):
        '''
        重命名文件或文件夹
        @param file_id: 文件ID
        @param new_name: 新名称
        @param is_dir: 是否为文件夹
        '''

        body = {
            'action': 'rename',
            'src': generate_src(file_id, is_dir),
            'new_name': new_name,
        }

        self.request('post', '/object/rename', json=body)

    def _copy(self, src_dir, file_id, dst_dir, is_dir=False):
        '''
        通过来源文件夹和文件ID复制文件或文件夹
        @param src_dir: 来源文件夹
        @param file_id: 文件ID
        @param dst_dir: 目标目录
        @param is_dir: 操作的文件是否为文件夹
        '''

        body = {
            'src_dir': src_dir,
            'src': generate_src(file_id, is_dir),
            'dst': dst_dir,
        }

        self.request('post', '/object/copy', json=body)

    def copy(self, file_path, dst_dir):
        '''
        通过路径复制文件或文件夹
        @param file_path: 源文件或文件夹路径
        @param dst_dir: 目标目录
        '''

        file_path = revise_file_path(file_path)

        src_dir = file_path[:file_path.rfind('/')] or '/'
        src_file_id, file_type = self.get_id(file_path, True)

        self._copy(src_dir, src_file_id, dst_dir, file_type == 'dir')

    def _move(self, src_dir, file_id, dst_dir, is_dir=False):
        '''
        通过来源文件夹和文件ID移动文件或文件夹
        @param src_dir: 来源文件夹
        @param file_id: 文件ID
        @param dst_dir: 目标目录
        @param is_dir: 操作的文件是否为文件夹
        '''

        body = {
            'action': 'move',
            'src_dir': src_dir,
            'src': generate_src(file_id, is_dir),
            'dst': dst_dir,
        }

        self.request('patch', '/object', json=body)

    def move(self, file_path, dst_dir):
        '''
        通过路径移动文件或文件夹
        @param file_path: 源文件或文件夹路径
        @param dst_dir: 目标目录
        '''

        file_path = revise_file_path(file_path)

        src_dir = file_path[:file_path.rfind('/')] or '/'
        src_file_id, file_type = self.get_id(file_path, True)

        self._move(src_dir, src_file_id, dst_dir, file_type == 'dir')

    def create_dir(self, dir_path):
        '''
        创建文件夹
        @param dir_path: 文件夹路径
        '''

        dir_path = revise_file_path(dir_path)

        self.request('put', '/directory', json={'path': dir_path})

    def upload_to_local(self, local_file: Path, sessionID, chunkSize, expires):
        if type(chunkSize) is not int or chunkSize < 0:
            raise ValueError('Invalid upload chunk size')
        size = local_file.stat().st_size
        with local_file.open('rb') as file:
            count = max(1, (size + chunkSize - 1) // chunkSize) if chunkSize else 1
            for block_id in range(count):
                chunk = file.read(chunkSize) if chunkSize else (file if size else b'')
                self.request('post', f'/file/upload/{sessionID}/{block_id}',
                             headers={'Content-Length': str(len(chunk) if chunkSize else size),
                                      'Content-Type': 'application/octet-stream'}, data=chunk)

    def upload_to_onedrive(self, local_file: Path, sessionID, chunkSize, expires, uploadURLs):
        self._onedrive_chunks(local_file, chunkSize, uploadURLs[0])
        self.request('post', f'/callback/onedrive/finish/{sessionID}', json={})

    def upload_to_oss(self, *args, **kwargs):
        raise NotImplementedError('OSS direct upload is not supported')

    def upload(self,
               file_path,
               local_file_path,
               policy_id=None,
               policy_type=None):
        '''
        上传文件通用方法
        @param file_path: 文件目标路径
        @param local_file_path: 本地文件路径
        @param policy_id: 存储策略ID（可选）
        @param policy_type: 存储策略类型（可选）
        当且仅当存储策略ID和类型同时存在时参数生效，否则程序将通过list方法获取存储策略信息
        '''

        local_file = Path(local_file_path)
        if not local_file.is_file():
            raise FileNotFoundError(f'{local_file_path} is not a file')

        file_path = revise_file_path(file_path)
        if file_path == '/':
            raise ValueError('Upload destination must include a file name')
        dir = file_path[:file_path.rfind('/')] or '/'
        name = file_path[file_path.rfind('/') + 1:]

        if not (policy_id and policy_type):
            policy = self.list(dir)['policy']
            policy_id, policy_type = policy['id'], policy['type']

        if policy_type not in ('local', 'onedrive'):
            raise ValueError(f'Unsupported storage policy: {policy_type}')
        if policy_type == 'onedrive' and local_file.stat().st_size == 0:
            raise ValueError('OneDrive direct upload does not support empty files')

        body = {
            'path': dir,
            'name': name,
            'size': local_file.stat().st_size,
            'last_modified': int(local_file.stat().st_mtime * 1000),
            'policy_id': policy_id,
            'mime_type': '',
        }

        r = self.request('put', '/file/upload', json=body)

        if policy_type == 'local':
            return self.upload_to_local(
                local_file=local_file,
                **r,
            )
        elif policy_type == 'onedrive':
            return self.upload_to_onedrive(
                local_file=local_file,
                **r,
            )
        else:
            raise ValueError(f'存储策略 {policy_type} 暂时不受支持')

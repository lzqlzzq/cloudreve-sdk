import os
from pathlib import Path
from tempfile import NamedTemporaryFile

from requests import HTTPError, Session
from requests.exceptions import ChunkedEncodingError


def save_response(response, save_path):
    """Commit only a complete download; an interrupted response leaves the old file intact."""
    if response.status_code != 200:
        raise HTTPError(f'Download HTTP {response.status_code}', response=response)
    destination = Path(save_path)
    temporary_path = None
    try:
        with NamedTemporaryFile(dir=destination.parent, delete=False) as file:
            temporary_path = Path(file.name)
            written = 0
            for chunk in response.iter_content(chunk_size=64 * 1024):
                if chunk:
                    file.write(chunk)
                    written += len(chunk)
            length = response.headers.get('Content-Length')
            if length is not None and not response.headers.get('Content-Encoding') and written != int(length):
                raise ChunkedEncodingError('Download ended before Content-Length was satisfied')
        os.replace(temporary_path, destination)
    finally:
        if temporary_path is not None and temporary_path.exists():
            temporary_path.unlink()


def download_file(url: str, save_path: str, session: Session = None, **request_kwargs):
    s = session if session is not None else Session()
    request_kwargs.setdefault('timeout', (10, 120))
    request_kwargs.setdefault('allow_redirects', False)
    try:
        with s.get(url, stream=True, **request_kwargs) as response:
            save_response(response, save_path)
    finally:
        if session is None:
            s.close()

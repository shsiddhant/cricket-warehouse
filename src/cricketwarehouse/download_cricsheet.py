from __future__ import annotations
from typing import TYPE_CHECKING, Any, Callable
import requests
from zipfile import ZipFile

if TYPE_CHECKING:
    from pathlib import Path

def download_from_url(
    url: str,
    filepath: str | Path,
    chunk_size: int | None = None,
    callback: Callable[[int, int], Any] | None = None
    ):
    """
    Download file from URL
    """
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        total_size = int(r.headers.get("content-length", 0))
        with open(filepath, "wb") as file:
            downloaded_size = 0
            for chunk in r.iter_content(chunk_size):
                if chunk:
                    file.write(chunk)
                    if callback is not None:
                        downloaded_size += len(chunk)
                        callback(downloaded_size, total_size)

def extract_files(filepath: str | Path, output_dir: str | Path):
    """
    Extract zip file.
    """
    with ZipFile(filepath) as zip_file:
        fileslist = zip_file.filelist
        zip_file.extractall(output_dir)
    json_files_list = [
        file.filename for file in fileslist if file.filename.endswith(".json")
        ]
    return json_files_list

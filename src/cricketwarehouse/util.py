from __future__ import annotations
from rich.progress import Progress
from typing import TYPE_CHECKING
from requests import HTTPError
from typer import Exit

from cricketwarehouse.download_cricsheet import download_from_url

if TYPE_CHECKING:
    from pathlib import Path


def download_ui(url: str, filepath: str | Path):
    try:
        with Progress() as progress:
            download_task = progress.add_task("Downloading file...", total=None)

            def callback(
                downloaded_size: int,
                total_size: int
            ) -> None:
                status_text = (
                    f"Downloaded {(downloaded_size/1024):.2f} KiB out of "
                    f"{(total_size/1024):.2f} KiB"
                )
                progress.update(
                    download_task, total=total_size, completed=downloaded_size,
                    description=status_text, refresh=True
                )
            download_from_url(url, filepath, chunk_size=65536, callback=callback)

    except HTTPError as e:
        print("Error: ", e.args[0]["message"])
        raise Exit(e.args[0]["error"])


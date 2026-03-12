from __future__ import annotations
#from typing import TYPE_CHECKING
from typing_extensions import Annotated
import typer

from cricketwarehouse.util import download_ui

from pathlib import Path  # noqa: TC003

app = typer.Typer(name="cricketwarehouse")

@app.command("download")
def download(
    url: Annotated[str, typer.Argument(help="Cricsheet url")],
    filepath: Annotated[Path, typer.Argument(help="Path to downloaded file.")],
):
    """
    Download data from Cricsheet.
    """
    download_ui(url, filepath)

if __name__ == "__main__":
    app()

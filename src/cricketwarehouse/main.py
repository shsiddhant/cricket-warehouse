from __future__ import annotations
from typing import Optional
from typing_extensions import Annotated
import typer

from cricketwarehouse.cli_util import (
    download_ui,
    ingest,
    init_source,
    update_venue_city_seed
)
from cricketwarehouse import JSON_FILES_DIR, RAW_DATA_SCHEMA

from pathlib import Path  # noqa: TC003

app = typer.Typer(name="cricketwarehouse")

@app.command("fetch")
def download(
    url: Annotated[str, typer.Argument(help="Cricsheet url")],
    filepath: Annotated[Path, typer.Argument(help="Path to downloaded file")],
    extaction_dir: Annotated[
        Optional[Path], typer.Option(help="Path to extaction directory")
        ] = JSON_FILES_DIR
    ):
    """
    Fetch data from Cricsheet.
    """
    if extaction_dir is not None:
        download_ui(url, filepath, output_dir=extaction_dir)

@app.command("init")
def init(
    seed: Annotated[
        Optional[Path], typer.Option(help="Path to venue city CSV.")
        ],
    ):
    """
    Initialize source tables and venue city seed.
    """
    init_source()
    print("Source tables initialized.")
    if seed:
        with open(seed, "w") as file:
            file.write("venue_name,city\n")
        print("Initialized venue city seed.")

@app.command("ingest")
def ingest_files(
    json_files_path: Annotated[
        Path, typer.Argument(help="Path to directory containing JSON files")
        ],
    schema: Annotated[
        Optional[str], typer.Option(help="Schema for source tables")
        ] = RAW_DATA_SCHEMA
    ):
    """
    Ingest JSON files into source tables.
    """
    json_files_list = list(json_files_path.glob("*.json"))
    if schema is not None:
        ingest(json_files_list, schema, json_table_name="matches_json")

@app.command("update")
def update_venue_city(
    seed: Annotated[
        Path, typer.Argument(help="Path to directory containing JSON files")
        ],
    ):
    """
    Update venue city seed.
    """
    update_venue_city_seed(seed)

if __name__ == "__main__":
    app()

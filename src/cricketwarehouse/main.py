from __future__ import annotations
from typing import Optional
from typing_extensions import Annotated
import typer

from cricketwarehouse.cli_util import (
    download_ui,
    ingest,
    init_source,
    update_venue_city_seed,
    update_city_country_seed,
)
from cricketwarehouse import (
    JSON_FILES_DIR,
    RAW_DATA_SCHEMA,
    SEEDS_DIR,
)

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
    seeds: Annotated[
        Optional[bool], typer.Option(help="Initalize seeds.")
        ] = False
    ):
    """
    Initialize source tables and seeds.
    """
    init_source()
    print("Source tables initialized.")
    venue_city = SEEDS_DIR / "venue_city.csv"
    city_country = SEEDS_DIR / "city_country.csv"
    if seeds:
        with open(venue_city, "w") as file:
            file.write("venue_name,city\n")
        print("\nInitialized venue city seed.")

        with open(city_country, "w") as file:
            file.write("city,country\n")
        print("\nInitialized city country seed.")

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
    seeds: Annotated[
        Optional[bool], typer.Option(help="Initalize seeds.")
        ] = False
    ):
    """
    Update venue city seed.
    """
    if seeds:
        venue_city_seed = SEEDS_DIR / "venue_city.csv"
        city_country_seed = SEEDS_DIR / "city_country.csv"
        update_venue_city_seed(venue_city_seed)
        print("\nUpdated venue city seed.")
        update_city_country_seed(venue_city_seed, city_country_seed)
        print("\nUpdated city country seed.")

if __name__ == "__main__":
    app()

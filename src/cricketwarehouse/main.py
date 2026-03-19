from __future__ import annotations
from typing import Optional
from typing_extensions import Annotated
import typer
import logging

from cricketwarehouse.util import (
    connect_db
)
from cricketwarehouse.cli_util import (
    download_ui,
    ingest_batch,
    init_source,
    update_venue_city_seed,
    update_city_country_seed,
)
from cricketwarehouse.config import (
    init_config
)
from cricketwarehouse.logging import custom_logger
from cricketwarehouse import (
    JSON_FILES_DIR,
    RAW_DATA_SCHEMA,
    SEEDS_DIR,
    DOWNLOAD_DIR,
    CONFIG_FILE,
)

from pathlib import Path  # noqa: TC003

app = typer.Typer(name="cricketwarehouse")

@app.command("fetch")
def download(
    url: Annotated[str, typer.Argument(help="Cricsheet url")],
    filepath: Annotated[
        Path, typer.Argument(help="Path to downloaded file")
        ] =DOWNLOAD_DIR / "cricsheet_json.zip",
    extaction_dir: Annotated[
        Optional[Path], typer.Option(help="Path to extaction directory")
        ] = JSON_FILES_DIR
    ):
    """
    Fetch data from Cricsheet.
    """
    logger = custom_logger("cricwh.fetch")
    logger.info(
        "cricwh fetch %s %s %s %s",
        url if url else "",
        filepath if filepath else "",
        "--extraction-dir" if extaction_dir else "",
        extaction_dir if extaction_dir else ""
    )
    if extaction_dir is not None:
        download_ui(url, filepath, output_dir=extaction_dir)

@app.command("configure")
def configure():
    """
    Configure cricket-warehouse.
    """
    logger = custom_logger("cricwh.configure")
    logger.info(
        "cricwh configure"
    )
    if CONFIG_FILE:
        init_config(CONFIG_FILE)


@app.command("init")
def init(
    seeds: Annotated[
        Optional[bool], typer.Option(help="Initalize seeds.")
        ] = False
    ):
    """
    Initialize source tables and seeds.
    """
    logger = custom_logger("cricwh.init")
    logger.info(
        "cricwh init %s",
        "--seeds" if seeds else ""
    )
    try:
        conn = connect_db()
        init_source(conn)
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
    except Exception as e:
        print(e)
        logger.error(e)
    else:
        logger.info("Initialization finished.")

@app.command("ingest")
def ingest_files(
    json_files_path: Annotated[
        Path, typer.Argument(help="Path to directory containing JSON files")
        ] = JSON_FILES_DIR,
    schema: Annotated[
        Optional[str], typer.Option(help="Schema for source tables")
        ] = RAW_DATA_SCHEMA
    ):
    """
    Ingest JSON files into source tables.
    """
    logger = custom_logger("cricwh.ingest")
    logger.info(
        "cricwh ingest %s %s %s",
        json_files_path,
        "--schema" if schema else "",
        schema if schema else ""
        )
    json_files_list = list(json_files_path.glob("*.json"))
    if schema is not None:
        try:
            conn = connect_db()
            ingest_batch(
                conn, json_files_list, schema, json_table_name="matches_json",
                batch_size=1000
            )
            msg = "Ingested %s files into source tables."
            n_files = len(json_files_list)
            print(msg % n_files)
            logger.info(msg, n_files)
        except Exception as e:
            print(e)
            logger.error(e)

@app.command("update")
def update_venue_city(
    seeds: Annotated[
        Optional[bool], typer.Option(help="Initalize seeds.")
        ] = False
    ):
    """
    Update dbt seeds.
    """
    custom_logger("cricwh.update")
    logger = logging.getLogger("cricwh.update")
    logger.info("cricwh update %s", "--seeds" if seeds else "")
    if seeds:
        venue_city_seed = SEEDS_DIR / "venue_city.csv"
        city_country_seed = SEEDS_DIR / "city_country.csv"
        try:
            conn = connect_db()
            update_venue_city_seed(conn, venue_city_seed)
            print("\nUpdated venue city seed.")
            update_city_country_seed(venue_city_seed, city_country_seed)
            print("\nUpdated city country seed.")
        except Exception as e:
            print(e)
            logger.error(e)
        else:
            msg = "Updated dbt seeds"
            print(msg)
            logger.info(msg)

if __name__ == "__main__":
    app()

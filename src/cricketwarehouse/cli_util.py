from __future__ import annotations
from typing import TYPE_CHECKING
from rich.progress import Progress
from requests import HTTPError
from typer import Exit
import psycopg2
import csv
import logging
from logging.handlers import MemoryHandler

from cricketwarehouse import (
    JSON_FILES_DIR,
    RAW_DATA_SCHEMA,
    MODELS_SCHEMA
)
from cricketwarehouse.util import (
    init_db,
    update_files_list,
    get_current_files,
    update_src_venues,
)
from cricketwarehouse.copy_raw_data import (
    copy_json_to_table,
    copy_deliveries_json,
)

from cricketwarehouse.download_cricsheet import (
    download_from_url,
    extract_files,
)

if TYPE_CHECKING:
    from pathlib import Path

def download_ui(
    url: str,
    filepath: str | Path,
    output_dir: Path = JSON_FILES_DIR,
    logger: logging.Logger = logging.getLogger("cricwh.fetch")
    ):
    """
    Download files from URL and show progress.
    """
    logger.info("Starting download from URL: %s...", url)
    try:
        with Progress() as progress:
            download_task = progress.add_task(
                "Downloading and extracting...", total=None
                )

            def download_callback(
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

            download_from_url(
                url, filepath, chunk_size=65536, callback=download_callback
                )

            json_files_list = extract_files(filepath, output_dir)
            logger.info(
                "Downloaded ZIP from URL: '%s' and extracted %s JSON files.",
                url, len(json_files_list)
            )

    except HTTPError as e:
        logger.error(e)
        raise Exit(1)

def init_source(
    conn,
    json_table_name: str = "matches_json",
    logger = logging.getLogger("cricwh.init")
    ):
    try:
        print("\nInitializing source tables...\n")
        init_db(conn, RAW_DATA_SCHEMA, json_table_name=json_table_name)
    except IOError as e:
        logger.error(e)
        if e.errno:
            raise Exit(e.errno)
        else:
            raise Exit(1)
    except psycopg2.Error as e:
        logger.error(e)
        if conn is not None:
            conn.rollback()
        raise Exit(2)
    else:
        conn.commit()
        logger.info("Initialized source tables.")

def ingest(
    conn,
    json_files_list: list[Path],
    schema: str = RAW_DATA_SCHEMA,
    json_table_name: str = "matches_json",
    logger = logging.getLogger("cricwh.ingest")
    ):
    try:
        # Get currently ingested files list
        current_files_list = get_current_files(conn, schema)
        # Copy match info JSON data to matches JSON table
        logger.info("Copying to source table: '%s'...", json_table_name)
        copy_json_to_table(
            conn, json_files_list, current_files_list, schema, json_table_name
        )
        logger.info("JSON data copied to source table: '%s'.", json_table_name)
        # Copy deliveries JSON data to delveries JSON table
        logger.info("Copying to source table: 'deliveries_json'...")
        copy_deliveries_json(conn, json_files_list, current_files_list, schema)
        logger.info("Deliveries data copied to source table: 'deliveries_json'.")
        # Update ingested files table
        logger.info("Updating files list table: 'json_files'...")
        update_files_list(conn, json_files_list, schema)
        logger.info("Updated 'json_files' with newly ingested files.")
        # Update venues source
        logger.info("Updating source table: src_venues...")
        update_src_venues(conn, json_files_list, current_files_list, schema)
        logger.info("Updated source table: src_venues.")
    except IOError as e:
        logger.error(e)
        if e.errno:
            raise Exit(e.errno)
        else:
            raise Exit(1)
    except psycopg2.Error as e:
        logger.error(e)
        if conn is not None:
            conn.rollback()
        raise Exit(2)
    except Exception as e:
        logger.error(e)
        raise Exit(3)
    else:
        conn.commit()

def ingest_batch(
    conn,
    json_files_list: list[Path],
    schema: str = RAW_DATA_SCHEMA,
    json_table_name: str = "matches_json",
    batch_size: int = 500,
    logger = logging.getLogger("cricwh.ingest")
    ):
    """
    """
    size = len(json_files_list)
    if not batch_size or batch_size <= 0:
        raise ValueError("Batch Size must be bigger than zero.")
    else:
        n_batches = divmod(size, batch_size)[0]
        batches = [
            slice(i * batch_size, (i + 1) * batch_size) for i in range(n_batches)
        ]
    batches.append(slice(n_batches * batch_size, None))
    logger.info("Ingesting %s files...", size)
    print("Ingesting %s files..." % size)
    logger.info("Batch size: %s", batch_size)
    print("Batch size: %s" % batch_size)
    for batch_num, batch in enumerate(batches):
        logger.info("Batch #%s of %s", batch_num + 1, len(batches))
        print("Batch #%s of %s" % ( batch_num + 1, len(batches)))
        ingest(
            conn,
            json_files_list[batch],
            schema,
            json_table_name
        )
    for handler in logger.handlers:
        if isinstance(handler, MemoryHandler):
            handler.flush()

def update_venue_city_seed(
    conn,
    venue_city_seed: Path,
    schema: str | None = RAW_DATA_SCHEMA,
    model_schema: str | None = MODELS_SCHEMA,
    logger = logging.getLogger("cricwh.update")
    ):
    """
    Update venue_city seed (CSV)
    """
    select_new_venue_names = f"""
        SELECT venue_name
        FROM {schema}.src_venues
        EXCEPT
        SELECT venue_name
        FROM {model_schema}.venue_city;
    """
    select_new = f"""
        SELECT venue_name, city
        FROM {schema}.src_venues
        WHERE venue_name IN %s;
    """
    try:
        with conn.cursor() as cur:
            cur.execute(select_new_venue_names)
            new_venue_names = tuple(venue_name for (venue_name,) in cur.fetchall())
            if new_venue_names:
                cur.execute(select_new, (new_venue_names,))
                new_venues = [
                    (venue_name, city) if city else (venue_name, f"{venue_name}-CITY")
                    for (venue_name, city) in cur.fetchall()
                ]
                with open(venue_city_seed, "a", newline="") as file:
                    writer = csv.writer(file)
                    writer.writerows(new_venues)
    except IOError as e:
        logger.error(e)
        if e.errno:
            raise Exit(e.errno)
        else:
            raise Exit(1)
    except psycopg2.Error as e:
        logger.error(e)
        if conn is not None:
            conn.rollback()
        raise Exit(2)
    except Exception:
        raise
    else:
        logger.info("Updated venue city seed.")

def update_city_country_seed(
    venue_city_seed: Path,
    city_country_seed: Path,
    logger = logging.getLogger("cricwh.update")
    ):
    """
    Update city country seed.
    """
    with open(venue_city_seed, "r") as file:
        reader = csv.reader(file)
        cities = {
            row[1] for row in reader
            if row[1] and row[1] != "city"
        }
    with open(city_country_seed, "+a") as file:
        reader = csv.reader(file)
        cities_current = {
            row[1] for row in reader
            if row[1] and row[1] != "city"
        }
        cities_new = cities.difference(cities_current)
        city_country = [(city, "UNKNOWN") for city in cities_new]
        writer = csv.writer(file)
        writer.writerows(city_country)
        logger.info("Updated city country seed.")

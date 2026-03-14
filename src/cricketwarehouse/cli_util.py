from __future__ import annotations
from typing import TYPE_CHECKING
from rich.progress import Progress
from requests import HTTPError
from typer import Exit
import psycopg2
import csv

from cricketwarehouse import (
    JSON_FILES_DIR,
    RAW_DATA_SCHEMA
)
from cricketwarehouse.util import (
    connect_db,
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
    output_dir: Path = JSON_FILES_DIR
    ):
    """
    Download files from URL and show progress.
    """
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

            extract_files(filepath, output_dir)

    except HTTPError as e:
        print("Error: ", e.args[0]["message"])
        raise Exit(e.args[0]["error"])

def init_source(json_table_name: str = "matches_json"):
    conn = None
    try:
        conn = connect_db()
        print("\nInitializing source tables...\n")
        init_db(conn, RAW_DATA_SCHEMA, json_table_name=json_table_name)
    except IOError as e:
        print("Error: ", e)
        if e.errno:
            raise Exit(e.errno)
        else:
            raise Exit(1)
    except psycopg2.Error as e:
        print("Error:", e)
        if conn is not None:
            conn.rollback()
        raise Exit(2)
    else:
        conn.commit()

def ingest(
    json_files_list: list[Path],
    schema: str = RAW_DATA_SCHEMA,
    json_table_name: str = "matches_json"
    ):
    conn: psycopg2.extensions.connection | None
    conn = None
    try:
        # Connect to DB
        conn = connect_db()
        # Get currently ingested files list
        current_files_list = get_current_files(conn, schema)
        # Copy match info JSON data to matches JSON table
        print(f"\nCopying to source table '{json_table_name}'...")
        copy_json_to_table(
            conn, json_files_list, current_files_list, schema, json_table_name
        )
        print(f"\nJSON data copied to source table '{json_table_name}'.")
        # Copy deliveries JSON data to delveries JSON table
        print("\nCopying to source table 'deliveries_json'...")
        copy_deliveries_json(conn, json_files_list, current_files_list, schema)
        print("\nDeliveries data copied to source table 'deliveries_json'.")
        # Update ingested files table
        print("\nUpdating 'json_files' table...")
        update_files_list(conn, json_files_list, schema)
        print("\nUpdated table 'json_files' with newly ingested files.")
        # Update venues source
        print("\nUpdating source table 'src_venues'...")
        update_src_venues(conn, json_files_list, current_files_list, schema)
        print("\nUpdated source table 'src_venues'")
    except IOError as e:
        print("Error: ", e)
        if e.errno:
            raise Exit(e.errno)
        else:
            raise Exit(1)
    except psycopg2.Error as e:
        print("Error:", e)
        if conn is not None:
            conn.rollback()
        raise Exit(2)
    else:
        conn.commit()

def update_venue_city_seed(
    venue_city_seed: Path,
    schema: str | None = RAW_DATA_SCHEMA,
    ):
    """
    Update venue_city seed (CSV)
    """
    select_new_venue_names = f"""
        SELECT venue_name
        FROM {schema}.src_venues
        EXCEPT
        SELECT venue_name
        FROM cricket.venue_city;
    """
    select_new = f"""
        SELECT venue_name, city
        FROM {schema}.src_venues
        WHERE venue_name IN %s;
    """
    conn = None
    try:
        conn = connect_db()
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
        print("Error: ", e)
        if e.errno:
            raise Exit(e.errno)
        else:
            raise Exit(1)
    except psycopg2.Error as e:
        print("Error:", e)
        if conn is not None:
            conn.rollback()
        raise Exit(2)
    except Exception:
        raise

def update_city_country_seed(
    venue_city_seed: Path,
    city_country_seed: Path,
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

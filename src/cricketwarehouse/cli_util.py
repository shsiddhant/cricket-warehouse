from __future__ import annotations
from typing import TYPE_CHECKING
from rich.progress import Progress
from requests import HTTPError
from typer import Exit
import psycopg2

from cricketwarehouse import (
    JSON_FILES_DIR,
    RAW_DATA_SCHEMA
)
from cricketwarehouse.util import (
    connect_db,
    init_db,
    update_files_list,
    get_current_files,
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
        print("\nUpdating `json_files` table...")
        update_files_list(conn, json_files_list, schema)
        print("\nUpdated table `json_files` with newly ingested files.")
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

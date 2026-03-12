from __future__ import annotations
from rich.progress import Progress
from typing import TYPE_CHECKING
from requests import HTTPError
from typer import Exit
import hashlib
import psycopg2

from cricketwarehouse import RAW_DATA_SCHEMA
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


def get_file_hash(filepath: str | Path, hash_method: str = "md5"):
    """
    Get file content hash.
    """
    with open(filepath, "rb") as file:
        digest = hashlib.file_digest(file, hash_method)
        file_hash = digest.hexdigest()
    return file_hash

def init_db(
    conn: psycopg2.extensions.connection,
    schema: str = RAW_DATA_SCHEMA,
    incremental: bool = False,
):
    drop_schema_sql = f"""
        DROP SCHEMA IF EXISTS {schema} CASCADE;
        CREATE SCHEMA {schema};
        """
    create_tables = f"""
        CREATE TABLE {schema}.json_files (
            id SERIAL,
            filename TEXT,
            file_hash TEXT,
            UNIQUE (filename),
            PRIMARY KEY (id)
        );
        CREATE TABLE {schema}.matches_json (
            id SERIAL,
            data JSONB,
            PRIMARY KEY (id)
        );
        CREATE TABLE {schema}.deliveries_json (
            match_id INT,
            hash_id TEXT,
            n_innings INT,
            team TEXT,
            n_over INT,
            n_delivery INT,
            delivery JSONB
        );
        """
    if not incremental:
        with conn.cursor() as cur:
            cur.execute(drop_schema_sql)
            cur.execute(create_tables)

def update_files_list(
    conn: psycopg2.extensions.connection,
    json_files_list: list[Path],
    schema: str = RAW_DATA_SCHEMA
    ):
    """
    """
    current_files_list = get_current_files(conn, schema)
    insert_new_file = f"""
        INSERT INTO {schema}.json_files (filename, file_hash)
        VALUES (%s, %s);
    """
    for filepath in json_files_list:
        filename = filepath.name
        file_hash = check_file_hash_present(current_files_list, filepath)
        if file_hash is not None:
            try:
                with conn.cursor() as cur:
                    cur.execute(insert_new_file, (filename, file_hash))
            except psycopg2.Error as e:
                conn.rollback()
                raise psycopg2.Error(e)

def get_current_files(
    conn: psycopg2.extensions.connection,
    schema: str = RAW_DATA_SCHEMA
    ):
    select_files_sql = f"""
        SELECT
            file_hash
        FROM {schema}.json_files;
    """
    with conn.cursor() as cur:
        cur.execute(select_files_sql)
        current_files_list = cur.fetchall()
    return current_files_list

def check_file_hash_present(
    current_files_list: list[tuple[str]],
    filepath: Path,
    ):
    file_hash = get_file_hash(filepath)
    if (file_hash,) not in current_files_list:
        return file_hash
    else:
        return None


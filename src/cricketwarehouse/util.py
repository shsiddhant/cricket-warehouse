from __future__ import annotations
from typing import TYPE_CHECKING
import hashlib
import psycopg2
import os
from dotenv import load_dotenv

from cricketwarehouse import (
    CONFIG_DIR,
    RAW_DATA_SCHEMA,
)

if TYPE_CHECKING:
    from pathlib import Path


def connect_db() -> psycopg2.extensions.connection:
    conn = None
    # Load Environment
    load_dotenv(CONFIG_DIR / ".env")
    # Connect to DB
    conn = psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        host=os.getenv("DB_HOST")
        )
    return conn

def init_db(
    conn: psycopg2.extensions.connection,
    schema: str = RAW_DATA_SCHEMA,
    json_table_name: str = "matches_json"
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
        CREATE TABLE {schema}.{json_table_name} (
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
    try:
        with conn.cursor() as cur:
            cur.execute(drop_schema_sql)
            cur.execute(create_tables)
    except psycopg2.Error as e:
        raise psycopg2.Error from e

def get_file_hash(filepath: str | Path, hash_method: str = "md5"):
    """
    Get file content hash.
    """
    with open(filepath, "rb") as file:
        digest = hashlib.file_digest(file, hash_method)
        file_hash = digest.hexdigest()
    return file_hash

def update_files_list(
    conn: psycopg2.extensions.connection,
    json_files_list: list[Path],
    schema: str = RAW_DATA_SCHEMA
    ):
    """
    Update JSON files table.
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
    """
    Get currently ingested files.
    """
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
    """
    Check if file hash is present in current JSON files table.
    """
    file_hash = get_file_hash(filepath)
    if (file_hash,) not in current_files_list:
        return file_hash
    else:
        return None




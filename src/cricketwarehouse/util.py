from __future__ import annotations
from typing import TYPE_CHECKING
import hashlib
import psycopg2
import json
import os
import subprocess
import platform

from cricketwarehouse import (
    RAW_DATA_SCHEMA,
)
from cricketwarehouse.config import read_config

if TYPE_CHECKING:
    from pathlib import Path

def open_default_editor(filepath: str | Path):
    current_os = platform.system()

    if current_os == "Windows":
        os.startfile(filepath, "edit")
    else:
        subprocess.run(["nano", str(filepath)], check=True)

def connect_db() -> psycopg2.extensions.connection:
    conn = None
    # Load Config
    config_data = read_config()
    keys = [
        "dbname",
        "user",
        "password",
        "host"
    ]
    db_info = {key: config_data[key] for key in keys}
    # Connect to DB
    conn = psycopg2.connect(**db_info)
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
            super_over BOOL,
            delivery JSONB
        );
        CREATE TABLE {schema}.src_venues (
            id SERIAL,
            venue_name TEXT,
            city TEXT,
            UNIQUE (venue_name),
            PRIMARY KEY (id)
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


def update_src_venues(
    conn: psycopg2.extensions.connection,
    json_files_list: list[Path],
    current_files_list: list[tuple[str]],
    schema: str = RAW_DATA_SCHEMA,
    ):
    """
    Update venues source table.
    """
    insert_new_venues = f"""
        INSERT INTO {schema}.src_venues (venue_name, city)
        VALUES (%s, %s)
        ON CONFLICT DO NOTHING
    """
    for filepath in json_files_list:
        file_hash = check_file_hash_present(current_files_list, filepath)
        if file_hash is not None:
            with open(filepath, "r") as file:
                data = json.load(file)
                venue_name = data["info"].get("venue")
                city = data["info"].get("city")
            try:
                with conn.cursor() as cur:
                    cur.execute(insert_new_venues, (venue_name, city))
            except psycopg2.Error as e:
                conn.rollback()
                raise psycopg2.Error(e)
            else:
                conn.commit()



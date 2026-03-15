from __future__ import annotations
from typing import TYPE_CHECKING
import json
import io
import csv
import logging

if TYPE_CHECKING:
    import psycopg2
    from pathlib import Path

from cricketwarehouse import RAW_DATA_SCHEMA
from cricketwarehouse.util import check_file_hash_present

logger = logging.getLogger("cricwh.ingest")

def copy_json_to_table(
    conn: psycopg2.extensions.connection,
    json_files_list: list[Path],
    current_files_list: list[tuple[str]],
    schema: str = RAW_DATA_SCHEMA,
    json_table_name: str = "matches_json",
    ) -> None:
    """
    Copy JSON files as JSONB rows in a temp table.

    conn : psycopg2.extensions.connection.
        A psycopg2 connection to the database.
    json_files : list[Path]
        A list containing the paths to the match JSON files.
    json_table_name : str
        Name of the staging table to copy JSON data to.

    """
    for filepath in json_files_list:
        file_hash = check_file_hash_present(current_files_list, filepath)
        if file_hash is not None:
            filename = filepath.name
            match_id = filename.removesuffix(".json")
            with open(filepath, "r") as file:
                data = json.load(file)
            try:
                data["match_id"] = int(match_id)
            except ValueError:
                logger.info("Match id not integer: %s. Skipping file...", match_id)
                continue
            logger.info(
                "Copying match info to source '%s': %s...", json_table_name, match_id
            )
            with conn.cursor() as cur:
                cur.copy_expert(
                    f"COPY {schema}.{json_table_name} (data) FROM STDIN",
                    io.StringIO(json.dumps(data))
                )

def json_explode(json_file_path, hash_id, deliveries=[]):
    filename = json_file_path.name
    match_id = filename.removesuffix(".json")
    with open(json_file_path, "rb") as file:
        data = json.load(file)
    for n, inn in enumerate(data["innings"]):
        if "overs" not in inn:
            raise ValueError(
                f"Missing key: 'overs' for match id: {match_id}"
            )
        for o, over in enumerate(inn["overs"]):
            for n_d, deliv in enumerate(over["deliveries"]):
                d = {}
                try:
                    d["match_id"] = int(match_id)
                except ValueError:
                    raise ValueError(
                        f"Match id not integer: {match_id}. Skipping file..."
                    )
                d["hash_id"] = hash_id
                d["n_innings"] = n
                d["team"] = inn["team"]
                d["n_over"] = o
                d["n_delivery"] = n_d
                d["super_over"] = inn.get("super_over")
                d["delivery"] = json.dumps(deliv)
                deliveries.append(d)
    return deliveries

def copy_deliveries_json(
    conn: psycopg2.extensions.connection,
    json_files_list: list[Path],
    current_files_list: list[tuple[str]],
    schema = RAW_DATA_SCHEMA
) -> None:
    """
    """
    columns = [
        "match_id",
        "hash_id",
        "n_innings",
        "team",
        "n_over",
        "n_delivery",
        "super_over",
        "delivery",
    ]
    deliveries = []
    stdin = io.StringIO()
    writer = csv.DictWriter(stdin, fieldnames=columns)
    for filepath in json_files_list:
        file_hash = check_file_hash_present(current_files_list, filepath)
        if file_hash is not None:
            filename = filepath.name
            match_id = filename.removesuffix(".json")
            try:
                deliveries = json_explode(filepath, file_hash, deliveries)
            except ValueError as e:
                logger.info(e)
                continue
            else:
                logger.info(
                    "Copying deliveries to source 'deliveries_json': %s...", match_id
                )
    writer.writerows(deliveries)
    stdin.seek(0)
    with conn.cursor() as cur:
        cur.copy_expert(
            f"COPY {schema}.deliveries_json ({','.join(columns)}) FROM STDIN WITH CSV",
            stdin
        )

from __future__ import annotations
from typing import TYPE_CHECKING
import json
import io
import csv
import hashlib
from pathlib import Path

if TYPE_CHECKING:
    import psycopg2

from cricketwarehouse import RAW_DATA_SCHEMA

def copy_json_to_table(
    conn: psycopg2.extensions.connection,
    json_files: list[Path],
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
    for match_json in json_files:
        match_json = Path(match_json).resolve()
        with open(match_json, "r") as file:
            data = json.load(file)
        data["match_id"] = int(match_json.name.removesuffix(".json"))
        with conn.cursor() as cur:
            cur.copy_expert(
                f"COPY {schema}.{json_table_name} (data) FROM STDIN",
                io.StringIO(json.dumps(data))
            )

def json_explode(json_file_path, hash_id, deliveries=[]):
    with open(json_file_path, "rb") as file:
        data = json.load(file)
    for n, inn in enumerate(data["innings"]):
        for o, over in enumerate(inn["overs"]):
            for n_d, deliv in enumerate(over["deliveries"]):
                d = {}
                d["match_id"] = int(
                    Path(json_file_path).name.removesuffix(".json")
                )
                d["hash_id"] = hash_id
                d["n_innings"] = n
                d["team"] = inn["team"]
                d["n_over"] = o
                d["n_delivery"] = n_d
                d["delivery"] = json.dumps(deliv)
                deliveries.append(d)
    return deliveries

def copy_deliveries_json(
    conn: psycopg2.extensions.connection,
    json_files,
    schema = "womenswc",
    hash_method = "md5",
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
        "delivery",
    ]
    deliveries = []
    stdin = io.StringIO()
    writer = csv.DictWriter(stdin, fieldnames=columns)
    for match_json in json_files:
        with open(match_json, "rb") as file:
            digest = hashlib.file_digest(file, hash_method)
            hash_id = digest.hexdigest()
        deliveries = json_explode(match_json, hash_id, deliveries)
    writer.writerows(deliveries)
    stdin.seek(0)
    with conn.cursor() as cur:
        cur.copy_expert(
            f"COPY {schema}.deliveries_json ({','.join(columns)}) FROM STDIN WITH CSV",
            stdin
        )

from __future__ import annotations
from typing import TYPE_CHECKING, cast
import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook


if TYPE_CHECKING:
    from psycopg2.extensions import connection as Psycopg2Connection

logger = logging.getLogger("airflow.task")
logger.setLevel(logging.DEBUG)

def connect_db_airflow(postgres_conn_id: str):
    try:
        postgres_hook = PostgresHook(postgres_conn_id)
        raw_conn = postgres_hook.get_conn()
        conn = cast("Psycopg2Connection", raw_conn)
    except Exception as e:
        logger.error(e)
        raise
    else:
        return conn


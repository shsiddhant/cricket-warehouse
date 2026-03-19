# ruff: noqa: F401
from __future__ import annotations
import pendulum
from pathlib import Path
import logging

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models.param import Param

from cricketwarehouse import CACHE_DIR
from cricketwarehouse.cli_util import (
    download_ui,
    ingest_batch,
    init_source,
)
from cricketwarehouse.airflow_util import (
    connect_db_airflow,
)

task_logger = logging.getLogger("airflow.task")

@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 3, 17, tz="UTC"),
    catchup=False,
    params={
        "url": Param(
            default="https://cricsheet.org/downloads/recently_added_2_json.zip",
            type="string"
        ),
        "postgres_conn_id": Param(
            default="cricwh_pg_conn",
            type="string"
        )
    },
    tags=["cricwh"]
)
def ingest_data():
    """
    This is an ingestion pipeline for fetching and ingesting data from Cricsheet.
    """
    @task
    def fetch_data_task():
        """
        #### Fetch data
        Fetch data from Cricsheet.
        """
        ctx = get_current_context()
        url = ctx["params"]["url"] # pyright: ignore[reportTypedDictNotRequiredAccess]
        task_logger.info("URL: %s", url)
        try:
            CACHE_DIR.mkdir(parents=True, exist_ok=True)
            download_ui(
                url, filepath=CACHE_DIR / "cricsheet_json.zip", output_dir=CACHE_DIR,
                logger=task_logger
            )
        except Exception as e:
            task_logger.exception(e)
            raise
    @task
    def init_source_tables():
        """
        Initalialize Source tables.
        """
        ctx = get_current_context()
        postgres_conn_id = ctx["params"]["postgres_conn_id"] # pyright: ignore[reportTypedDictNotRequiredAccess]
        try:
            conn = connect_db_airflow(postgres_conn_id)
            init_source(conn, logger=task_logger)
        except Exception as e:
            task_logger.exception(e)
            raise

    @task
    def ingest_data_task():
        """
        Ingest data from Cricsheet.
        """
        ctx = get_current_context()
        postgres_conn_id = ctx["params"]["postgres_conn_id"] # pyright: ignore[reportTypedDictNotRequiredAccess]
        json_files_list = list(CACHE_DIR.glob("*.json"))
        task_logger.info("Files: %s", len(json_files_list))
        try:
            conn = connect_db_airflow(postgres_conn_id)
            ingest_batch(
                conn,
                json_files_list,
                batch_size=500,
                logger=task_logger
            )
        except Exception as e:
            task_logger.exception(e)
            raise

    #-------------------------------------------------

    task_fetch = fetch_data_task()
    task_init = init_source_tables()
    task_ingest = ingest_data_task()

    task_fetch >> task_init >> task_ingest # pyright: ignore[reportUnusedExpression]

ingest_data()

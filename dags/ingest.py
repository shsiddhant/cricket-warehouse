from __future__ import annotations
import pendulum
from pathlib import Path
import logging
import os
from dotenv import load_dotenv

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models.param import Param

from cosmos import (
    DbtTaskGroup,
    ExecutionConfig,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
)
from cosmos.constants import InvocationMode

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

load_dotenv()

DBT_PROJECT_NAME = "cricket_warehouse"
DBT_PROFILES_DIR = os.getenv("DBT_PROFILES_DIR", "/opt/airflow/dbt/")

profile_config = ProfileConfig(
    profile_name="cricket_warehouse",
    target_name="dev",
    profiles_yml_filepath=Path(DBT_PROFILES_DIR) / "profiles.yml"
)

shared_execution_config = ExecutionConfig(
    invocation_mode=InvocationMode.SUBPROCESS
)

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
        ),
        "reset": Param(
            default=False,
            type="boolean",
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
        postgres_conn_id = ctx["params"]["postgres_conn_id"]    # pyright: ignore[reportTypedDictNotRequiredAccess]
        reset = ctx["params"]["reset"]   # pyright: ignore[reportTypedDictNotRequiredAccess]
        try:
            if reset:
                conn = connect_db_airflow(postgres_conn_id)
                init_source(conn, logger=task_logger)
            else:
                task_logger.info(
                    "Not resetting source tables. "
                    "Set reset parameter as True if necessary."
                )
        except Exception as e:
            task_logger.exception(e)
            raise

    @task
    def ingest_data_task():
        """
        Ingest data from Cricsheet.
        """
        ctx = get_current_context()
        postgres_conn_id = ctx["params"]["postgres_conn_id"]    # pyright: ignore[reportTypedDictNotRequiredAccess]
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

    task_staging = DbtTaskGroup(
        group_id="staging",
        project_config=ProjectConfig(
            dbt_project_path=Path(DBT_PROFILES_DIR).as_posix(),
        ),
        render_config=RenderConfig(
            select=["path:models/staging"],
            enable_mock_profile=False,
            airflow_vars_to_purge_dbt_ls_cache=["purge"]
        ),
        execution_config=shared_execution_config,
        operator_args={"install_deps": True},
        profile_config=profile_config,
        default_args={"retries": 0},
    )

    task_fetch = fetch_data_task()
    task_init = init_source_tables()
    task_ingest = ingest_data_task()

    task_fetch >> task_init >> task_ingest >> task_staging # pyright: ignore[reportUnusedExpression]

ingest_data()

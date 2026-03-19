from __future__ import annotations
import yaml
import os
from dotenv import load_dotenv

from cricketwarehouse import (
    RAW_DATA_SCHEMA,
    MODELS_SCHEMA,
)

load_dotenv()

def init_config(config_file):
    example_config = {
        "dbname": os.getenv("DB_NAME", "cricket_warehouse"),
        "user": os.getenv("DB_USER", "my-user"),
        "password": os.getenv("DB_PASSWORD", "my-password"),
        "host": os.getenv("DB_HOST", "localhost"),
        "port": int(os.getenv("DB_PORT", "5432")),
        "profile": os.getenv("DBT_PROFILE", "cricket_warehouse"),
        "threads": 1,
        "type": "postgres",
        "source_schema": os.getenv("SRC_SCHEMA", RAW_DATA_SCHEMA),
        "schema": os.getenv("SCHEMA", MODELS_SCHEMA)
    }
    with open(config_file, "w") as file:
        yaml.dump(example_config, file, default_flow_style=False)

def read_config(config_file):
    with open(config_file, "r") as file:
        config_data = yaml.safe_load(file)
    return config_data

from __future__ import annotations
import yaml

from cricketwarehouse import (
    CONFIG_FILE,
    RAW_DATA_SCHEMA,
    MODELS_SCHEMA,
)


def init_config():
    example_config = {
        "dbname": "my-db",
        "user": "my-user",
        "password": "my-password",
        "host": "localhost",
        "port": 5432,
        "profile": "cricket_warehouse",
        "threads": 1,
        "type": "postgres",
        "source_schema": RAW_DATA_SCHEMA,
        "schema": MODELS_SCHEMA
    }
    with open(CONFIG_FILE, "w") as file:
        yaml.dump(example_config, file, default_flow_style=False)

def read_config():
    with open(CONFIG_FILE, "r") as file:
        config_data = yaml.safe_load(file)
    return config_data


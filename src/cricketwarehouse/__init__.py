from __future__ import annotations
from platformdirs import (
    user_config_path,
    user_cache_path,
    user_state_path
)
from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv()


APPNAME = "cricketwarehouse"

CONFIG_DIR = user_config_path(appname=APPNAME, appauthor=False, ensure_exists=True)
STATE_DIR = user_state_path(appname=APPNAME, appauthor=False, ensure_exists=True)
CACHE_DIR = user_cache_path(appname=APPNAME, appauthor=False, ensure_exists=True)

DOWNLOAD_DIR = CACHE_DIR
JSON_FILES_DIR = CACHE_DIR

SEEDS_DIR = Path(os.getenv("DBT_PROFILES_DIR", "dbt")) / "seeds"

MODELS_SCHEMA = "cricket"
RAW_DATA_SCHEMA = "raw"

DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
JSON_FILES_DIR.mkdir(parents=True, exist_ok=True)

CONFIG_FILE = CONFIG_DIR / "config.yaml"
LOG_FILE = STATE_DIR / "cricwh.log"

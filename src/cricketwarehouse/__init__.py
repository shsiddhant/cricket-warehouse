from __future__ import annotations
from platformdirs import (
    user_config_path,
    user_data_path,
    user_cache_path,
)
from pathlib import Path


APPNAME = "cricketwarehouse"

CONFIG_DIR = user_config_path(appname=APPNAME, appauthor=False, ensure_exists=True)
USER_DATA_DIR = user_data_path(appname=APPNAME, appauthor=False, ensure_exists=True)
CACHE_DIR = user_cache_path(appname=APPNAME, appauthor=False, ensure_exists=True)

DOWNLOAD_DIR = CACHE_DIR / "raw" / "download"
JSON_FILES_DIR = CACHE_DIR / "raw" / "extracted"

PROJECT_DIR = Path(__file__).parent.parent.parent.resolve()
SEEDS_DIR = PROJECT_DIR / "seeds"

MODELS_SCHEMA = "cricket"
RAW_DATA_SCHEMA = "raw"

DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
JSON_FILES_DIR.mkdir(parents=True, exist_ok=True)

CONFIG_FILE = CONFIG_DIR / "config.yaml"
LOG_FILE = USER_DATA_DIR / "cricwh.log"

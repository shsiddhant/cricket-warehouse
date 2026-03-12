from __future__ import annotations
from pathlib import Path

PROJECT_DIR = Path(__file__).parent.parent.parent.resolve()

DATA_DIR = PROJECT_DIR / "data"
CONFIG_DIR = PROJECT_DIR / "config"
DOWNLOAD_DIR = DATA_DIR / "raw" / "download"
JSON_FILES_DIR = DATA_DIR / "raw" / "extracted"
SEEDS_DIR = PROJECT_DIR / "seeds"
MAIN_SCHEMA = "cricket"
RAW_DATA_SCHEMA = "raw"

DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
JSON_FILES_DIR.mkdir(parents=True, exist_ok=True)

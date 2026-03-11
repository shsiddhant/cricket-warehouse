from __future__ import annotations
from pathlib import Path

DATA_DIR = Path(__file__).parent.parent.parent.resolve() / "data"
CITY_COUNTRY_CSV = DATA_DIR / "city_country.csv"
DOWNLOAD_DIR = DATA_DIR / "raw" / "download"
JSON_FILES_DIR = DATA_DIR / "raw" / "extracted"
MAIN_SCHEMA = "cricket"
RAW_DATA_SCHEMA = "raw"
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
JSON_FILES_DIR.mkdir(parents=True, exist_ok=True)


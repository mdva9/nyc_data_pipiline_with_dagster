import polars as pl
from dagster import op
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / 'data'
RAW_DIR = DATA_DIR / 'tmp'
RAW_DIR.mkdir(parents=True, exist_ok=True)

@op
def transform(file_path: str)->:
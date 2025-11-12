import polars as pl
from dagster import op
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / 'data'
PROCESSED_DIR = DATA_DIR / 'processed'
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

@op
def load(file_path:str):
    """"""
    df = pl.read_parquet(file_path)

    out_path = PROCESSED_DIR / "yellow_tripdata_cleaned.parquet"

    df.write_parquet(out_path)

    return f"Saved cleaned dataset to {out_path}"


if __name__ == "__main__":
    file_path = DATA_DIR / "tmp" / "cleaned_trips.parquet"
    load(str(file_path))

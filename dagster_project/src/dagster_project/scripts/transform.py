import os
import polars as pl
from dagster import op
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / 'data'
TMP_DIR = DATA_DIR / 'tmp'
TMP_DIR.mkdir(parents=True, exist_ok=True)

@op
def transform(file_path: str):
    print("📂 Current working directory:", os.getcwd())
    """"""
    df = pl.read_parquet(file_path)

    df_clean=(
        df.
        filter((pl.col("passenger_count") > 0) &
        (pl.col("trip_distance") > 0) &
        (pl.col("payment_type") != 6) &
        (pl.col("total_amount") > 0)).sort("tpep_pickup_datetime")
    )

    out_path = TMP_DIR / "cleaned_trips.parquet"
    df_clean.write_parquet(out_path)

    return str(out_path)

if __name__ == "__main__":
    file_path = DATA_DIR / "raw"/ "yellow_tripdata_2024-01.parquet"
    transform(str(file_path))

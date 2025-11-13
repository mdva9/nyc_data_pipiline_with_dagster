import polars as pl
from dagster import op
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / 'data'
TMP_DIR = DATA_DIR / 'tmp'
TMP_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Transform Operation (Dagster Op)
#
# This operation performs the data cleaning and filtering step of the ETL
# pipeline. It reads the raw Parquet file produced during the extract step,
# applies several validation and filtering rules using Polars, and writes a
# cleaned intermediate dataset to the data/tmp/ directory.
#
# Responsibilities:
#   - Load the raw Parquet file from the provided file path
#   - Filter out invalid or unrealistic trips:
#       * passenger_count > 0
#       * trip_distance > 0
#       * payment_type != 6 (unknown/invalid code)
#       * total_amount > 0
#   - Sort records chronologically by pickup datetime
#   - Write the cleaned dataset to data/tmp/ as cleaned_trips.parquet
#   - Return the path to the cleaned file for downstream processing
#
# Dagster manages:
#   - Receiving the raw file path from the extract() operation
#   - Routing this cleaned output to the load() operation
#   - Logging, execution tracking, and visualization in the Dagit UI
#
# This op represents the "Transform" stage of the ETL pipeline and replaces the
# SQL-based transformation originally done inside BigQuery in the GCP version
# of the project.
# ---------------------------------------------------------------------------
@op
def transform(file_path: str):

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

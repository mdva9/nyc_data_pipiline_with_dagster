import polars as pl
from dagster import op
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / 'data'
PROCESSED_DIR = DATA_DIR / 'processed'
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Load Operation (Dagster Op)
#
# This operation represents the final stage of the ETL pipeline. It takes the
# cleaned dataset produced by the transform() step and writes it to the
# data/processed/ directory as the final output of the pipeline.
#
# Responsibilities:
#   - Read the cleaned Parquet file from the given file path
#   - Define the destination path for the final processed dataset
#   - Write the dataset to data/processed/yellow_tripdata_cleaned.parquet
#   - Return a confirmation message including the output file location
#
# Dagster manages:
#   - Receiving the cleaned file path from the transform() operation
#   - Executing this step after all upstream computations succeed
#   - Logging and visualization in the Dagit UI
#
# This op completes the ETL workflow, replacing the original "Load" step in
# BigQuery from the GCP ELT version and adapting it to a fully local storage
# process using Parquet files.
# ---------------------------------------------------------------------------

@op
def load(file_path:str):

    df = pl.read_parquet(file_path)

    out_path = PROCESSED_DIR / "yellow_tripdata_cleaned.parquet"

    df.write_parquet(out_path)


    return f"Saved cleaned dataset to {out_path}"
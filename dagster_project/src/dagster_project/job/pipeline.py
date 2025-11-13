from dagster import job
from dagster_project.scripts.extract import extract
from dagster_project.scripts.transform import transform
from dagster_project.scripts.load import load

# ---------------------------------------------------------------------------
# Main Dagster Job of the project (local ETL pipeline)
#
# This job orchestrates the three core operations of the data pipeline:
#   - extract():   downloads the raw Parquet file from the public NYC Taxi source
#   - transform(): cleans and filters the data using Polars
#   - load():      writes the final transformed dataset into the processed/ folder
#
# Dagster automatically manages:
#   - the execution order (extract → transform → load)
#   - passing outputs from one step as inputs to the next
#   - visual monitoring and debugging through the Dagit UI
#
# This job replaces the original Airflow DAG used in the GCP version of the
# project, providing a simpler, more readable, and fully local implementation
# adapted to the requirements of the technical task.
# ---------------------------------------------------------------------------
@job
def pipeline():
    load(transform(extract()))


import requests
from dagster import op
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / 'data'
RAW_DIR = DATA_DIR / 'raw'
RAW_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Extract Operation (Dagster Op)
#
# This operation downloads a raw Parquet dataset from the public NYC Taxi
# endpoint and stores it locally in the data/raw/ directory.
#
# Responsibilities:
#   - Define the download URL for the target month of yellow taxi trip data
#   - Perform an HTTP GET request to retrieve the Parquet file
#   - Save the downloaded content to the raw/ folder
#   - Return the local file path so it can be passed to downstream steps
#
# Dagster manages:
#   - Execution of this operation as the first step of the ETL pipeline
#   - Passing the output (file path) to the transform() operation
#   - Logging and monitoring through the Dagit UI
#
# This op serves as the "Extract" step of the pipeline and mirrors the logic
# used in the original Airflow-based ELT process, adapted for a fully local
# ETL workflow.
# ---------------------------------------------------------------------------

@op
def extract():

    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"

    file_path = RAW_DIR/ "yellow_tripdata_2024-01.parquet"

    response = requests.get(url)
    response.raise_for_status()

    with open (file_path, "wb") as file:
        file.write(response.content)

    return str(file_path)
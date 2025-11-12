import requests
from dagster import op
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / 'data'
RAW_DIR = DATA_DIR / 'raw'
RAW_DIR.mkdir(parents=True, exist_ok=True)

@op
def extract():
    """"""
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"

    file_path = RAW_DIR/ "yellow_tripdata_2024-01.parquet"

    response = requests.get(url)
    response.raise_for_status()

    with open (file_path, "wb") as file:
        file.write(response.content)

    return str(file_path)

if __name__ == "__main__":
    extract()
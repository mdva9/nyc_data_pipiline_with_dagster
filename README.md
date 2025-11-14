# 🚖 NYC Yellow Taxi Data Pipeline with Dagster (Local ETL Pipeline)
This project demonstrates a local implementation of an end-to-end ETL data pipeline using Dagster, Polars, and uv as the package manager.
The pipeline ingests raw taxi trip data from the NYC Open Data source, cleans it locally using Polars, and writes the final processed dataset to Parquet files.

This project is a simplified rework of a previous ELT pipeline originally implemented on GCP (BigQuery + Airflow).
Here, the goal is to migrate the orchestration logic to Dagster, in a fully local environment, as part of a technical task.
---

## 📂 Project Structure
```
nyc-taxi-dagster-pipeline/
│
├── src/
│   └── dagster_project/
│       ├── data/                         # Local data folders
│       │   ├── raw/                      # Extracted raw data (Parquet)
│       │   ├── tmp/                      # Intermediate cleaned data
│       │   └── processed/                # Final processed dataset
│       │
│       ├── scripts/                      # Dagster ops scripts
│       │   ├── extract.py                # Download raw taxi Parquet file
│       │   ├── transform.py              # Clean data using Polars
│       │   └── load.py                   # Save final processed dataset
│       │
│       ├── job/
│       │   ├── pipeline.py               # Dagster ETL job definition
│       └── __init__.py
│
├── pyproject.toml                        # uv project configuration
├── uv.lock                               # uv lockfile
├── README.md                             # Project documentation
└── workspace.yaml                        # Dagster workspace config
```
---

## 🚀 Pipeline Architecture

**Steps:**
1. Downloads the selected NYC Yellow Taxi dataset (Parquet) into **data/raw/**  
2. Transform the dataset with **Polars** and result saved in data/tmp/cleaned_trips.parquet
3. Stores the final processed dataset inside data/processed/ as **Parquet**.
---

## ⚙️ Local Setup

### 1. Prerequisites
- Python 3.12+
- **uv** installed (package manager)

### 2. Clone the repository

```bash
git clone  <your-repo-url>
cd nyc-taxi-dagster-pipeline/
```
### 3. Install dependencies with uv

This command will :

- create a lightweight virtual environment

- install Dagster, Dagit, Polars, PyArrow

- configure the packaged project structure defined in pyproject.toml

```bash
uv sync
```

## Pipeline Execution
These commands allow you to execute the pipeline in two different ways.
```bash
cd dagster_project/
```

### Dagit UI
Start the Dagster development server:
```bash
uv run dagster dev
```
Then open:
 http://127.0.0.1:3000

- Select the pipeline job
- Click on LaunchPad Section 
- Click Launch Run (bottom-right)

### CLI Execution
```bash
uv run dagster job execute -m dagster_project.job.pipeline -j pipeline
```



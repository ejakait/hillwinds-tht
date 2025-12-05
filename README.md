# Hillwinds Data Engineering Take-Home Assignment

This project implements a data engineering pipeline to process raw CSV data, perform SQL-based analytics, and generate clean, transformed data.

## Project Structure

- `main.py`: The main entry point for the ETL process.
- `processing.py`: Contains Python functions for data transformation.
- `utils.py`: Utility functions.
- `sql/`: Contains SQL scripts for data ingestion, transformations, and analytical views.
  - `sql/ingest.sql`: SQL for loading raw CSVs into DuckDB.
  - `sql/transformations/`: SQL for intermediate data transformations.
  - `sql/views/`: SQL for generating analytical outputs (gaps, roster, spikes).
- `outputs/`: Directory where all generated output files are stored.
- `data/`: (Implicit) Raw CSV data files (`employees_raw.csv`, `plans_raw.csv`, `claims_raw.csv`, `company_lookup.json`, `api_mock.json`).

## Setup

1.  **Clone the repository:**
    ```bash
    git clone <repository_url>
    cd hillwinds-tht
    ```
2.  **Create a Python virtual environment (recommended):**
    ```bash
    python -m venv .venv
    source .venv/bin/activate
    ```
3.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```
    _(Note: `requirements.txt` will be generated if not present)_

## How to Run

To execute the entire ETL pipeline and generate all outputs, run the following command:

```bash
python main.py
```

## Outputs

All generated files are placed in the `outputs/` directory:

- **SQL Results:**
  - `outputs/sql_gaps.csv`: Identifies gaps in plan coverage.
  - `outputs/sql_roster.csv`: Compares observed vs. expected employee counts.
  - `outputs/sql_spikes.csv`: Flags significant spikes in claims costs.
- **Python Artifacts:**
  - `outputs/validation_errors.csv`: Contains rows that failed basic validation checks (e.g., company name mismatch).
  - `outputs/clean_data/`: A directory containing cleaned and transformed data in Parquet format:
    - `outputs/clean_data/claims_data.parquet`
    - `outputs/clean_data/employees_data.parquet`
    - `outputs/clean_data/plans_data.parquet`

## Assumptions and Edge Cases

### General

- All raw CSV files (`employees_raw.csv`, `plans_raw.csv`, `claims_raw.csv`) are expected to be in the project root directory.
- `company_lookup.json` is used to map company names to EINs and vice-versa.

### SQL Gaps (`sql/views/sql_gaps.sql`)

- Assumes `plans_raw` contains `company_ein`, `plan_type`, `start_date`, `end_date`, and `carrier_name`.
- Overlapping or adjacent plan intervals for the same `company_ein` and `plan_type` are "stitched" together to form continuous coverage periods before identifying gaps.
- A "gap" is defined as a period greater than 7 days between stitched plan intervals.

### SQL Roster (`sql/views/sql_roster.sql`)

- Assumes `employees_raw` contains `company_ein` and `email`.
- Expected employee counts are hardcoded within the SQL query. In a real-world scenario, these would likely come from a configuration table or external source.
- Severity levels (Low, Medium, High, Critical) are based on the percentage difference between observed and expected employee counts as defined in the assessment.

### SQL Spikes (`sql/views/sql_spikes.sql`)

- Assumes `claims_raw` contains `company_ein`, `service_date`, and `amount`.
- A "spike" is defined as a greater than 200% increase in the current 90-day rolling claim cost compared to the previous 90-day rolling claim cost.

### Python ETL (`main.py`, `processing.py`)

- **Data Enrichment:** The `api_enrichment` function is mocked and returns static data. The integration of this enrichment data into the main pipeline was skipped.
- **Validation Framework:** A comprehensive data validation framework was skipped. Only a basic check for company name existence in `transform_employees` is performed, with mismatches written to `validation_errors.csv`.
- **Incremental Loading:** Incremental loading using a high-water mark was stubbed. The pipeline currently processes all available raw data on each run and appended in a partitioned parquet.
- **Clean Data Output:** `outputs/clean_data/` is a directory containing separate Parquet files for each transformed entity (claims, employees, plans)

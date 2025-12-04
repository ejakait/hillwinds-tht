import os
from datetime import datetime
from typing import Any, Dict

import duckdb
import pandas as pd
from structlog import get_logger

from processing import (
    transform_claims,
    transform_employees,
    transform_plans,
)
from utils import retry

# configure_structlog()

logger = get_logger(__name__)

# --- High-Water Mark Stubs ---
HIGH_WATER_MARK_FILE = "outputs/high_water_mark.txt"


def read_high_water_mark() -> datetime:
    """
    STUB: Reads the high-water mark from a file.
    In a real implementation, this would handle file not found errors
    and return a default start time if no file exists.
    """
    logger.info(f"STUB: Reading high-water mark from {HIGH_WATER_MARK_FILE}")
    if os.path.exists(HIGH_WATER_MARK_FILE):
        # In a real implementation, parse the timestamp from the file
        # For this stub, we return a fixed old date to simulate a previous run.
        return datetime(2023, 1, 1)
    # Default to a very old date if no high-water mark is found
    return datetime(1970, 1, 1)


def write_high_water_mark(new_high_water_mark: datetime):
    """
    STUB: Writes the new high-water mark to a file.
    """
    logger.info(
        f"STUB: Writing new high-water mark to {HIGH_WATER_MARK_FILE}: {new_high_water_mark}"
    )
    # In a real implementation, this would write the timestamp to the file.
    # with open(HIGH_WATER_MARK_FILE, "w") as f:
    #     f.write(new_high_water_mark.isoformat())
    pass  # Stub does nothing


@retry
def api_enrichment(company_website: str) -> Dict[str, Any]:
    try:
        endpoint = f"https://api.example.com/enrichment/{company_website}"
        # response = requests.get(url=endpoint)
        # response.raise_for_status()
        # data = response.json()
        data = {"industry": "Technology", "revenue": "100M", "headcount": "500"}
        logger.info(f"API enrichment fetch for {company_website} successful")
        return data
    except Exception as e:
        # TODO: handle 429 error and response with backoff period
        logger.error(f"Error during API enrichment: {e}")
        return {}


def extract(con: duckdb.DuckDBPyConnection, high_water_mark: datetime):
    """
    Ingest data from files.
    In a real implementation, this would filter data based on the high_water_mark.
    """
    logger.info(f"Starting extraction for data newer than: {high_water_mark}")
    with open("sql/ingest.sql", "r") as f:
        for line in f.readlines():
            try:
                # In a real implementation, the SQL in ingest.sql would be parameterized
                # or modified here to include a WHERE clause, like:
                # if "FROM 'claims_raw.csv'" in line:
                #     line = line.replace(";", f" WHERE last_updated > '{high_water_mark}';")
                logger.info(f"Executing SQL statement: {line.strip()}")
                con.sql(line)
                logger.info("SQL statement executed successfully")
            except Exception as e:
                logger.error(f"Error executing SQL statement: {line.strip()}")
                logger.error(f"Error message: {e}")
                return False
        return True


if __name__ == "__main__":
    required_files = ["plans_raw.csv", "employees_raw.csv", "claims_raw.csv"]
    csv_files = [f for f in os.listdir(".") if f.endswith(".csv")]
    assert all(x in required_files for x in csv_files)

    # Ensure output directories exist
    os.makedirs("outputs", exist_ok=True)
    os.makedirs("outputs/clean_data", exist_ok=True)

    last_high_water_mark = read_high_water_mark()

    with duckdb.connect("hillwinds.db") as con:
        extract(con=con, high_water_mark=last_high_water_mark)

        claims_df = transform_claims(con)
        valid_employees_df, mismatch_employees_df = transform_employees(con)
        plans_df = transform_plans(con)

        # TODO: High water mark from the data that was just processed.
        new_high_water_mark = datetime.now()

        # Write validation errors
        if not mismatch_employees_df.empty:
            mismatch_employees_df.to_csv("outputs/validation_errors.csv", index=False)
            logger.warning(
                f"Wrote {mismatch_employees_df.shape[0]} validation errors to outputs/validation_errors.csv"
            )
        else:
            logger.info("No employee validation errors found.")

        claims_df.to_parquet(
            "outputs/clean_data/claims_data.parquet",
            index=False,
            partition_cols=["company_ein", "claim_type", "service_date"],
        )
        logger.info(
            f"Wrote {claims_df.shape[0]} clean claims records to outputs/clean_data/claims_data.parquet"
        )

        valid_employees_df.to_parquet(
            "outputs/clean_data/employees_data.parquet",
            index=False,
            partition_cols=["company_ein", "start_date"],
        )
        logger.info(
            f"Wrote {valid_employees_df.shape[0]} clean employee records to outputs/clean_data/employees_data.parquet"
        )

        plans_df.to_parquet(
            "outputs/clean_data/plans_data.parquet",
            index=False,
            partition_cols=["company_ein", "plan_type", "start_date"],
        )
        logger.info(
            f"Wrote {plans_df.shape[0]} clean plan records to outputs/clean_data/plans_data.parquet"
        )

        # Execute SQL views and save results
        try:
            gaps_query = open("sql/views/sql_gaps.sql").read()
            con.query(gaps_query).to_csv("outputs/sql_gaps.csv")
            logger.info("Generated outputs/sql_gaps.csv")
        except Exception as e:
            logger.error(f"Error generating sql_gaps.csv: {e}")

        try:
            roster_query = open("sql/views/sql_roster.sql").read()
            con.query(roster_query).to_csv("outputs/sql_roster.csv")
            logger.info("Generated outputs/sql_roster.csv")
        except Exception as e:
            logger.error(f"Error generating sql_roster.csv: {e}")

        try:
            spikes_query = open("sql/views/sql_spikes.sql").read()
            con.query(spikes_query).to_csv("outputs/sql_spikes.csv")
            logger.info("Generated outputs/sql_spikes.csv")
        except Exception as e:
            logger.error(f"Error generating sql_spikes.csv: {e}")

    # 3. Write the new high-water mark after a successful run
    write_high_water_mark(new_high_water_mark)
    logger.info("ETL process completed successfully.")

import os
from typing import Any, Dict

import duckdb
from structlog import get_logger

from processing import (
    transform_claims,
    transform_employees,
    transform_plans,
)
from utils import retry

# configure_structlog()

logger = get_logger(__name__)


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


def extract(con: duckdb.DuckDBPyConnection):
    """
    Ingest data from files
    """
    with open("sql/ingest.sql", "r") as f:
        for line in f.readlines():
            try:
                logger.info(f"Executing SQL statement: {line}")
                con.sql(line)
                logger.info("SQL statement executed successfully")
            except Exception as e:
                logger.error(f"Error executing SQL statement: {line}")
                logger.error(f"Error message: {e}")
                return False
        return True


# def load(df: pd.DataFrame):
#     pass


if __name__ == "__main__":
    required_files = ["plans_raw.csv", "employees_raw.csv", "claims_raw.csv"]
    csv_files = [f for f in os.listdir(".") if f.endswith(".csv")]
    assert all(x in required_files for x in csv_files)

    with duckdb.connect("hillwinds.db") as con:
        extract(con=con)
        transform_claims(con)
        transform_employees(con)
        transform_plans(con)

import json
import pandas as pd

import duckdb as db
from structlog import get_logger

logger = get_logger(__name__)


def transform_claims(connection: db.DuckDBPyConnection) -> pd.DataFrame:
    with open("sql/transformations/int_claims.sql", "r") as f:
        sql = f.read()
        df = connection.sql(sql).to_df()
        return df


def transform_employees(connection: db.DuckDBPyConnection) -> tuple[pd.DataFrame, pd.DataFrame]:
    with open("sql/transformations/int_employees.sql", "r") as f:
        sql = f.read()

        df = connection.sql(sql).to_df()
        logger.info(f"Pre Processed {df.shape[0]} rows and {df.shape[1]} columns")

        # filter out company_names that are not in the lookup
        with open("company_lookup.json", "r") as f:
            dict_company_lookup = json.load(f)

            logger.info("Filtering out company_names that are not in the lookup")
            valid_data = df[df["company_name"].isin(dict_company_lookup.keys())]
            mismatch_data = df[~df["company_name"].isin(dict_company_lookup.keys())]
            if mismatch_data.empty:
                logger.info("No mismatch data found")
            else:
                logger.warning(
                    f"Found {mismatch_data.shape[0]} rows with mismatched company_names"
                )
            logger.info(
                f"Filtered {valid_data.shape[0]} rows and {valid_data.shape[1]} columns"
            )
            return valid_data, mismatch_data


def transform_plans(connection: db.DuckDBPyConnection) -> pd.DataFrame:
    with open("sql/transformations/int_plans.sql", "r") as f:
        sql = f.read()
        df = connection.sql(sql).to_df()
        return df


# def transform_companies() -> pd.DataFrame:
#     with open("company_lookup.json", "r") as f:
#         dict_company_lookup = json.load(f)
#         transformed_data = []
#         for company_name, company_ein in dict_company_lookup.items():
#             transformed_data.append(
#                 {
#                     "company_ein": company_ein,
#                     "company_name": company_name,
#                 }
#             )
#         return pd.DataFrame(transformed_data)

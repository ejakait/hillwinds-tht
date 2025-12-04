import json

import duckdb as db
from structlog import get_logger

logger = get_logger(__name__)

CLEAN_DATA_PATH = "outputs/clean_data"


def transform_claims(connection: db.DuckDBPyConnection):
    with open("sql/transformations/int_claims.sql", "r") as f:
        sql = f.read()
        df = connection.sql(sql).to_df()
        df.to_parquet(
            f"{CLEAN_DATA_PATH}/claims_data.parquet",
            partition_cols=["company_ein", "claim_type", "service_date"],
        )


def transform_employees(connection: db.DuckDBPyConnection):
    with open("sql/transformations/int_employees.sql", "r") as f:
        sql = f.read()

        df = connection.sql(sql).to_df()
        logger.info(f"Pre Proceesed {df.shape[0]} rows and {df.shape[1]} columns")

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

                mismatch_data.to_csv("outputs/validation_errors.csv", index=False)
            logger.info(
                f"Filtered {valid_data.shape[0]} rows and {valid_data.shape[1]} columns"
            )
            valid_data.to_parquet(
                f"{CLEAN_DATA_PATH}/employees_data.parquet",
                partition_cols=["company_ein", "start_date"],
            )

            return valid_data


def transform_plans(connection: db.DuckDBPyConnection):
    with open("sql/transformations/int_plans.sql", "r") as f:
        sql = f.read()
        df = connection.sql(sql).to_df()
        df.to_parquet(
            f"{CLEAN_DATA_PATH}/plans_data.parquet",
            partition_cols=["company_ein", "plan_type", "start_date"],
        )


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

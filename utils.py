import json
import time

import pandas as pd
from structlog import get_logger

logger = get_logger(__name__)


def map_company_name_to_ein(data: pd.DataFrame) -> pd.DataFrame:
    with open("company_lookup.json", "r") as file:
        company_lookup_raw = json.load(file)
        company_lookup = {v: k for k, v in company_lookup_raw.items()}
        data["company_name"] = data["company_ein"].map(lambda x: company_lookup[x])

    return data


def retry(func):
    def wrapper(*args, **kwargs):
        max_attempts = 3
        for attempt in range(max_attempts):
            try:
                time.sleep(3)
                return func(*args, **kwargs)
            except Exception as e:
                if attempt == max_attempts - 1:
                    raise e
                logger.warning(f"Attempt {attempt + 1} failed: {e}")

    return wrapper

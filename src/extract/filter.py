import logging
import time
from datetime import timedelta
from pydantic import ValidationError
from utils import fetch_api_json

import pandas as pd

logger = logging.getLogger(__name__)


def fetch_relic_chunk(base_url: str, endpoint: str, params: dict):
    """Fetches all data from Relic API in chunks of 100/request (API limit)"""
    start = 1
    chunk = params["chunk_size"]
    logger.info(f"Processing data in chunks of {chunk} from {endpoint}")
    while True:
        params["start"] = start
        response = fetch_api_json(base_url, endpoint, params)

        if not response:
            break
        api_end = response["rankTotal"] + chunk
        if start > api_end:
            break
        logger.info(f"Processing chunk {start}/{api_end}")

        yield response
        start += chunk
        time.sleep(0.2)


def validate_json_schema(json_data, validation_schema):
    try:
        data = json_data
        validated_data = validation_schema.model_validate(data)
        return validated_data
    except ValidationError as e:
        logger.error(f"Validation Error: {e}")
        return []


def validate_parquet_schema(content, validation_schema):
    df = pd.read_parquet(content)
    records = df.to_dict(orient="records")
    for record in records:
        try:
            validation_schema.model_validate(record)
        except ValidationError as e:
            logger.error(f"Validation error: {e}")

    # Reset the pointer to start of file:
    content.seek(0)
    return content


def generate_weekly_queries(start_date, end_date):
    # Move start date to the next Sunday if it's not already a Sunday
    while start_date.weekday() != 6:
        start_date += timedelta(days=1)

    # Move end date to the previous Saturday if it's not already a Saturday
    while end_date.weekday() != 5:
        end_date -= timedelta(days=1)

    queries = []
    print(f"Finding all files between {start_date} and {end_date}.")
    while start_date <= end_date:
        # Calculate the end date of the current week (the next Saturday)
        end_date_saturday = start_date + timedelta(days=6)
        query = {
            "dated": start_date,
            "query_str": f"{start_date.strftime('%Y-%m-%d')}_{end_date_saturday.strftime('%Y-%m-%d')}",
        }
        queries.append(query)
        # Move to the next week
        start_date += timedelta(days=7)
    return queries


def create_stats_endpoints(extract_file: str, weekly_querys: list):
    endpoints = []
    for weekly_query in weekly_querys:
        result_dated = f"{weekly_query['dated']}_{extract_file}"
        result_query = f"{weekly_query['query_str']}/{extract_file}.parquet"
        endpoints.append({"file_date": result_dated, "endpoint_str": result_query})
    logger.info(f"{len(endpoints)} found.")
    return endpoints

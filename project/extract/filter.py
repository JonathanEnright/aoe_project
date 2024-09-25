import json
import logging
import time
from typing import List, Dict
from datetime import timedelta
from pydantic import ValidationError
from utils import fetch_api_file
import pandas as pd

logger = logging.getLogger(__name__)


def fetch_relic_chunk(base_url: str, endpoint: str, params: Dict) -> List:
    """Fetches all data from Relic API in chunks of 100/request (API limit)"""
    start = 1
    response_list = []
    while True:
        params["start"] = start
        logger.info(f"processing chunk {start} ")
        response = fetch_api_file(base_url, endpoint, params)

        if not response:
            break
        # Assume end of data if response bytes is <1KB.
        if len(response.getvalue()) <= 1024:
            logger.info(f"Assumed end of data reached!")
            break

        response_list.append(response)
        start += params["chunk_size"]
        time.sleep(1)
    return response_list


def validate_json_schema(content, validation_schema):
    try:
        data = json.load(content)
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
            logger.error("Validation error:", e)

    # Reset the pointer to start of file:
    content.seek(0)
    return content


def generate_weekly_queries(start_date, end_date):
    sunday_start = start_date - timedelta(days=start_date.weekday() + 1)
    saturday_end = end_date + timedelta(days=(5 - end_date.weekday() + 7) % 7)

    queries = []
    current = sunday_start
    logger.info(f"Finding all files between {sunday_start} and {saturday_end}.")
    while current <= saturday_end:
        week_end = current + timedelta(days=6)
        query = f"{current.strftime('%Y-%m-%d')}_{week_end.strftime('%Y-%m-%d')}"
        result = {"dated": current, "query_str": query}
        queries.append(result)
        current += timedelta(days=7)

    return queries


def create_stats_endpoints(extract_file: str, weekly_querys: list):
    endpoints = []
    for weekly_query in weekly_querys:
        result_query = f"{weekly_query['query_str']}/{extract_file}"
        result_dated = f"{weekly_query['dated']}_{extract_file}"
        endpoints.append({"file_date": result_dated, "endpoint_str": result_query})
    logger.info(f"{len(endpoints)} found.")
    return endpoints

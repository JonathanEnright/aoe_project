import json
import logging
import time
import requests
from typing import List
from datetime import date
from pydantic import ValidationError
from utils import fetch_api_file
from extract import ApiSchema, WeeklyDump, RelicResponse

logger = logging.getLogger(__name__)


def extract_db_dumps_metadata(config) -> List[WeeklyDump]:
    """Extracts valid database dumps from the API."""
    logger.info("Downloading API metadata file.")
    metadata_file = fetch_api_file(config.base_url, config.db_endpoint)

    try:
        metadata_content = metadata_file.read()
        source_schema_json = json.loads(metadata_content)

        # Validate to our Pydantic 'ApiSchema' definition
        source_schema = ApiSchema.model_validate(source_schema_json)

        # Filter to pull only populated files between date range
        valid_dumps = filter_valid_dumps(
            source_schema, config.run_date, config.run_end_date
        )
    except ValidationError as e:
        logger.error(f"Schema validation failed with error:\n{e}")
        return []

    return valid_dumps


def filter_valid_dumps(
    source_schema: ApiSchema, run_date: date, run_end_date: date
) -> List[WeeklyDump]:
    """Filters the database dumps based on start date and number of matches."""
    logger.info("Filtering valid database dumps based on date and match count.")
    valid_dumps = []
    for weekly_dump in source_schema.db_dumps:
        valid_dump = (
            weekly_dump.start_date >= run_date
            and weekly_dump.start_date < run_end_date
            and weekly_dump.num_matches != 0
        )
        if valid_dump:
            valid_dumps.append(weekly_dump)
    logger.info(f"Found {len(valid_dumps)} valid dumps.")
    return valid_dumps


def fetch_relic_data(config, WAIT_SEC: int = 1) -> RelicResponse:
    """Submit GET requests in chunks of 100 to obtain player & leaderboard data from Relic API"""
    api_url = config.relic_base_url + config.relic_endpoint
    params = config.relic_params
    chunk_size = params["chunk_size"]

    combined_stat_groups = []
    combined_leaderboard_stats = []
    start = 1

    while True:
        params["start"] = start
        response = requests.get(api_url, params=params)
        response.raise_for_status()
        logger.info(f"processing chunk {start} ")
        time.sleep(WAIT_SEC)

        try:
            data = response.json()
            validated_data = RelicResponse(**data)

            # Append new data
            combined_stat_groups.extend(validated_data.statGroups)
            combined_leaderboard_stats.extend(validated_data.leaderboardStats)

            # Check if we've reached the end of the leaderboard
            if len(validated_data.statGroups) < chunk_size:
                break

            start += chunk_size

        except ValidationError as e:
            logger.info(f"Validation Error: {e}")
            break

    # Construct the final response object
    relic_data = RelicResponse(
        result=validated_data.result,
        statGroups=combined_stat_groups,
        leaderboardStats=combined_leaderboard_stats,
        rankTotal=validated_data.rankTotal,
    )

    return relic_data

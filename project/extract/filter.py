import json
import logging
import time
from typing import List, Tuple, Generator
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


def fetch_relic_chunk(config, start: int) -> Tuple[List, List]:
    """
    Fetches a single chunk of player & leaderboard data from Relic API (limit 100/request).
    """
    params = config.relic_params
    params["start"] = start
    logger.info(f"processing chunk {start} ")
    response = fetch_api_file(config.relic_base_url, config.relic_endpoint, params)

    try:
        data = json.load(response)
        validated_data = RelicResponse(**data)
        return validated_data.statGroups, validated_data.leaderboardStats
    except ValidationError as e:
        logger.info(f"Validation Error: {e}")
        return [], []


def chunk_relic_data(config) -> Generator[RelicResponse, None, None]:
    """
    Generates RelicResponse objects, each containing at most 5000 rows of data.
    These objects will be transformed to seperate JSON files.
    This is necessary to overcome the 16MB VARIANT field limit in Snowflake.
    """
    chunk_size = config.relic_params["chunk_size"]
    start = 1

    current_relic_data = RelicResponse(statGroups=[], leaderboardStats=[])
    while True:
        stat_groups, leaderboard_stats = fetch_relic_chunk(config, start)

        if not stat_groups:  # Empty response, likely end of data
            break

        for group, leaderboard in zip(stat_groups, leaderboard_stats):
            current_relic_data.statGroups.append(group)
            current_relic_data.leaderboardStats.append(leaderboard)
            if len(current_relic_data.statGroups) >= config.relic_max_rows:
                yield current_relic_data
                current_relic_data = RelicResponse(statGroups=[], leaderboardStats=[])

        if len(stat_groups) < chunk_size:  # Last chunk
            if current_relic_data.statGroups:
                yield current_relic_data
            break

        start += chunk_size
        time.sleep(1)

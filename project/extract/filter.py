import json
import logging
from typing import List
from datetime import date
from pydantic import ValidationError
from utils import fetch_api_file
from extract.models import ApiSchema, WeeklyDump

logger = logging.getLogger(__name__)


def extract_metadata(config) -> List[WeeklyDump]:
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

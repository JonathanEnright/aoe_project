from pydantic import BaseModel, ValidationError
from utils import Config, ApiDownloader
import logging
import json
from typing import Dict, List, Any
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Define expected schema for db_dumps API endpoint via Pydantic models
# ----------------------------------------------------------------------------
class WeeklyDump(BaseModel):
    start_date: datetime
    end_date: datetime
    num_matches: int
    matches_url: str
    players_url: str
    match_checksum: str
    player_checksum: str


class ApiSchema(BaseModel):
    db_dumps: List[WeeklyDump]
    total_matches: int
    total_players: int
# ----------------------------------------------------------------------------

def filter_valid_dumps(
    source_schema: ApiSchema, run_date: datetime, end_date: datetime
) -> List[WeeklyDump]:
    """Filters the database dumps based on start date and number of matches."""
    logger.info("Filtering valid database dumps based on date and match count.")
    valid_dumps = []
    for weekly_dump in source_schema.db_dumps:
        valid_dump = (
            weekly_dump.start_date >= run_date
            and weekly_dump.start_date < end_date
            and weekly_dump.num_matches != 0
        )
        if valid_dump:
            valid_dumps.append(weekly_dump)
    logger.info(f"Found {len(valid_dumps)} valid dumps.")
    return valid_dumps


def download_data(
    downloader: ApiDownloader, valid_dumps: List[WeeklyDump], data_dir: str = "data"
):
    """Downloads match and player data for each valid dump."""
    logger.info("Downloading match and player data for valid dumps.")
    for dump in valid_dumps:
        matches_endpoint = dump.matches_url
        players_endpoint = dump.players_url
        dated = dump.start_date.strftime('%Y-%m-%d')

        # Download 'matches' and 'players' parquet data with date prefix
        logger.info(f"Downloading matches for {dated} from {matches_endpoint}")
        downloader.download(matches_endpoint, f"{data_dir}/{dated}_matches.parquet")
        logger.info(f"Downloading players for {dated} from {players_endpoint}")
        downloader.download(players_endpoint, f"{data_dir}/{dated}_players.parquet")
    logger.info("Data download complete.")


def main():
    config = Config("config.yaml")

    # 1. Pull API metadata files and save locally
    logger.info("Downloading API metadata file.")
    downloader = ApiDownloader(config.base_url)
    downloader.download(config.db_endpoint, config.metadata_file)

    # 2. Validate schema hasnt changed and find latest valid file metadata.
    logger.info("Validating schema and filtering valid files.")
    try:
        with open(config.metadata_file, "r") as json_file:
            source_schema_json = json.load(json_file)
        
        # Validate to to our pydantic 'ApiSchema' definition
        source_schema = ApiSchema.model_validate(source_schema_json)

        # Filter to pull only populated files between date range
        valid_files = filter_valid_dumps(
            source_schema, config.run_date, config.end_date
        )
    except ValidationError as e:
        logger.error(f"Schema Validation failed with error:\n{e}")
        return

    # 3. Download data files for matches and players.
    download_data(downloader, valid_files)


if __name__ == "__main__":
    main()
    logger.info("Script Complete.")

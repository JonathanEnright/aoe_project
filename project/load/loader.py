import logging
import json
import io
from typing import List
from utils import create_s3_session, upload_to_s3, fetch_api_file
from extract import WeeklyDump, RelicResponse

logger = logging.getLogger(__name__)


def load_db_dumps_data(valid_dumps: List[WeeklyDump], config):
    """Loads valid dumps into S3."""
    logger.info("Authenticating to S3.")
    s3 = create_s3_session()

    logger.info("Downloading match and player data for valid dumps.")
    for dump in valid_dumps:
        matches_endpoint = dump.matches_url
        players_endpoint = dump.players_url
        dated = dump.start_date.strftime("%Y-%m-%d")

        # Download 'matches' and 'players' parquet data with date prefix
        logger.info(f"Downloading matches for {dated} from {matches_endpoint}")
        match_file = fetch_api_file(config.base_url, matches_endpoint)
        upload_to_s3(s3, match_file, config.bucket, f"{dated}_matches.parquet")

        logger.info(f"Downloading players for {dated} from {players_endpoint}")
        players_file = fetch_api_file(config.base_url, players_endpoint)
        upload_to_s3(s3, players_file, config.bucket, f"{dated}_players.parquet")
    logger.info("Data loading complete.")


def load_relic_api_data(relic_data: RelicResponse, config):
    """Loads relic data into s3."""
    logger.info("Authenticating to S3.")
    s3 = create_s3_session()

    json_data = json.dumps(
        relic_data.model_dump(include={"statGroups", "leaderboardStats"}), indent=4
    )
    file_obj = io.BytesIO(json_data.encode("utf-8"))
    upload_to_s3(s3, file_obj, config.bucket, config.relic_file_name)

    logger.info("Data loading complete.")

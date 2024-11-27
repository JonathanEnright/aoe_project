from utils import Config, timer, fetch_api_file, create_s3_session
from extract import (
    Players,
    generate_weekly_queries,
    create_stats_endpoints,
    validate_parquet_schema,
)
from load import load_parquet_data
import logging
import os
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Get the directory of the current script
script_dir = Path(__file__).resolve().parent

YAML_CONFIG = os.path.join(script_dir, "config.yaml")


@timer
def main(*args, **kwargs):
    config = Config(YAML_CONFIG)

    # Setup:
    s3 = create_s3_session()
    _fn = config.players_fn_suffix
    _file_dir = config.players_folder_name
    _validation_schema = Players

    _start_date = config.run_date
    _end_date = config.run_end_date
    _base_url = config.stats_base_url + config.stats_dir_url
    _params = None
    _s3_bucket = config.bucket

    # Pre-extract phase
    weekly_querys = generate_weekly_queries(_start_date, _end_date)
    endpoints = create_stats_endpoints(_fn, weekly_querys)

    for i, endpoint in enumerate(endpoints):
        endpoint_url = endpoint["endpoint_str"]
        dated_filename = endpoint["file_date"]
        file_dir = f"{_file_dir}/{dated_filename.split('_')[0]}"

        # Extract phase
        content = fetch_api_file(_base_url, endpoint_url, _params)

        if content is None:
            logger.error(
                f"Failed to fetch data after 3 attempts for endpoint: {endpoint_url}"
            )
            continue  # Skip to the next iteration of the loop

        # Validate phase
        validated_data = validate_parquet_schema(content, _validation_schema)

        # Load phase
        load_parquet_data(validated_data, file_dir, dated_filename, _s3_bucket, s3)
        logger.info(f"{i+1}/{len(endpoints)} loaded.")
    logger.info("Script complete.")


if __name__ == "__main__":
    main()

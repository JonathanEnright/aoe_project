from utils import Config, timer, fetch_api_json, create_s3_session
from extract import validate_json_schema, ApiSchema
from load import load_json_data
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
    _base_url = config.stats_base_url
    _endpoint = config.metadata_endpoint
    _params = None
    _validation_schema = ApiSchema
    _fn = f"{config.run_end_date}_{config.metadata_fn_suffix}"
    _file_dir = config.metadata_folder_name
    _s3_bucket = config.bucket

    try:
        # Extract phase
        logger.info("Starting data extraction.")
        json_data = fetch_api_json(_base_url, _endpoint, _params)

        if json_data is None:
            logger.error(
                f"Failed to fetch data after 3 attempts for endpoint: {_endpoint}"
            )
            raise

        # Validate phase
        logger.info("Validating data.")
        validated_data = validate_json_schema(json_data, _validation_schema)

        # Load phase
        logger.info("Starting data loading.")
        load_json_data(validated_data, _file_dir, _fn, _s3_bucket, s3)

        logger.info("ELT process completed successfully.")
    except Exception as e:
        logger.error(f"ELT process failed: {e}")
        raise
    logger.info("Script complete.")


if __name__ == "__main__":
    main()

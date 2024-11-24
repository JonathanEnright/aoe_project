from utils import Config, timer, create_s3_session
from extract import validate_json_schema, fetch_relic_chunk
from extract import RelicResponse
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
    _base_url = config.relic_base_url
    _endpoint = config.relic_endpoint
    _params = config.relic_params
    _validation_schema = RelicResponse
    _fn = config.relic_fn_suffix
    _file_dir = config.relic_folder_name
    _s3_bucket = config.bucket

    # Extract phase
    logger.info("Starting data extraction.")
    content_chunk = fetch_relic_chunk(_base_url, _endpoint, _params)

    for i, json_data in enumerate(content_chunk):
        fn = f"{_fn}_{i+1}"

        # Validate phase
        validated_data = validate_json_schema(json_data, _validation_schema)

        # Load phase
        load_json_data(validated_data, _file_dir, fn, _s3_bucket, s3)
        logger.info("Script complete.")


if __name__ == "__main__":
    main()

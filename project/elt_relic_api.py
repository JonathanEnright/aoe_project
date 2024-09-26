from utils import Config, timer, create_s3_session
from extract import validate_json_schema, fetch_relic_chunk
from extract import RelicResponse
from load import load_json_data
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@timer
def main():
    config = Config("config.yaml")

    # Setup:
    s3 = create_s3_session()
    _base_url = config.relic_base_url
    _endpoint = config.relic_endpoint
    _params = config.relic_params
    _validation_schema = RelicResponse
    _output_prefix = config.relic_file_name
    _s3_bucket = config.bucket

    # Extract phase
    logger.info("Starting data extraction.")
    content_chunk = fetch_relic_chunk(_base_url, _endpoint, _params)

    for i, json_data in enumerate(content_chunk):
        file_name_prefix = f"{_output_prefix}_{i+1}"

        # Validate phase
        validated_data = validate_json_schema(json_data, _validation_schema)

        # Load phase
        load_json_data(validated_data, file_name_prefix, _s3_bucket, s3)


if __name__ == "__main__":
    main()
    logger.info("Script complete.")

from utils import Config, timer, fetch_api_json, create_s3_session
from extract import validate_json_schema
from extract import ApiSchema
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
    _base_url = config.stats_base_url
    _endpoint = config.metadata_endpoint
    _params = None
    _validation_schema = ApiSchema
    _output_prefix = f"{config.run_end_date}_{config.metadata_output_prefix}"
    _s3_bucket = config.bucket

    # Extract phase
    logger.info("Starting data extraction.")
    json_data = fetch_api_json(_base_url, _endpoint, _params)

    # Validate phase
    validated_data = validate_json_schema(json_data, _validation_schema)

    # Load phase
    logger.info("Starting data loading.")
    s3 = load_json_data(validated_data, _output_prefix, _s3_bucket, s3)
    logger.info("Script complete.")


if __name__ == "__main__":
    main()

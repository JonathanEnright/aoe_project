from utils import Config, timer
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
    s3 = None
    _base_url = config.relic_base_url
    _endpoint = config.relic_endpoint
    _params = config.relic_params
    _validation_schema = RelicResponse
    _output_prefix = config.relic_file_name
    _s3_bucket = config.bucket

    # Extract phase
    logger.info("Starting data extraction.")
    content_chunk = fetch_relic_chunk(_base_url, _endpoint, _params)
    
    for i,content in enumerate(content_chunk):
        file_name_prefix = f"{_output_prefix}_{i+1}"
        
        # Validate phase
        validated_data = validate_json_schema(content, _validation_schema)
        
        # Load phase
        s3 = load_json_data(validated_data, file_name_prefix, _s3_bucket, s3)
        logger.info(f"{i+1}/{len(content_chunk)} loaded.")


if __name__ == "__main__":
    main()
    logger.info("Script complete.")

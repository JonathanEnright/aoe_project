from utils import Config, timer, fetch_api_file
from extract import generate_weekly_queries, create_stats_endpoints, validate_parquet_schema
from extract import Matches
from load import load_parquet_data
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
    _stat_file_name = config.stats_matches
    _validation_schema = Matches

    s3 = None
    _start_date = config.run_date
    _end_date = config.run_end_date
    _base_url = config.stats_base_url + config.stats_dir_url
    _params = None  
    _s3_bucket = config.bucket


    # Pre-extract phase
    weekly_querys = generate_weekly_queries(_start_date, _end_date)
    endpoints = create_stats_endpoints(_stat_file_name, weekly_querys)

    for i,endpoint in enumerate(endpoints):
        endpoint_url = endpoint['endpoint_str']
        dated_filename = endpoint['file_date']
        
        # Extract phase
        content = fetch_api_file(_base_url, endpoint_url, _params)

        # Validate phase
        validated_data = validate_parquet_schema(content, _validation_schema)
        
        # Load phase
        s3 = load_parquet_data(validated_data, dated_filename, _s3_bucket, s3)
        logger.info(f"{i+1}/{len(endpoints)} loaded.")


if __name__ == "__main__":
    main()
    logger.info("Script complete.")

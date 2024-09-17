from utils import Config, timer
from extract import chunk_relic_data
from load import load_relic_api_data
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@timer
def main():
    config = Config("config.yaml")

    # Extract phase
    logger.info("Starting data extraction.")
    relic_data = list(chunk_relic_data(config))

    # Load phase
    logger.info("Starting data loading.")
    load_relic_api_data(relic_data, config)


if __name__ == "__main__":
    main()
    logger.info("Script complete.")

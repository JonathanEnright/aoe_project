from utils import Config
from extract import extract_db_dumps_metadata
from load import load_db_dumps_data
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def main():
    config = Config("config.yaml")

    # Extract phase
    logger.info("Starting data extraction.")
    valid_dumps = extract_db_dumps_metadata(config)

    # Load phase
    logger.info("Starting data loading.")
    load_db_dumps_data(valid_dumps, config)


if __name__ == "__main__":
    main()
    logger.info("Script complete.")

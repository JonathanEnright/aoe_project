from utils import Config
from extract.filter import extract_metadata
from load.loader import load_data
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
    valid_dumps = extract_metadata(config)

    # Load phase
    logger.info("Starting data loading.")
    load_data(valid_dumps, config)


if __name__ == "__main__":
    main()
    logger.info("Script complete.")

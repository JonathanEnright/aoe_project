from utils import Config, sf_connect
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

# Set up script to update only 'dev' file date ranges.
# 'prod' runs will be bring in all data by default,
#  unless control table manually overridden or updated here (ENV = "prod").
ENV = "dev"


def update_load_master(connection, env: str, from_date: str, to_date: str):
    query = f"""
    UPDATE load_master
    SET 
        load_start_date = '{from_date}'
        ,load_end_date = '{to_date}'
    WHERE
        environment = '{env}' 
    ;
    """
    cursor = connection.cursor()
    cursor.execute(query)
    result = cursor.fetchone()[0]
    cursor.close()
    return result


def main(*args, **kwargs):
    config = Config(YAML_CONFIG)
    run_date_from = config.run_date
    run_date_to = config.run_end_date

    connection = sf_connect(db="aoe", schema="control")
    update_load_master(connection, ENV, run_date_from, run_date_to)
    connection.close()
    logger.info("Script complete.")


if __name__ == "__main__":
    main()

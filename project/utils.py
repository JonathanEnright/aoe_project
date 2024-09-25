import yaml
from datetime import datetime, timedelta
import requests
import os
import boto3
from dotenv import load_dotenv
from typing import Dict, Optional, BinaryIO
import io
import time
import logging


logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

# -----------------------------------------------------------------------------
# Classes
# -----------------------------------------------------------------------------


class Config:
    def __init__(self, yaml_file: str):
        with open(yaml_file, "r") as f:
            self.__dict__.update(yaml.safe_load(f))
        self.run_date = self.parse_date(self.backdate_days_start, self.date_format)
        self.run_end_date = self.parse_date(self.backdate_days_end, self.date_format)
        self.db_endpoint_url = "/".join([self.base_url, self.db_endpoint])

    @staticmethod
    def parse_date(backdate_days: int, date_format: str):
        return (datetime.now() - timedelta(days=backdate_days)).date()


# -----------------------------------------------------------------------------
# Functions
# -----------------------------------------------------------------------------


def fetch_api_file(base_url: str, endpoint: str, params: Optional[Dict] = None) -> BinaryIO | None:
    """Fetches a file from an API endpoint and returns it as a BytesIO object."""
    try:
        url = base_url + endpoint
        response = requests.get(url, params=params)
        response.raise_for_status()
        content = io.BytesIO(response.content) 
        return content
    except requests.RequestException as e:
        logger.error(f"Error fetching data: {e}")
        return None



def create_s3_session(s3=None):
    if s3 == None:
        logger.info("Authenticating to S3.")
        s3 = boto3.client(
            "s3",
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_REGION"),
        )
    return s3


def upload_to_s3(s3, data, bucket_name, path_key):
    try:
        s3.upload_fileobj(data, bucket_name, path_key)
        logger.info(
            f"File '{path_key}' uploaded to S3 bucket '{bucket_name}' successfully!"
        )
    except Exception as e:
        logger.error(f"Error uploading file: {e}")


def timer(func):
    """A simple timer decorator to record how long a function took to run."""

    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        elapsed_time = end_time - start_time
        logger.info(
            f"Function '{func.__name__}' took {elapsed_time:.1f} seconds to run."
        )
        return result

    return wrapper

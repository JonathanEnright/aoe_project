import yaml
from datetime import datetime, timedelta
import requests
import os
import boto3
from dotenv import load_dotenv
import io

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


def fetch_api_file(base_url: str, endpoint: str):
    url = base_url + endpoint
    response = requests.get(url)
    response.raise_for_status()
    return io.BytesIO(response.content)


def create_s3_session():
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
        print(f"File '{path_key}' uploaded to S3 bucket '{bucket_name}' successfully!")
    except Exception as e:
        print(f"Error uploading file: {e}")

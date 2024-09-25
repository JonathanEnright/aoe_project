import logging
import io
from typing import List
from utils import create_s3_session, upload_to_s3


logger = logging.getLogger(__name__)

def load_json_data(data_list, file_name_prefix, bucket, s3):
    """Loads Pydantic model as json files in S3."""

    s3 = create_s3_session(s3)
    file_name = f"{file_name_prefix}.json"
    json_data = data_list.model_dump_json(indent=4)
    file_obj = io.BytesIO(json_data.encode("utf-8"))
    upload_to_s3(s3, file_obj, bucket, file_name)
    return s3


def load_parquet_data(data, file_name, bucket, s3):
    """Loads parquet data directly into S3 bucket."""
    s3 = create_s3_session(s3)
    upload_to_s3(s3, data, bucket, file_name)
    return s3

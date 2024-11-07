import logging
import io
from utils import upload_to_s3


logger = logging.getLogger(__name__)


def load_json_data(model, file_name_prefix, bucket, s3):
    """Loads Pydantic model as json files in S3."""
    file_name = f"{file_name_prefix}.json"
    json_data = model.model_dump_json(indent=4)
    json_to_bytes = io.BytesIO(json_data.encode("utf-8"))
    with json_to_bytes as file_obj:
        upload_to_s3(s3, file_obj, bucket, file_name)


def load_parquet_data(data, file_name, bucket, s3):
    """Loads parquet data directly into S3 bucket."""
    with data as file_obj:
        upload_to_s3(s3, file_obj, bucket, file_name)

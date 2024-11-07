import pytest
from datetime import datetime, date
# import pandas as pd
import io
from pydantic import BaseModel
# from unittest.mock import Mock, patch

from extract.filter import *
from load.loader import *


@pytest.fixture
def sample_dates():
    return {"start_date": date(2023, 1, 1), "end_date": date(2023, 1, 14)}


# Test Data and Fixtures
@pytest.fixture
def sample_validation_schema():
    class TestSchema(BaseModel):
        id: int
        name: str

    return TestSchema


# # Tests for validate_parquet_schema
# def test_validate_parquet_schema(sample_validation_schema):
#     # Create a sample DataFrame
#     df = pd.DataFrame([{"id": 1, "name": "test1"}, {"id": 2, "name": "test2"}])

#     # Create a parquet file in memory
#     parquet_buffer = io.BytesIO()
#     df.to_parquet(parquet_buffer)
#     parquet_buffer.seek(0)

#     result = validate_parquet_schema(parquet_buffer, sample_validation_schema)
#     assert result is not None


# Tests for validate_json_schema
def test_validate_json_schema_valid(sample_validation_schema):
    valid_data = {"id": 1, "name": "test"}
    result = validate_json_schema(valid_data, sample_validation_schema)
    assert result.id == 1
    assert result.name == "test"


def test_validate_json_schema_invalid(sample_validation_schema):
    invalid_data = {"id": "not_an_integer", "name": "test"}
    result = validate_json_schema(invalid_data, sample_validation_schema)
    assert result == []


def test_generate_weekly_queries(sample_dates):
    queries = generate_weekly_queries(
        sample_dates["start_date"], sample_dates["end_date"]
    )
    assert len(queries) == 2  # Should generate 2 weeks worth of queries
    assert all("dated" in q and "query_str" in q for q in queries)
    assert queries[0]["query_str"].startswith("2023-01-01")


def test_create_stats_endpoints():
    weekly_queries = [{"dated": date(2023, 1, 1), "query_str": "2023-01-01_2023-01-07"}]
    extract_file = "stats.json"
    endpoints = create_stats_endpoints(extract_file, weekly_queries)
    assert len(endpoints) == 1
    assert endpoints[0]["endpoint_str"] == "2023-01-01_2023-01-07/stats.json"
    assert endpoints[0]["file_date"] == "2023-01-01_stats.json"

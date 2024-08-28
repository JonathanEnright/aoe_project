import yaml
import operator as op
from datetime import datetime, timedelta
import requests
import json
import pathlib
from jsonschema import validate, ValidationError, SchemaError

# -----------------------------------------------------------------------------
# Functions
# -----------------------------------------------------------------------------


def valid_schema(source: dict, target: dict) -> str:
    """Reads a source json file against a target yaml file to validate schema is consistent."""
    try:
        validate(instance=source, schema=target)
        return True
    except (ValidationError, SchemaError) as e:
        print(f"Schema Validation failed with error:\n{e}")
        return False


# -----------------------------------------------------------------------------
# Classes
# -----------------------------------------------------------------------------


class Config:
    def __init__(self, yaml_file: str):
        with open(yaml_file, "r") as f:
            self.__dict__.update(yaml.safe_load(f))
        self.run_date = self.parse_date(self.backdate_days_start, self.date_format)
        self.end_date = self.parse_date(self.backdate_days_end, self.date_format)
        self.db_endpoint_url = "/".join([self.base_url, self.db_endpoint])

    @staticmethod
    def parse_date(backdate_days: int, date_format: str) -> str:
        return (datetime.now() - timedelta(days=backdate_days)).strftime(date_format)


class ApiDownloader:
    def __init__(self, base_url: str):
        self.base_url = base_url

    def download(self, endpoint: str, output_path: str):
        """
        Downloads the content from the specified endpoint and saves it locally.

        Args:
            endpoint (str): The endpoint to download from (e.g., "/api/db_dumps.json").
            output_path (str): The local path to save the downloaded content.

        Example:
            downloader = ApiDownloader('https://aoestats.io')\n
            downloader.download('/api/db_dumps.json', 'prod/metadata.json')\n
            downloader.download(\n
                '/media/db_dumps/date_range%3D2024-07-14_2024-07-20/matches.parquet'\n
                ,'prod/2024-07-14_matches.parquet')
        """

        url = self.base_url + endpoint
        response = requests.get(url)
        response.raise_for_status()

        if endpoint.endswith(".json"):
            self._save_json(response.content, output_path)
        elif endpoint.endswith(".parquet"):
            self._save_parquet(response.content, output_path)
        else:
            raise ValueError(
                "Unsupported file format. Only .json and .parquet are supported."
            )

    def _save_json(self, content, output_path):
        """Saves the JSON content to the specified output path."""
        output_path = pathlib.Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)  # Create dir if needed
        with open(output_path, "w") as f:
            json.dump(json.loads(content), f, indent=4)

    def _save_parquet(self, content, output_path):
        """Saves the parquet content to the specified output path."""
        output_path = pathlib.Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)  # Create dir if needed
        with open(output_path, "wb") as f:
            f.write(content)


# -----------------------------------------------------------------------------
# Currently unused class.
# Allows filtering logic to be defined schema in yaml file.
# Allows modulaity in data pipeline processing, however too verbose for current project.
# -----------------------------------------------------------------------------


class ComparisonEvaluator:
    def __init__(self, config: Config):
        self.config = config

    def evaluate(self, yaml_data, json_data):
        """
        Evaluate comparisons defined in yaml_data using values from json_data and config.

        Args:
            yaml_data: A dictionary containing comparison definitions.
            json_data: A dictionary containing values to compare.

        Returns:
            bool: True if all comparisons pass, False otherwise.
        """
        operators = {">=": op.ge, "<=": op.le, "==": op.eq}

        for test in yaml_data.get("tests", []):
            left_key_name = test["left_var"]
            operator_name = test["operator"]
            right_key_name = test["right_var"]

            left_key_value = json_data.get(left_key_name)
            # Get config value name, else, get the actual value
            right_key_value = getattr(self.config, right_key_name, right_key_name)

            if left_key_value is None:
                raise ValueError(f"Variable '{left_key_name}' not found in json data.")
            if right_key_value is None:
                raise ValueError(
                    f"Variable '{right_key_name}' not found in config attributes."
                )

            if operator_name not in operators:
                raise ValueError(f"Unsupported operator: {operator_name}")

            if not operators[operator_name](left_key_value, right_key_value):
                return False

        return True

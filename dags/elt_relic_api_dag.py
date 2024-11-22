from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os

# Add the 'src' directory to the Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(os.path.join(project_root, "src"))

from src.elt_relic_api import main as run_elt_relic_api

default_args = {
    "owner": "Jono",
    "depends_on_past": False,
    "email": ["test@live.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    "elt_relic_api_id",
    default_args=default_args,
    description="ETL process for relic_api",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["etl", "relic"],
) as dag:
    run_elt_relic_api_task = PythonOperator(
        task_id="run_elt_relic_api",
        python_callable=run_elt_relic_api,
    )

    run_elt_relic_api_task

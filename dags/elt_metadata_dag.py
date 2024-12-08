from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os

# Add the 'src' directory to the Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(os.path.join(project_root, "src"))

from src.elt_metadata import main as run_elt_metadata

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
    "elt_metadata_dag_id",
    default_args=default_args,
    description="ETL process for metadata",
    schedule_interval=timedelta(days=7),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["etl", "metadata"],
) as dag:
    run_elt_metadata_task = PythonOperator(
        task_id="run_elt_metadata",
        python_callable=run_elt_metadata,
    )

    run_elt_metadata_task

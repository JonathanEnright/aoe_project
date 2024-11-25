from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os

# Add the 'src' directory to the Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(os.path.join(project_root, "src"))

from src.set_load_master import main as run_set_load_master

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
    "set_load_master_id",
    default_args=default_args,
    description="ETL process for load_master",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["etl", "master"],
) as dag:
    run_set_load_master_task = PythonOperator(
        task_id="run_set_load_master",
        python_callable=run_set_load_master,
    )

    run_set_load_master_task

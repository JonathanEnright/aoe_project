from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os

# Add the 'src' directory to the Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(os.path.join(project_root, "src"))

from src.elt_stat_players import main as run_elt_stat_players

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
    "elt_stat_players_id",
    default_args=default_args,
    description="ETL process for stat_players",
    schedule_interval=timedelta(days=7),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["etl", "stats"],
) as dag:
    run_elt_stat_players_task = PythonOperator(
        task_id="run_elt_stat_players",
        python_callable=run_elt_stat_players,
    )

    run_elt_stat_players_task

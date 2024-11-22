from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from datetime import datetime

default_args = {"start_date": datetime(2021, 1, 1)}


def _dag_start():
    print("Starting Airflow Pipeline to run all DAGS")


def _dag_end():
    print("Finished running Airflow Pipeline for all DAGS")


with DAG(
    "trigger_dag", schedule_interval="@daily", default_args=default_args, catchup=False
) as dag:
    start_trigger = PythonOperator(task_id="run_all_start", python_callable=_dag_start)

    end_trigger = PythonOperator(task_id="run_all_end", python_callable=_dag_end)

    trigger_elt_metadata = TriggerDagRunOperator(
        task_id="trigger_metadata",
        trigger_dag_id="elt_metadata_dag_id",
        wait_for_completion=True,
        poke_interval=30,
    )

    trigger_elt_stat_matches = TriggerDagRunOperator(
        task_id="trigger_stat_matches",
        trigger_dag_id="elt_stat_matches_id",
        wait_for_completion=True,
        poke_interval=30,
    )

    trigger_elt_stat_players = TriggerDagRunOperator(
        task_id="trigger_stat_players",
        trigger_dag_id="elt_stat_players_id",
        wait_for_completion=True,
        poke_interval=30,
    )

    trigger_elt_relic_api = TriggerDagRunOperator(
        task_id="trigger_relic_api",
        trigger_dag_id="elt_relic_api_id",
        wait_for_completion=True,
        poke_interval=30,
    )

    trigger_dbt_dag = TriggerDagRunOperator(
        task_id="trigger_dbt_dag",
        trigger_dag_id="dbt_dag_id",
        wait_for_completion=True,
        poke_interval=60,
    )

    # Run order: Check for metadata, then load stats datasets. Can run relic_api simultaneously.
    start_trigger >> trigger_elt_metadata
    trigger_elt_metadata >> trigger_elt_stat_matches
    trigger_elt_metadata >> trigger_elt_stat_players
    start_trigger >> trigger_elt_relic_api

    # Only trigger dbt_dag if all 3 datasets are finished.
    trigger_elt_stat_matches >> trigger_dbt_dag
    trigger_elt_stat_players >> trigger_dbt_dag
    trigger_elt_relic_api >> trigger_dbt_dag

    # Finally close dag run after dbt_dag has finished.
    trigger_dbt_dag >> end_trigger

import os
from datetime import datetime, timedelta
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig

profile_config = ProfileConfig(
    profile_name="dbt_aoe",
    target_name="dev",
    profiles_yml_filepath=f"{os.environ['AIRFLOW_HOME']}/src/transform/dbt_aoe/profiles.yml",
)

dag = DbtDag(
    profile_config=profile_config,
    project_config=ProjectConfig(
        f"{os.environ['AIRFLOW_HOME']}/src/transform/dbt_aoe",
    ),
    operator_args={"install_deps": True},
    execution_config=ExecutionConfig(
        dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",
    ),
    schedule_interval=timedelta(days=7),
    start_date=datetime(2023, 9, 10),
    catchup=False,
    dag_id="dbt_dag_id",
)

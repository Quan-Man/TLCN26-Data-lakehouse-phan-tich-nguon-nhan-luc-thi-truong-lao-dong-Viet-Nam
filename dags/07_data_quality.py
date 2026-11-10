
"""Standalone Data Quality DAG for Education Silver."""

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator


LOCAL_TZ = pendulum.timezone("Asia/Ho_Chi_Minh")

default_args = {
    "owner": "data-engineering-team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="data_quality",
    description="Independent Data Quality checks for Education Silver",
    default_args=default_args,
    start_date=pendulum.datetime(
        2026, 9, 1, tz=LOCAL_TZ
    ),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["data-quality", "silver", "education"],
) as dag:

    check_silver = BashOperator(
        task_id="check_silver",
        bash_command=(
            "python /opt/airflow/jobs/dq/check_silver.py"
        ),
    )

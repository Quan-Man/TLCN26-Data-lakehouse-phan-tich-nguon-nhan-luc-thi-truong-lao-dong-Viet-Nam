
"""Education Bronze -> Silver -> Data Quality."""

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="silver_transformation",
    description="Education Bronze to Silver Delta Lake with Data Quality",
    start_date=pendulum.datetime(
        2026, 9, 1, tz="Asia/Ho_Chi_Minh"
    ),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering-team",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["silver", "education", "spark", "delta", "minio", "dq"],
) as dag:

    build_education_silver = BashOperator(
        task_id="build_education_silver",
        bash_command=(
            "python /opt/airflow/jobs/silver/clean_admissions.py"
        ),
    )

    check_education_silver = BashOperator(
        task_id="check_education_silver",
        bash_command=(
            "python /opt/airflow/jobs/dq/check_silver.py"
        ),
    )

    build_education_silver >> check_education_silver

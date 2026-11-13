
"""Economic & Labour Bronze -> Silver -> Data Quality."""

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator


LOCAL_TZ = pendulum.timezone("Asia/Ho_Chi_Minh")


with DAG(
    dag_id="05_silver_transformation",
    description=(
        "Economic and Labour Bronze official to Silver official Delta Lake "
        "with Data Quality"
    ),
    start_date=pendulum.datetime(
        2026, 9, 1, tz=LOCAL_TZ
    ),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering-team",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=[
        "silver",
        "economic",
        "labour",
        "enterprise",
        "spark",
        "delta",
        "minio",
        "dq",
    ],
) as dag:

    # =====================================================
    # 1. Bronze -> Silver
    # =====================================================

    build_economic_silver = BashOperator(
        task_id="build_economic_silver",
        bash_command=(
            "python "
            "/opt/airflow/jobs/silver/clean_economic.py"
        ),
    )

    # =====================================================
    # 2. Silver Data Quality
    # =====================================================

    check_economic_silver = BashOperator(
        task_id="check_economic_silver",
        bash_command=(
            "python "
            "/opt/airflow/jobs/dq/check_economic_silver.py"
        ),
    )

    # =====================================================
    # 3. Task Dependencies
    # =====================================================

    build_economic_silver >> check_economic_silver


from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="02_build_shared_dimensions",
    description="Build shared location and date dimensions",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    default_args={
        "owner": "lakehouse",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["lakehouse", "shared", "gold"],
) as dag:

    validate_shared_dimensions = BashOperator(
        task_id="validate_shared_dimensions",
        bash_command=(
            "cd /opt/airflow && "
            "python -m jobs.gold.build_shared_dimensions "
            "--dry-run"
        ),
    )

    build_shared_dimensions = BashOperator(
        task_id="build_shared_dimensions",
        bash_command=(
            "cd /opt/airflow && "
            "python -m jobs.gold.build_shared_dimensions"
        ),
    )

    validate_shared_dimensions >> build_shared_dimensions

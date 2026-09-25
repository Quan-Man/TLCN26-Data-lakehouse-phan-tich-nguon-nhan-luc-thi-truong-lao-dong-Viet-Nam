from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator


LOCAL_TZ = pendulum.timezone("Asia/Ho_Chi_Minh")

default_args = {
    "owner": "data-engineering-team",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="04_bronze_ingestion",
    description="Load dữ liệu raw vào Bronze Lakehouse",
    default_args=default_args,
    start_date=pendulum.datetime(
        2026,
        9,
        1,
        tz=LOCAL_TZ,
    ),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["bronze", "lakehouse"],
) as dag:

    load_job_postings = BashOperator(
        task_id="load_job_postings",
        bash_command="""
        python /opt/airflow/scripts/bronze/load_job_postings.py
        """,
    )

    load_education = BashOperator(
        task_id="load_education",
        bash_command="""
        python /opt/airflow/scripts/bronze/load_education.py
        """,
    )

    load_labor_statistics = BashOperator(
        task_id="load_labor_statistics",
        bash_command="""
        python /opt/airflow/scripts/bronze/load_labor_statistics.py
        """,
    )

    [
        load_job_postings,
        load_education,
        load_labor_statistics,
    ]
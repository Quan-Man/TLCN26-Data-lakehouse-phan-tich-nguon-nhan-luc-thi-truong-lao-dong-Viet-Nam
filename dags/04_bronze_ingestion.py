"""Bronze ingestion orchestration.

Currently enables education only. Recruitment and economic ingestion are
intentionally not scheduled until their ETL jobs are implemented.
"""
from datetime import timedelta
import sys

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

# docker-compose mounts ./jobs to /opt/airflow/jobs.
JOBS_BRONZE_DIR = "/opt/airflow/jobs/bronze"
if JOBS_BRONZE_DIR not in sys.path:
    sys.path.insert(0, JOBS_BRONZE_DIR)

from load_admissions import load_university_admissions, load_admission_dates

LOCAL_TZ = pendulum.timezone("Asia/Ho_Chi_Minh")

with DAG(
    dag_id="04_bronze_ingestion",
    description="Raw to Bronze Delta Lake (education implemented)",
    start_date=pendulum.datetime(2026, 9, 1, tz=LOCAL_TZ),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering-team",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["bronze", "education", "spark", "delta", "minio"],
) as dag:
    load_university_admissions_task = PythonOperator(
        task_id="load_university_admissions",
        python_callable=load_university_admissions,
    )

    load_admission_dates_task = PythonOperator(
        task_id="load_admission_dates",
        python_callable=load_admission_dates,
    )

    # Independent Delta outputs. No dependency required between these tasks.
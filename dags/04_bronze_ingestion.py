
"""Raw to Bronze ingestion for Jobs, Education and Economy."""

import sys
from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


# ============================================================
# 1. ENABLE / DISABLE DATA SOURCES
# ============================================================

ENABLE_JOBS = False
ENABLE_EDUCATION = False
ENABLE_ECONOMY = True


# ============================================================
# 2. CONFIGURATION
# ============================================================

LOCAL_TZ = pendulum.timezone("Asia/Ho_Chi_Minh")

JOBS_BRONZE_DIR = "/opt/airflow/jobs/bronze"

if JOBS_BRONZE_DIR not in sys.path:
    sys.path.insert(0, JOBS_BRONZE_DIR)


default_args = {
    "owner": "data-engineering-team",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# ============================================================
# 3. IMPORT ETL FUNCTIONS
# ============================================================

if ENABLE_EDUCATION:
    from load_admissions import (
        load_university_admissions,
        load_admission_dates,
    )

if ENABLE_ECONOMY:
    from load_economic_labour import load_economic_labour


# ============================================================
# 4. VALIDATE CONFIGURATION
# ============================================================

if not (ENABLE_JOBS or ENABLE_EDUCATION or ENABLE_ECONOMY):
    raise ValueError(
        "At least one data source must be enabled."
    )


# ============================================================
# 5. AIRFLOW DAG
# ============================================================

with DAG(
    dag_id="04_bronze_ingestion",
    description=(
        "Raw to Bronze Lakehouse for "
        "Jobs, Education and Economy"
    ),
    default_args=default_args,
    start_date=pendulum.datetime(
        2026, 9, 1, tz=LOCAL_TZ
    ),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=[
        "bronze",
        "lakehouse",
        "jobs",
        "education",
        "economy",
        "minio",
    ],
) as dag:

    # ========================================================
    # 6. JOBS BRONZE
    # ========================================================

    if ENABLE_JOBS:

        load_job_postings = BashOperator(
            task_id="load_job_postings",
            bash_command=(
                "cd /opt/airflow && "
                "python /opt/airflow/jobs/bronze/ingest_job_posting.py "
                "--minio --bucket bronze --prefix job_posting --create-bucket"
            ),
        )

    # ========================================================
    # 7. EDUCATION BRONZE
    # ========================================================

    if ENABLE_EDUCATION:

        load_university_admissions_task = PythonOperator(
            task_id="load_university_admissions",
            python_callable=load_university_admissions,
        )

        load_admission_dates_task = PythonOperator(
            task_id="load_admission_dates",
            python_callable=load_admission_dates,
        )

    # ========================================================
    # 8. ECONOMY BRONZE
    # ========================================================

    if ENABLE_ECONOMY:

        load_economic_labour_task = PythonOperator(
            task_id="load_economic_labour",
            python_callable=load_economic_labour,
            op_kwargs={
                "input_dir": "/opt/airflow/data/economy",
                "output": "s3a://bronze/economic",
            },
        )

    # ========================================================
    # 9. TASK DEPENDENCIES
    # ========================================================

    # All Bronze tasks are independent.
    # Airflow schedules enabled tasks according to
    # the configured executor and parallelism.

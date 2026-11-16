
"""Bronze ingestion orchestration for Education and Economy.

Education:
    - University admissions
    - Admission announcement dates

Economy:
    - 18 source CSV files to 18 Bronze Delta tables

Enable or disable each source using configuration flags.
"""

import sys
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator


# ============================================================
# 1. ENABLE / DISABLE DATA SOURCES
# ============================================================

ENABLE_EDUCATION = True
ENABLE_ECONOMY = False


# ============================================================
# 2. CONFIGURATION
# ============================================================

LOCAL_TZ = pendulum.timezone("Asia/Ho_Chi_Minh")

JOBS_BRONZE_DIR = "/opt/airflow/jobs/bronze"

if JOBS_BRONZE_DIR not in sys.path:
    sys.path.insert(0, JOBS_BRONZE_DIR)


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

if not (ENABLE_EDUCATION or ENABLE_ECONOMY):
    raise ValueError(
        "At least one data source must be enabled."
    )


# ============================================================
# 5. AIRFLOW DAG
# ============================================================

with DAG(
    dag_id="04_bronze_ingestion",
    description="Raw to Bronze Delta Lake for Education and Economy",
    start_date=pendulum.datetime(
        2026, 9, 1, tz=LOCAL_TZ
    ),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering-team",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=[
        "bronze",
        "education",
        "economy",
        "spark",
        "delta",
        "minio",
    ],
) as dag:

    # ========================================================
    # 6. EDUCATION BRONZE
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
    # 7. ECONOMY BRONZE
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
    # 8. TASK DEPENDENCIES
    # ========================================================

    # Independent outputs:
    # No dependencies required between Education and Economy.


"""Bronze to Silver transformation for Jobs, Education and Economy."""

from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator


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

default_args = {
    "owner": "data-engineering-team",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# ============================================================
# 3. VALIDATE CONFIGURATION
# ============================================================

if not (ENABLE_JOBS or ENABLE_EDUCATION or ENABLE_ECONOMY):
    raise ValueError(
        "At least one data source must be enabled."
    )


# ============================================================
# 4. AIRFLOW DAG
# ============================================================

with DAG(
    dag_id="05_silver_transformation",
    description=(
        "Bronze to Silver transformation for "
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
        "silver",
        "lakehouse",
        "jobs",
        "education",
        "economy",
        "etl",
        "minio",
        "dq",
    ],
) as dag:

    # ========================================================
    # 5. JOBS SILVER
    # ========================================================

    if ENABLE_JOBS:

        clean_jobs = BashOperator(
            task_id="clean_job_postings",
            bash_command=(
                "cd /opt/airflow && "
                "python /opt/airflow/jobs/silver/"
                "process_job_silver.py "
                "--bronze-uri s3://bronze/job_posting/ "
                "--minio "
                "--bucket silver "
                "--prefix jobs"
            ),
        )

    # ========================================================
    # 6. EDUCATION SILVER
    # ========================================================

    if ENABLE_EDUCATION:

        build_education_silver = BashOperator(
            task_id="build_education_silver",
            bash_command=(
                "cd /opt/airflow && "
                "python /opt/airflow/jobs/silver/"
                "clean_admissions.py"
            ),
        )

        check_education_silver = BashOperator(
            task_id="check_education_silver",
            bash_command=(
                "cd /opt/airflow && "
                "python /opt/airflow/jobs/dq/"
                "check_education_silver.py"
            ),
        )

        build_education_silver >> check_education_silver

    # ========================================================
    # 7. ECONOMY SILVER
    # ========================================================

    if ENABLE_ECONOMY:

        build_economic_silver = BashOperator(
            task_id="build_economic_silver",
            bash_command=(
                "cd /opt/airflow && "
                "python /opt/airflow/jobs/silver/"
                "clean_economic.py"
            ),
        )

        check_economic_silver = BashOperator(
            task_id="check_economic_silver",
            bash_command=(
                "cd /opt/airflow && "
                "python /opt/airflow/jobs/dq/"
                "check_economic_silver.py"
            ),
        )

        build_economic_silver >> check_economic_silver

    # ========================================================
    # 8. CROSS-SOURCE DEPENDENCIES
    # ========================================================

    # Jobs, Education and Economy are independent
    # at the Silver layer.
    #
    # Shared dimensions and Gold facts are built
    # in DAG 06_gold_analytics.

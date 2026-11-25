from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator

# CONFIGURATION
LOCAL_TZ = pendulum.timezone("Asia/Ho_Chi_Minh")

default_args = {
    "owner": "tlcn2026-nhom3",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="02_silver_transformation",
    description=("Bronze to Silver transformation"),
    default_args=default_args,
    start_date=pendulum.datetime(2026, 9, 1, tz=LOCAL_TZ),
    schedule=None,
    catchup=False,
    max_active_runs=1,
) as dag:
    # JOBS SILVER
    build_job_silver = BashOperator(
        task_id="build_job_silver",
        bash_command=(
            "cd /opt/airflow && "
            "python /opt/airflow/jobs/silver/"
            "clean_jobs.py "
            "--bronze-uri s3://bronze/job_posting/ "
            "--minio --bucket silver --prefix jobs"
        ),
    )

    check_job_silver = BashOperator(
        task_id="check_job_silver",
        bash_command=(
            "cd /opt/airflow && "
            "python /opt/airflow/jobs/dq/"
            "check_jobs_silver.py"
        ),
    )

    # EDUCATION SILVER
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

    # ECONOMY SILVER
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

    ([build_job_silver >> check_job_silver]
    >> [build_education_silver, check_education_silver]
    >> [build_economic_silver, check_economic_silver])

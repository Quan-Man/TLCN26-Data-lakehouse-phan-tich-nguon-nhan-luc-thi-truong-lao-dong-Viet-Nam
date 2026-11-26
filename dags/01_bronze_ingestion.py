
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
    dag_id="01_bronze_ingestion",
    description="Raw to Bronze Lakehouse",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 9, 1, tz=LOCAL_TZ),
    schedule=None,
    catchup=False,
    max_active_runs=1,
) as dag:

    # JOBS BRONZE
    load_job_postings = BashOperator(
        task_id="load_job_postings",
        bash_command=(
            "cd /opt/airflow && "
            "python /opt/airflow/jobs/bronze/load_job_posting.py "
            "--minio --bucket bronze --prefix job_posting --create-bucket"
        ),
    )

    # EDUCATION BRONZE
    load_university_admissions_task = BashOperator(
        task_id="load_university_admissions",
        bash_command=(
            "cd /opt/airflow/jobs/bronze && "
            "python -c 'import os; from load_admissions import "
            "load_university_admissions; "
            "load_university_admissions(run_id=os.environ[\"AIRFLOW_CTX_DAG_RUN_ID\"])'"
        ),
    )

    load_admission_dates_task = BashOperator(
        task_id="load_admission_dates",
        bash_command=(
            "cd /opt/airflow/jobs/bronze && "
            "python -c 'import os; from load_admissions import "
            "load_admission_dates; "
            "load_admission_dates(run_id=os.environ[\"AIRFLOW_CTX_DAG_RUN_ID\"])'"
        ),
    )

    # ECONOMY BRONZE
    load_economic_labour_task = BashOperator(
        task_id="load_economic_labour",
        bash_command=(
            "cd /opt/airflow && "
            "python /opt/airflow/jobs/bronze/load_economic_labour.py "
            "--input-dir /opt/airflow/data/economy "
            "--output s3a://bronze/economic "
            "--allow-production-overwrite"
        ),
        env={
            "ECONOMIC_PRODUCTION_OVERWRITE": "YES_I_HAVE_BACKUP",
        },
        append_env=True,
    )

    # DEPENDENCIES
    (
        load_job_postings
        >> [
            load_university_admissions_task,
            load_admission_dates_task
        ]
        >> load_economic_labour_task
    )

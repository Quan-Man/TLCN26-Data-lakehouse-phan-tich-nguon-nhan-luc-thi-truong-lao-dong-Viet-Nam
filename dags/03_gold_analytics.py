from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator

LOCAL_TZ = pendulum.timezone("Asia/Ho_Chi_Minh")

SHARED_ROOT = "s3a://gold/shared"
JOBS_SILVER_ROOT = "s3a://silver/jobs"
JOBS_GOLD_ROOT = "s3a://gold/jobs"

ECONOMY_ENV = "OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1"

DEFAULT_ARGS = {
    "owner": "tlcn2026-nhom3",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="04_gold_analytics",
    description=(
        "Build Gold facts, dimensions and register analytics tables"
    ),
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2026, 9, 1, tz=LOCAL_TZ),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
) as dag:
    # SHARED DIMENSIONS
    build_shared_dimensions = BashOperator(
        task_id="build_shared_dimensions",
        bash_command=(
            "cd /opt/airflow && "
            "python -m jobs.gold.build_shared_dimensions"
        ),
    )

    build_shared_dimensions

    # JOBS GOLD
    # Build dim_skill, dim_company and fact_job_posting
    build_fact_job = BashOperator(
        task_id="build_fact_job_posting",
        bash_command=(
            "cd /opt/airflow && "
            "python -m jobs.gold.build_jobs_gold "
            f"--silver-root {JOBS_SILVER_ROOT} "
            f"--shared-root {SHARED_ROOT} "
            f"--gold-root {JOBS_GOLD_ROOT} --no-bridges"
        ),
    )

    # Build fact_job_skill
    build_job_skill = BashOperator(
        task_id="build_job_skill",
        bash_command=(
            "cd /opt/airflow && "
            "python -m jobs.gold.build_fact_job_skill"
        ),
    )

    # Build bridge_job_location
    build_bridge_job = BashOperator(
        task_id="build_job_bridge",
        bash_command=(
            "cd /opt/airflow && "
            "python -m jobs.gold.build_job_bridges "
            f"--silver-root {JOBS_SILVER_ROOT} "
            f"--shared-root {SHARED_ROOT} "
            f"--gold-root {JOBS_GOLD_ROOT}"
        ),
    )

    # Jobs dependencies
    build_fact_job >> [
        build_job_skill,
        build_bridge_job,
    ]
    
    # EDUCATION GOLD
    # Build Education dimensions and fact_admission
    build_education_gold = BashOperator(
        task_id="build_education_gold",
        bash_command=(
            "cd /opt/airflow && "
            "python -m jobs.gold.build_education_gold"
        ),
    )

    # Validate Education Gold
    check_education_gold = BashOperator(
        task_id="check_education_gold",
        bash_command=(
            "cd /opt/airflow && "
            "python -m jobs.dq.check_education_gold"
        ),
    )

    # Education dependencies
    (
        register_shared_gold
        >> build_education_gold
        >> check_education_gold
    )

    # ECONOMY GOLD
    # Build Economy dimensions and facts
    build_economy_gold = BashOperator(
        task_id="build_economy_gold",
        bash_command=(
            f"cd /opt/airflow && {ECONOMY_ENV} "
            "python -m jobs.gold.build_economy_gold"
        ),
    )

    # Validate Economy Gold
    check_economy_gold = BashOperator(
        task_id="check_economy_gold",
        bash_command=(
            f"cd /opt/airflow && {ECONOMY_ENV} "
            "python -m jobs.dq.check_economic_gold"
        ),
    )

    # Economy dependencies
    (
        build_economy_gold
        >> check_economy_gold
    )

    register_trino = BashOperator(
        task_id="register_trino",
        bash_command=(
            "cd /opt/airflow && "
            "python -m jobs.gold.register_trino"
        ),
    )

    register_trino

"""Gold analytics for Jobs, Education and Economy."""

from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator


# ============================================================
# 1. ENABLE / DISABLE DATA SOURCES
# ============================================================

ENABLE_JOBS = True
ENABLE_EDUCATION = False
ENABLE_ECONOMY = False

ENABLE_POSTGRES_LOAD = False


# ============================================================
# 2. CONFIGURATION
# ============================================================

LOCAL_TZ = pendulum.timezone("Asia/Ho_Chi_Minh")

SHARED_ROOT = "s3a://gold/shared"
JOBS_SILVER_ROOT = "s3a://silver/jobs"
JOBS_GOLD_ROOT = "s3a://gold/jobs"

ECONOMY_ENV = "OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1"

DEFAULT_ARGS = {
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
    dag_id="06_gold_analytics",
    description=(
        "Build shared dimensions, Gold facts, "
        "validate and register analytics tables"
    ),
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(
        2026, 9, 1, tz=LOCAL_TZ
    ),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=[
        "gold",
        "analytics",
        "lakehouse",
        "jobs",
        "education",
        "economy",
        "spark",
        "delta",
        "dq",
        "trino",
    ],
) as dag:

    # ========================================================
    # 5. SHARED DIMENSIONS
    # ========================================================

    build_shared_dimensions = BashOperator(
        task_id="build_shared_dimensions",
        bash_command=(
            "cd /opt/airflow && "
            "python -m jobs.gold.build_shared_dimensions"
        ),
    )

    register_shared_gold = BashOperator(
        task_id="register_shared_gold",
        bash_command=("cd /opt/airflow && "
                      "python -m jobs.gold.register_jobs_trino --scope shared"),
    )
    build_shared_dimensions >> register_shared_gold

    # ========================================================
    # 6. JOBS GOLD
    # ========================================================

    if ENABLE_JOBS:

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

        # Register Jobs Delta tables in Trino
        register_jobs_gold = BashOperator(
            task_id="register_jobs_gold",
            bash_command=(
                "cd /opt/airflow && "
                "python -m jobs.gold.register_jobs_trino --scope jobs"
            ),
        )

        # Jobs dependencies
        register_shared_gold >> build_fact_job

        build_fact_job >> [
            build_job_skill,
            build_bridge_job,
        ]

        [
            build_job_skill,
            build_bridge_job,
        ] >> register_jobs_gold

        # Optional PostgreSQL serving layer
        if ENABLE_POSTGRES_LOAD:

            load_postgres = BashOperator(
                task_id="load_job_postgres",
                bash_command=(
                    "cd /opt/airflow && "
                    "python /opt/airflow/jobs/gold/"
                    "load_job_postgres.py"
                ),
            )

            register_jobs_gold >> load_postgres

    # ========================================================
    # 7. EDUCATION GOLD
    # ========================================================

    if ENABLE_EDUCATION:

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

        # Register Education Delta tables
        register_education_gold = BashOperator(
            task_id="register_education_gold",
            bash_command=(
                "cd /opt/airflow && "
                "python -m jobs.gold.build_education_gold "
                "--register-only"
            ),
        )

        # Education dependencies
        (
            register_shared_gold
            >> build_education_gold
            >> check_education_gold
            >> register_education_gold
        )

    # ========================================================
    # 8. ECONOMY GOLD
    # ========================================================

    if ENABLE_ECONOMY:

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

        # Register Economy Delta tables
        register_economy_gold = BashOperator(
            task_id="register_economy_gold",
            bash_command=(
                f"cd /opt/airflow && {ECONOMY_ENV} "
                "python -m jobs.gold.build_economy_gold "
                "--register-only"
            ),
        )

        # Economy dependencies
        (
            register_shared_gold
            >> build_economy_gold
            >> check_economy_gold
            >> register_economy_gold
        )

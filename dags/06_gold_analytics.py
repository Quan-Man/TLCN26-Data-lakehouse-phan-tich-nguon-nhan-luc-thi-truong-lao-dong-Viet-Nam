
# -*- coding: utf-8 -*-
"""Gold analytics orchestration for Education and Economy.

Education:
    Build Gold -> Data Quality -> Hive Registration

Economy:
    Build Gold -> Data Quality -> Hive Registration

Enable or disable each data source using configuration flags.
"""

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator


# ============================================================
# 1. ENABLE / DISABLE DATA SOURCES
# ============================================================

ENABLE_EDUCATION = True
ENABLE_ECONOMY = False


# ============================================================
# 2. CONFIGURATION
# ============================================================

LOCAL_TZ = pendulum.timezone("Asia/Ho_Chi_Minh")

if not (ENABLE_EDUCATION or ENABLE_ECONOMY):
    raise ValueError(
        "At least one data source must be enabled."
    )

# Limit BLAS/OpenMP threads for Economy Spark processes.
ECONOMY_ENV = "OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1"


# ============================================================
# 3. AIRFLOW DAG
# ============================================================

with DAG(
    dag_id="06_gold_analytics",
    description=(
        "Build, validate and register Education and Economy "
        "Gold Delta tables"
    ),
    start_date=pendulum.datetime(
        2026, 9, 1, tz=LOCAL_TZ
    ),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering-team",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=[
        "gold",
        "education",
        "economy",
        "spark",
        "delta",
        "minio",
        "dq",
        "hive",
    ],
) as dag:

    # ========================================================
    # 4. EDUCATION GOLD
    # ========================================================

    if ENABLE_EDUCATION:

        # Build Education Gold Delta tables
        build_education_gold = BashOperator(
            task_id="build_education_gold",
            bash_command=(
                "cd /opt/airflow && "
                "python -m jobs.gold.build_education_gold"
            ),
        )

        # Validate Education Gold data quality
        check_education_gold = BashOperator(
            task_id="check_education_gold",
            bash_command=(
                "cd /opt/airflow && "
                "python -m jobs.dq.check_education_gold"
            ),
        )

        # Register Education Gold tables in Hive Metastore
        register_education_gold = BashOperator(
            task_id="register_education_gold",
            bash_command=(
                "cd /opt/airflow && "
                "python -m jobs.gold.build_education_gold "
                "--register-only"
            ),
        )

        build_education_gold >> check_education_gold >> register_education_gold

    # ========================================================
    # 5. ECONOMY GOLD
    # ========================================================

    if ENABLE_ECONOMY:

        # Build Economy Gold Delta tables
        build_economy_gold = BashOperator(
            task_id="build_economy_gold",
            bash_command=(
                f"cd /opt/airflow && {ECONOMY_ENV} "
                "python -m jobs.gold.build_economy_gold"
            ),
        )

        # Validate Economy Gold data quality
        check_economy_gold = BashOperator(
            task_id="check_economy_gold",
            bash_command=(
                f"cd /opt/airflow && {ECONOMY_ENV} "
                "python -m jobs.dq.check_economic_gold"
            ),
        )

        # Register Economy Gold tables in Hive Metastore
        register_economy_gold = BashOperator(
            task_id="register_economy_gold",
            bash_command=(
                f"cd /opt/airflow && {ECONOMY_ENV} "
                "python -m jobs.gold.build_economy_gold "
                "--register-only"
            ),
        )

        build_economy_gold >> check_economy_gold >> register_economy_gold

    # ========================================================
    # 6. CROSS-SOURCE DEPENDENCIES
    # ========================================================

    # Education and Economy Gold pipelines are independent.
    # No cross-source dependencies are required here.

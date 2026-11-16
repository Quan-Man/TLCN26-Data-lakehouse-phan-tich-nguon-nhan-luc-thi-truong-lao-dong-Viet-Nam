
"""Silver transformation and Data Quality for Education and Economy.

Education:
    Bronze -> Silver -> Data Quality

Economy:
    Bronze -> Silver -> Data Quality

Enable or disable each source using configuration flags.
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


# ============================================================
# 3. AIRFLOW DAG
# ============================================================

with DAG(
    dag_id="05_silver_transformation",
    description=(
        "Education and Economy Bronze to Silver Delta Lake "
        "with Data Quality"
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
        "silver",
        "education",
        "economy",
        "labour",
        "enterprise",
        "spark",
        "delta",
        "minio",
        "dq",
    ],
) as dag:

    # ========================================================
    # 4. EDUCATION SILVER
    # ========================================================

    if ENABLE_EDUCATION:

        build_education_silver = BashOperator(
            task_id="build_education_silver",
            bash_command=(
                "python /opt/airflow/jobs/silver/clean_admissions.py"
            ),
        )

        check_education_silver = BashOperator(
            task_id="check_education_silver",
            bash_command=(
                "python /opt/airflow/jobs/dq/check_education_silver.py"
            ),
        )

        # Education: Bronze -> Silver -> Data Quality
        build_education_silver >> check_education_silver

    # ========================================================
    # 5. ECONOMY SILVER
    # ========================================================

    if ENABLE_ECONOMY:

        build_economic_silver = BashOperator(
            task_id="build_economic_silver",
            bash_command=(
                "python /opt/airflow/jobs/silver/clean_economic.py"
            ),
        )

        check_economic_silver = BashOperator(
            task_id="check_economic_silver",
            bash_command=(
                "python /opt/airflow/jobs/dq/check_economic_silver.py"
            ),
        )

        # Economy: Bronze -> Silver -> Data Quality
        build_economic_silver >> check_economic_silver

    # ========================================================
    # 6. CROSS-SOURCE DEPENDENCIES
    # ========================================================

    # Education and Economy are independent pipelines.
    # No dependency between the two source groups.

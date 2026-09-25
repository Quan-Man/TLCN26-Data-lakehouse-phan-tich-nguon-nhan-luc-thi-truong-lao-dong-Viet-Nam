from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator


# ============================================================
# CONFIG
# ============================================================

LOCAL_TZ = pendulum.timezone("Asia/Ho_Chi_Minh")


default_args = {
    "owner": "data-engineering-team",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# ============================================================
# DAG
# ============================================================

with DAG(
    dag_id="education_ingestion",
    description="Thu thập dữ liệu giáo dục Việt Nam",
    default_args=default_args,
    start_date=pendulum.datetime(
        2026,
        9,
        1,
        tz=LOCAL_TZ,
    ),
    schedule="0 2 * * *",       # chạy mỗi ngày lúc 02:00
    catchup=False,
    max_active_runs=1,
    tags=[
        "education",
        "bronze",
        "scraping",
    ],
) as dag:

    # --------------------------------------------------------
    # 1. Crawl TopCV
    # --------------------------------------------------------

    crawl_topcv = BashOperator(
        task_id="crawl_topcv",
        bash_command="""
        python /opt/airflow/scripts/crawlers/topcv_crawler.py
        """,
    )

    # --------------------------------------------------------
    # 2. Crawl CareerViet
    # --------------------------------------------------------

    crawl_careerviet = BashOperator(
        task_id="crawl_careerviet",
        bash_command="""
        python /opt/airflow/scripts/crawlers/careerviet_crawler.py
        """,
    )

    # --------------------------------------------------------
    # 3. Crawl Vieclam24h
    # --------------------------------------------------------

    crawl_vieclam24h = BashOperator(
        task_id="crawl_vieclam24h",
        bash_command="""
        python /opt/airflow/scripts/crawlers/vieclam24h_crawler.py
        """,
    )

    # --------------------------------------------------------
    # 4. Crawl Timviec365
    # --------------------------------------------------------

    crawl_timviec365 = BashOperator(
        task_id="crawl_timviec365",
        bash_command="""
        python /opt/airflow/scripts/crawlers/timviec365_crawler.py
        """,
    )

    # --------------------------------------------------------
    # 5. Validate raw JSON
    # --------------------------------------------------------

    validate_raw_data = BashOperator(
        task_id="validate_raw_data",
        bash_command="""
        python /opt/airflow/scripts/validation/validate_education_json.py
        """,
    )

    # --------------------------------------------------------
    # Dependency
    # --------------------------------------------------------

    [
        crawl_topcv,
        crawl_careerviet,
        crawl_vieclam24h,
        crawl_timviec365,
    ] >> validate_raw_data
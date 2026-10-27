from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


# ============================================================
# CONFIG
# ============================================================

LOCAL_TZ = pendulum.timezone("Asia/Ho_Chi_Minh")

default_args = {
    "owner": "data-engineering-team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# ============================================================
# MASTER DAG
# ============================================================

with DAG(
    dag_id="00_master_pipeline",
    description=(
        "Master DAG điều phối toàn bộ pipeline "
        "Data Lakehouse phân tích nguồn nhân lực "
        "và thị trường lao động Việt Nam"
    ),
    default_args=default_args,

    start_date=pendulum.datetime(
        2026,
        9,
        1,
        tz=LOCAL_TZ,
    ),

    # Chạy mỗi ngày lúc 02:00
    schedule="0 2 * * *",

    catchup=False,

    max_active_runs=1,

    tags=[
        "master",
        "lakehouse",
        "labor-market",
        "data-engineering",
    ],

) as dag:

    # ========================================================
    # 1. JOB MARKET INGESTION
    # ========================================================

    job_market_ingestion = TriggerDagRunOperator(
        task_id="trigger_job_market_ingestion",

        trigger_dag_id="job_market_ingestion",

        wait_for_completion=True,

        reset_dag_run=False,

        poke_interval=30,

        allowed_states=["success"],

        failed_states=["failed"],
    )


    # ========================================================
    # 2. EDUCATION INGESTION
    # ========================================================

    education_ingestion = TriggerDagRunOperator(
        task_id="trigger_education_ingestion",

        trigger_dag_id="education_ingestion",

        wait_for_completion=True,

        reset_dag_run=False,

        poke_interval=30,

        allowed_states=["success"],

        failed_states=["failed"],
    )


    # ========================================================
    # 3. LABOR STATISTICS INGESTION
    # ========================================================

    labor_statistics_ingestion = TriggerDagRunOperator(
        task_id="trigger_labor_statistics_ingestion",

        trigger_dag_id="labor_statistics_ingestion",

        wait_for_completion=True,

        reset_dag_run=False,

        poke_interval=30,

        allowed_states=["success"],

        failed_states=["failed"],
    )


    # ========================================================
    # 4. SILVER TRANSFORMATION
    # ========================================================

    silver_transformation = TriggerDagRunOperator(
        task_id="trigger_silver_transformation",

        trigger_dag_id="silver_transformation",

        wait_for_completion=True,

        reset_dag_run=False,

        poke_interval=30,

        allowed_states=["success"],

        failed_states=["failed"],
    )


    # ========================================================
    # 5. GOLD ANALYTICS
    # ========================================================

    gold_analytics = TriggerDagRunOperator(
        task_id="trigger_gold_analytics",

        trigger_dag_id="gold_analytics",

        wait_for_completion=True,

        reset_dag_run=False,

        poke_interval=30,

        allowed_states=["success"],

        failed_states=["failed"],
    )


    # ========================================================
    # 6. DATA QUALITY
    # ========================================================

    data_quality = TriggerDagRunOperator(
        task_id="trigger_data_quality",

        trigger_dag_id="data_quality",

        wait_for_completion=True,

        reset_dag_run=False,

        poke_interval=30,

        allowed_states=["success"],

        failed_states=["failed"],
    )


    # ========================================================
    # PIPELINE DEPENDENCY
    # ========================================================

    # --------------------------------------------------------
    # Các nguồn dữ liệu có thể chạy song song
    # --------------------------------------------------------

    [
        job_market_ingestion,
        education_ingestion,
        labor_statistics_ingestion,
    ] >> silver_transformation


    # --------------------------------------------------------
    # Silver hoàn thành → Gold
    # --------------------------------------------------------

    silver_transformation >> gold_analytics


    # --------------------------------------------------------
    # Gold hoàn thành → Data Quality
    # --------------------------------------------------------

    gold_analytics >> data_quality
"""Official Economy Bronze ingestion, 18 Delta tables."""
from datetime import timedelta
import sys
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

BRONZE_JOBS_DIR = "/opt/airflow/jobs/bronze"
if BRONZE_JOBS_DIR not in sys.path:
    sys.path.insert(0, BRONZE_JOBS_DIR)
from load_economic_labour import load_economic_labour

LOCAL_TZ = pendulum.timezone("Asia/Ho_Chi_Minh")
with DAG(
    dag_id="04_bronze_ingestion",
    description="18 source CSVs to 18 raw-shaped Delta tables (official destination, overwrite gated)",
    start_date=pendulum.datetime(2026, 9, 1, tz=LOCAL_TZ),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering-team",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["bronze", "economy", "spark", "delta", "minio"],
) as dag:
    load_economic_labour_task = PythonOperator(
        task_id="load_economic_labour",
        python_callable=load_economic_labour,
        op_kwargs={
            "input_dir": "/opt/airflow/data/economy",
            "output": "s3a://bronze/economic",
        },
    )

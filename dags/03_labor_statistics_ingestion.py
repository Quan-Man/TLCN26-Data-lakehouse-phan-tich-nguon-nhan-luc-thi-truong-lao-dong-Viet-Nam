"""Independent Economic/Labour CSV -> Bronze Delta ingestion DAG.

This DAG intentionally does not modify the education Bronze DAG.
The loader is a spark-submit CLI, so BashOperator is used instead of
PythonOperator. Spark/Delta/S3A dependencies must exist in the Airflow image.
"""
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

LOCAL_TZ = pendulum.timezone("Asia/Ho_Chi_Minh")

# Paths inside the Airflow container (verify the Docker Compose volume mounts).
ECONOMIC_LOADER = "/opt/airflow/jobs/bronze/load_economic_labour.py"
ECONOMIC_INPUT_DIR = "/opt/airflow/data/economy"
ECONOMIC_BRONZE_OUTPUT = "s3a://bronze/economic/economic_statistics"

with DAG(
    dag_id="03_labor_statistics_ingestion",
    description="Ingest 19 Economic/Labour CSV files to Bronze Delta on MinIO",
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
    tags=["bronze", "economic", "labour", "spark", "delta", "minio"],
) as dag:
    load_economic_labour_task = BashOperator(
        task_id="load_economic_labour",

bash_command=(
    "set -euo pipefail; "
    "test -f '/opt/airflow/jobs/bronze/load_economic_labour.py'; "
    "test -d '/opt/airflow/data/economy'; "
    "spark-submit "
    "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension "
    "--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog "
    "'/opt/airflow/jobs/bronze/load_economic_labour.py' "
    "--input-dir '/opt/airflow/data/economy' "
    "--output 's3a://bronze/economic/economic_statistics' "
    "--batch-id 'economic_initial' "
    "--mode overwrite"
),

    )

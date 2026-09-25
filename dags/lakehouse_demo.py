from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="lakehouse_demo",
    description="Synthetic data: MinIO Bronze -> Delta Silver/Gold -> PostgreSQL",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Ho_Chi_Minh"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "nhom3", "retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["lakehouse", "demo"],
) as dag:
    run_pipeline = BashOperator(
        task_id="bronze_silver_gold_postgres",
        bash_command="python /opt/airflow/jobs/demo_pipeline.py",
        execution_timeout=timedelta(minutes=15),
    )

from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator


LOCAL_TZ = pendulum.timezone("Asia/Ho_Chi_Minh")

default_args = {
    "owner": "data-engineering-team",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="data_quality",
    description="Kiểm tra chất lượng dữ liệu Lakehouse",
    default_args=default_args,
    start_date=pendulum.datetime(
        2026,
        9,
        1,
        tz=LOCAL_TZ,
    ),
    schedule="30 5 * * *",
    catchup=False,
    tags=["data-quality"],
) as dag:

    check_bronze = BashOperator(
        task_id="check_bronze",
        bash_command="""
        python /opt/airflow/scripts/dq/check_bronze.py
        """,
    )

    check_silver = BashOperator(
        task_id="check_silver",
        bash_command="""
        python /opt/airflow/scripts/dq/check_silver.py
        """,
    )

    check_gold = BashOperator(
        task_id="check_gold",
        bash_command="""
        python /opt/airflow/scripts/dq/check_gold.py
        """,
    )

    check_bronze >> check_silver >> check_gold
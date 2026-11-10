
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="gold_education",
    description="Build, validate, and register Education Gold Delta tables",
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Ho_Chi_Minh"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering-team",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["gold", "education", "delta", "minio"],
) as dag:

    # 1. Xây dựng các bảng Gold
    build_education_gold = BashOperator(
        task_id="build_education_gold",
        bash_command="cd /opt/airflow && python -m jobs.gold.build_education_gold",
    )

    # 2. Kiểm tra chất lượng dữ liệu Gold
    check_education_gold = BashOperator(
        task_id="check_education_gold",
        bash_command="cd /opt/airflow && python -m jobs.dq.check_gold",
    )

    # 3. Đăng ký bảng Gold vào Hive Metastore
    register_education_gold = BashOperator(
        task_id="register_education_gold",
        bash_command="cd /opt/airflow && python -m jobs.gold.build_education_gold --register-only",
    )

    # Thứ tự chạy
    build_education_gold >> check_education_gold >> register_education_gold

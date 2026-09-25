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
    dag_id="gold_analytics",
    description="Xây dựng các bảng phân tích thị trường lao động",
    default_args=default_args,
    start_date=pendulum.datetime(
        2026,
        9,
        1,
        tz=LOCAL_TZ,
    ),
    schedule="0 5 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["gold", "analytics"],
) as dag:

    build_labor_demand = BashOperator(
        task_id="build_labor_demand",
        bash_command="""
        python /opt/airflow/scripts/gold/labor_demand.py
        """,
    )

    build_skill_demand = BashOperator(
        task_id="build_skill_demand",
        bash_command="""
        python /opt/airflow/scripts/gold/skill_demand.py
        """,
    )

    build_education_supply = BashOperator(
        task_id="build_education_supply",
        bash_command="""
        python /opt/airflow/scripts/gold/education_supply.py
        """,
    )

    build_labor_supply = BashOperator(
        task_id="build_labor_supply",
        bash_command="""
        python /opt/airflow/scripts/gold/labor_supply.py
        """,
    )

    build_supply_demand_gap = BashOperator(
        task_id="build_supply_demand_gap",
        bash_command="""
        python /opt/airflow/scripts/gold/supply_demand_gap.py
        """,
    )

    [
        build_labor_demand,
        build_skill_demand,
        build_education_supply,
        build_labor_supply,
    ] >> build_supply_demand_gap
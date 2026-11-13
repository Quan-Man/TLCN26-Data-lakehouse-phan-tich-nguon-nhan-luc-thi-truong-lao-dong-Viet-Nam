# -*- coding: utf-8 -*-
"""Economy Gold: idempotent Delta build -> DQ -> Hive registration.

No manual Trino metadata flush. Unchanged tables keep the same Delta files.
Changed tables still depend on normal Trino Delta metadata invalidation.
"""
from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id='06_gold_analytics',
    description='Build, validate and register 8 Economy Gold Delta tables',
    start_date=pendulum.datetime(2026, 9, 1, tz='Asia/Ho_Chi_Minh'),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        'owner': 'data-engineering-team',
        'retries': 0,
        'retry_delay': timedelta(minutes=5),
    },
    tags=['gold', 'economy', 'spark', 'delta', 'dq'],
) as dag:
    build_economy_gold = BashOperator(
        task_id='build_economy_gold',
        bash_command='cd /opt/airflow && OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 python -m jobs.gold.build_economy_gold',
    )
    check_economy_gold = BashOperator(
        task_id='check_economy_gold',
        bash_command='cd /opt/airflow && OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 python -m jobs.dq.check_economic_gold',
    )
    register_economy_gold = BashOperator(
        task_id='register_economy_gold',
        bash_command='cd /opt/airflow && OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 python -m jobs.gold.build_economy_gold --register-only',
    )
    build_economy_gold >> check_economy_gold >> register_economy_gold

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
    dag_id="silver_transformation",
    description="Làm sạch và chuẩn hóa dữ liệu Bronze sang Silver",
    default_args=default_args,
    start_date=pendulum.datetime(
        2026,
        9,
        1,
        tz=LOCAL_TZ,
    ),
    schedule="0 4 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["silver", "etl", "lakehouse"],
) as dag:

    clean_jobs = BashOperator(
        task_id="clean_job_postings",
        bash_command="""
        python /opt/airflow/scripts/silver/clean_job_postings.py
        """,
    )

    normalize_company = BashOperator(
        task_id="normalize_company",
        bash_command="""
        python /opt/airflow/scripts/silver/normalize_company.py
        """,
    )

    normalize_industry = BashOperator(
        task_id="normalize_industry",
        bash_command="""
        python /opt/airflow/scripts/silver/normalize_industry.py
        """,
    )

    normalize_occupation = BashOperator(
        task_id="normalize_occupation",
        bash_command="""
        python /opt/airflow/scripts/silver/normalize_occupation.py
        """,
    )

    normalize_skill = BashOperator(
        task_id="normalize_skill",
        bash_command="""
        python /opt/airflow/scripts/silver/normalize_skill.py
        """,
    )

    clean_university = BashOperator(
        task_id="clean_university",
        bash_command="""
        python /opt/airflow/scripts/silver/clean_university.py
        """,
    )

    clean_program = BashOperator(
        task_id="clean_program",
        bash_command="""
        python /opt/airflow/scripts/silver/clean_program.py
        """,
    )

    build_fact_job = BashOperator(
        task_id="build_fact_job_posting",
        bash_command="""
        python /opt/airflow/scripts/silver/build_fact_job_posting.py
        """,
    )

    # Dependency

    clean_jobs >> [
        normalize_company,
        normalize_industry,
        normalize_occupation,
        normalize_skill,
    ]

    [
        normalize_company,
        normalize_industry,
        normalize_occupation,
        normalize_skill,
    ] >> build_fact_job

    clean_university >> clean_program
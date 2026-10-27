"""Publish the seven local Education Gold Parquet files into PostgreSQL for Superset.
Run inside the airflow-scheduler container after education_03_gold succeeds.
Only tables in schema education_gold are replaced; other schemas are untouched.
"""
import io
import os
from pathlib import Path
import pandas as pd
import psycopg2
from psycopg2 import sql
from pandas.api.types import (
    is_bool_dtype, is_integer_dtype, is_float_dtype, is_datetime64_any_dtype
)

SOURCE = Path(os.getenv('EDUCATION_GOLD_DIR', '/opt/airflow/data/gold/admission'))
SCHEMA = 'education_gold'
TABLES = [
    'dim_date', 'dim_location', 'dim_university', 'dim_major', 'dim_program',
    'fact_admission', 'agg_education_supply_yearly'
]


def sql_type(series):
    if is_bool_dtype(series):
        return 'BOOLEAN'
    if is_integer_dtype(series):
        return 'BIGINT'
    if is_float_dtype(series):
        return 'DOUBLE PRECISION'
    if is_datetime64_any_dtype(series):
        return 'TIMESTAMPTZ' if series.dt.tz is not None else 'TIMESTAMP'
    return 'TEXT'


def main():
    frames = {}
    for name in TABLES:
        path = SOURCE / (name + '.parquet')
        if not path.is_file():
            raise FileNotFoundError(f'Missing Gold file: {path}. Run Gold DAG first.')
        df = pd.read_parquet(path)
        if not df.columns.is_unique:
            raise ValueError(f'Duplicate columns in {name}')
        frames[name] = df
        print(f'Ready: {name} ({len(df)} rows, {len(df.columns)} columns)')

    if len(frames['fact_admission']) != 7082:
        print('WARNING: fact_admission row count differs from the previously observed 7082.')

    connection = psycopg2.connect(
        host=os.getenv('PGHOST', 'postgres'),
        port=int(os.getenv('PGPORT', '5432')),
        dbname=os.getenv('PGDATABASE', 'lakehouse'),
        user=os.getenv('PGUSER', 'lakehouse'),
        password=os.environ['PGPASSWORD'],
        connect_timeout=15,
    )
    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(sql.SQL('CREATE SCHEMA IF NOT EXISTS {}').format(sql.Identifier(SCHEMA)))
                for name, df in frames.items():
                    target = sql.SQL('{}.{}').format(sql.Identifier(SCHEMA), sql.Identifier(name))
                    cursor.execute(sql.SQL('DROP TABLE IF EXISTS {}').format(target))
                    defs = sql.SQL(', ').join(
                        sql.SQL('{} {}').format(sql.Identifier(col), sql.SQL(sql_type(df[col])))
                        for col in df.columns
                    )
                    cursor.execute(sql.SQL('CREATE TABLE {} ({})').format(target, defs))
                    if not df.empty:
                        buf = io.StringIO()
                        df.to_csv(buf, index=False, header=False, na_rep='\\N', lineterminator='\n')
                        buf.seek(0)
                        cols = sql.SQL(', ').join(sql.Identifier(col) for col in df.columns)
                        copy_stmt = sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT CSV, NULL '\\N')").format(target, cols)
                        cursor.copy_expert(copy_stmt.as_string(connection), buf)
                    cursor.execute(sql.SQL('SELECT COUNT(*) FROM {}').format(target))
                    count = cursor.fetchone()[0]
                    if count != len(df):
                        raise RuntimeError(f'Row count mismatch for {name}: {count} vs {len(df)}')
                    print(f'Loaded: {SCHEMA}.{name} = {count} rows')
        print('SUCCESS: Education Gold tables are available in PostgreSQL.')
    finally:
        connection.close()


if __name__ == '__main__':
    main()

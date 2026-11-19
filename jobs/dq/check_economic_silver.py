"""Economy Silver read-only DQ, optimized for the small 18-CSV dataset.

No Spark union of 18 unpivoted tables, no large join, no Delta source_column
IN predicate. Compare bounded records on the driver (default max 100,000).
"""
import hashlib
import os
import re
from collections import Counter
from decimal import Decimal, InvalidOperation
from pyspark.sql import SparkSession
import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[2]))
from jobs.silver.clean_economic import prepare, schema, GROUPS

BRONZE_ROOT = 's3a://bronze/economic'
SILVER_ROOT = 's3a://silver/economic'
MAX_OBSERVATIONS = int(os.getenv('ECONOMIC_DQ_MAX_OBSERVATIONS', '100000'))
def spark_session():
    endpoint = os.getenv('MINIO_ENDPOINT', 'http://minio:9000')
    user = os.getenv('MINIO_ROOT_USER') or os.getenv('AWS_ACCESS_KEY_ID')
    password = os.getenv('MINIO_ROOT_PASSWORD') or os.getenv('AWS_SECRET_ACCESS_KEY')
    if not user or not password:
        raise RuntimeError('Missing MinIO credentials')
    return (SparkSession.builder.appName('economic-silver-dq-bounded')
            .master('local[2]')
            .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
            .config('spark.sql.catalog.spark_catalog',
                    'org.apache.spark.sql.delta.catalog.DeltaCatalog')
            .config('spark.hadoop.fs.s3a.endpoint', endpoint)
            .config('spark.hadoop.fs.s3a.access.key', user)
            .config('spark.hadoop.fs.s3a.secret.key', password)
            .config('spark.hadoop.fs.s3a.path.style.access', 'true')
            .config('spark.hadoop.fs.s3a.connection.ssl.enabled',
                    str(endpoint.startswith('https://')).lower())
            .config('spark.hadoop.fs.s3a.aws.credentials.provider',
                    'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
            .config('spark.sql.shuffle.partitions', '2')
            .config('spark.sql.codegen.wholeStage', 'false')
            .config('spark.sql.codegen.factoryMode', 'NO_CODEGEN')
            .getOrCreate())


def bounded_rows(df, label, max_rows):
    # Enforce bound BEFORE collect: never unexpectedly bring huge tables to driver.
    rows = df.limit(max_rows + 1).collect()
    if len(rows) > max_rows:
        raise ValueError(f'{label}: exceeds safety limit {max_rows}; use distributed DQ')
    return [r.asDict(recursive=True) for r in rows]


def nonempty(value):
    return value is not None and str(value).strip() != ''


def expected_id(source_raw_id, column):
    return hashlib.sha256(f'{source_raw_id}|{column}'.encode('utf-8')).hexdigest()


def fail(message):
    raise ValueError('SILVER DQ FAILED: ' + message)


def check():
    spark=spark_session()
    try:
        expected=prepare(spark)
        total=0
        for group in GROUPS:
            idcol=group+'_statistic_id'
            df=spark.read.format('delta').load(f'{SILVER_ROOT}/{group}_statistics')
            if dict(df.dtypes)!=dict((f.name,f.dataType.simpleString()) for f in schema(group)): fail(f'{group}: schema mismatch')
            rows=bounded_rows(df,group,MAX_OBSERVATIONS)
            lookup={r[idcol]:r for r in expected[group]}
            seen=set()
            for r in rows:
                k=r[idcol]
                if not k or k in seen or k not in lookup: fail(f'{group}: invalid statistic ID {k}')
                seen.add(k)
                for field,value in lookup[k].items():
                    if r[field]!=value: fail(f'{group}: {k}: {field} mismatch: {r[field]!r} != {value!r}')
                if (r['indicator_value'] is None)!=(r['value_status']=='MISSING'): fail('Missing status mismatch')
            if seen!=set(lookup): fail(f'{group}: missing observations {len(set(lookup)-seen)}')
            total+=len(rows)
            print(f'DQ PASS: {group}_statistics={len(rows)}; statuses={dict(Counter(r["value_status"] for r in rows))}',flush=True)
        return {'silver_observations':total}
    finally: spark.stop()
if __name__=='__main__': print(check())
from __future__ import annotations

import os
import re
import argparse
from datetime import date
from functools import reduce

from pyspark.sql import DataFrame, SparkSession, Window, functions as F, types as T

SILVER = "s3a://silver/economic"
GOLD = "s3a://gold/economic"
REGISTER_HIVE = False  # Only the explicit --register-only mode connects to Hive
HIVE_URI = os.getenv("HIVE_METASTORE_URI", "thrift://hive-metastore:9083")
GOLD_DB = os.getenv("ECONOMY_GOLD_DATABASE", "economic")
SHARED_LOCATION = "s3a://gold/shared/dim_location"  # READ ONLY; do not write
STRICT_DQ = os.getenv("ECONOMY_GOLD_STRICT_DQ", "true").lower() == "true"
ALLOW_UNKNOWN = os.getenv("ECONOMY_GOLD_ALLOW_UNKNOWN_INDICATORS", "false").lower() == "true"

def spark_session(register_hive: bool = False) -> SparkSession:
    access_key = os.environ["MINIO_ROOT_USER"]
    secret_key = os.environ["MINIO_ROOT_PASSWORD"]
    spark = (
        SparkSession.builder
        .appName("DW_GOLD_Economy")
        .master("local[1]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.codegen.wholeStage", "false")
        .config("spark.sql.codegen.factoryMode", "NO_CODEGEN")
        .config("spark.sql.ansi.enabled", "false")
    )
    if register_hive:
        spark = spark.enableHiveSupport()
    spark = (spark.getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def hash_key(*columns: str):
    # Null-safe and unambiguous deterministic SHA256 surrogate key.
    chunks = [F.when(F.col(c).isNull(), F.lit("N:"))
              .otherwise(F.concat(F.lit("S:"), F.length(F.col(c).cast("string")).cast("string"),
                                  F.lit(":"), F.col(c).cast("string"))) for c in columns]
    return F.sha2(F.concat_ws("|", *chunks), 256)


def required(df: DataFrame, cols: list[str], name: str):
    missing = sorted(set(cols) - set(df.columns))
    if missing:
        raise ValueError(f"{name} missing required columns: {missing}; actual={df.columns}")


def read_silver(spark: SparkSession, table: str) -> DataFrame:
    path = f"{SILVER}/{table}"
    print(f"Reading Silver {path}", flush=True)
    return spark.read.format("delta").load(path)


def enforce_unique(df: DataFrame, columns: list[str], name: str):
    if df.groupBy(*columns).count().filter("count > 1").limit(1).count():
        df.groupBy(*columns).count().filter("count > 1").show(20, truncate=False)
        raise ValueError(f"Duplicate keys in {name}: {columns}")


def _same_published_data(new: DataFrame, old: DataFrame) -> bool:
    """Avoid a Delta commit and new Parquet files on identical reruns.

    loaded_at is audit metadata, not a business-data change. Compare full row
    multisets (not just counts) to avoid silently missing corrections.
    """
    if new.schema != old.schema:
        return False
    cols = [c for c in new.columns if c != "loaded_at"]
    left = new.select(*cols)
    right = old.select(*cols)
    return (left.exceptAll(right).limit(1).count() == 0 and
            right.exceptAll(left).limit(1).count() == 0)


# New single-fact model. Shared tables are read-only.
from jobs.silver.clean_economic import category
SILVER_IDS = {'economic_statistics':'economic_statistic_id', 'labour_statistics':'labour_statistic_id', 'enterprise_statistics':'enterprise_statistic_id'}
GOLD_TABLES = {'economic': {n:f'{GOLD}/{n}' for n in ('dim_industry','dim_occupation','dim_economic_indicator','fact_economic_statistics')}, 'shared': {n:f's3a://gold/shared/{n}' for n in ('dim_date','dim_location')}}
FACT='fact_economic_statistics'
GRAIN=['date_key','location_key','industry_key','occupation_key','indicator_key']
FACT_COLUMNS=['statistic_fact_key',*GRAIN,'indicator_value','previous_year_value','growth_rate','absolute_change','value_status','notes','source_statistic_id','loaded_at']

def read_observations(spark):
    frames=[]
    for table, id_col in SILVER_IDS.items():
        d=read_silver(spark,table)
        required(d,[id_col,'year','location_id','industry_id','industry_scope','indicator_code','indicator_name','indicator_type','indicator_value','unit','aggregation_method','value_status','notes'],table)
        if table!='labour_statistics':
            d=d.withColumn('occupation_id',F.lit('NOT_APPLICABLE')).withColumn('occupation_scope',F.lit('NOT_APPLICABLE'))
        required(d,['occupation_id','occupation_scope'],table)
        d=d.withColumnRenamed(id_col,'source_statistic_id').withColumn('indicator_group',F.lit(table.replace('_statistics','')))
        frames.append(d.select('source_statistic_id','year','location_id','industry_id','industry_scope','occupation_id','occupation_scope','indicator_code','indicator_name','indicator_type','indicator_value','unit','aggregation_method','value_status','notes','indicator_group'))
    d=reduce(lambda a,b:a.unionByName(b),frames).cache()
    for c in ['source_statistic_id','year','location_id','industry_id','occupation_id','indicator_code','indicator_name','indicator_type','unit','aggregation_method','value_status']:
        if d.filter(F.col(c).isNull()).limit(1).count(): raise ValueError(f'Null {c}')
    if d.filter(~F.col('value_status').isin('ACTUAL','ESTIMATED','DERIVED','UNVERIFIED','MISSING') | (F.col('indicator_value').isNull() != (F.col('value_status')=='MISSING'))).limit(1).count(): raise ValueError('Invalid value status')
    enforce_unique(d,['source_statistic_id'],'Silver lineage')
    for kind in ('industry','occupation'):
        i=F.col(kind+'_id'); scope=F.col(kind+'_scope')
        if d.filter(scope.isNull() | ~scope.isin('ALL','NOT_APPLICABLE','UNKNOWN','SPECIFIC') | ((scope=='SPECIFIC') & i.isin('ALL','NOT_APPLICABLE','UNKNOWN')) | ((scope!='SPECIFIC') & (scope!=i))).limit(1).count(): raise ValueError(f'Invalid {kind} scope')
    return d

def category_dimension(spark,d,kind):
    # Silver has IDs only. Recover source labels using the exact Silver category function.
    sources={'industry':('investment','labour_structure','productivity'),'occupation':('occupation',)}[kind]
    labels={}
    for table in sources:
        for row in spark.read.format('delta').load(f's3a://bronze/economic/{table}').select(kind).distinct().collect():
            label=row[0]; identity=category('IND' if kind=='industry' else 'OCC',label)
            if identity!='ALL': labels.setdefault(identity,set()).add(label.strip())
    names={'ALL':'Tất cả ngành' if kind=='industry' else 'Tất cả nghề','NOT_APPLICABLE':'Không áp dụng','UNKNOWN':'Không xác định'}
    ids={r[0] for r in d.select(kind+'_id').distinct().collect()} | set(names)
    rows=[]
    for identity in sorted(ids):
        if identity not in names and identity not in labels: raise ValueError(f'Missing label {kind}: {identity}')
        label=names[identity] if identity in names else sorted(labels[identity])[0]
        # No official classification code in source; use the stable source ID as internal code.
        rows.append((identity,identity,label))
    frame=spark.createDataFrame(rows,[kind+'_id',kind+'_code',kind+'_name'])
    return frame.select(hash_key(kind+'_id').alias(kind+'_key'),*frame.columns)

def make_fact(d):
    series=GRAIN[1:]; enforce_unique(d,GRAIN,'fact grain')
    w=Window.partitionBy(*series).orderBy('date_key')
    d=d.withColumn('_prev_date',F.lag('date_key').over(w)).withColumn('_prev_value',F.lag('indicator_value').over(w))
    d=d.withColumn('previous_year_value',F.when(F.col('_prev_date')==F.col('date_key')-10000,F.col('_prev_value')))
    prev=F.col('previous_year_value'); val=F.col('indicator_value')
    # Relative change is valid for rates and shares as well as levels.
    # INDEX growth remains disabled until its reference basis is verified.
    comparable=F.upper(F.col('indicator_type')).isin('COUNT','AMOUNT','AVERAGE','RATE','SHARE')
    return (d.withColumn('absolute_change',val-prev)
        .withColumn('growth_rate',F.when(comparable & (prev!=0),((val-prev)/F.abs(prev)*100).cast('double')))
        .withColumn('statistic_fact_key',hash_key(*GRAIN)).withColumn('loaded_at',F.current_timestamp()).select(*FACT_COLUMNS))

def candidate(spark):
    d=read_observations(spark)
    tables={n:spark.read.format('delta').load(path) for n,path in GOLD_TABLES['shared'].items()}
    tables['dim_industry']=category_dimension(spark,d,'industry')
    tables['dim_occupation']=category_dimension(spark,d,'occupation')
    cols=['indicator_code','indicator_name','indicator_group','indicator_type','unit','aggregation_method']
    dim=d.select(*cols).distinct(); enforce_unique(dim,['indicator_code'],'indicator metadata')
    tables['dim_economic_indicator']=dim.select(hash_key('indicator_code').alias('indicator_key'),*cols,F.lit('ANNUAL').alias('frequency'))
    for name,identity,key in [('dim_location','location_id','location_key'),('dim_industry','industry_id','industry_key'),('dim_occupation','occupation_id','occupation_key'),('dim_economic_indicator','indicator_code','indicator_key')]:
        mapping=tables[name].select(identity,key); enforce_unique(mapping,[identity],name)
        d=d.join(F.broadcast(mapping),identity,'left')
    d=d.withColumn('date_key',F.col('year')*10000+101)
    tables[FACT]=make_fact(d).cache()
    validate_outputs(tables,d.count())
    return tables

def validate_outputs(tables,total):
    fact=tables[FACT]
    for name,df in tables.items():
        pk='statistic_fact_key' if name==FACT else ('indicator_key' if name=='dim_economic_indicator' else name[4:]+'_key')
        enforce_unique(df,[pk],name)
        if df.filter(F.col(pk).isNull()).limit(1).count(): raise ValueError(f'{name}: null PK')
    enforce_unique(fact,GRAIN,'fact grain'); enforce_unique(fact,['source_statistic_id'],'fact lineage')
    if fact.count()!=total: raise ValueError('Silver/Gold count mismatch')
    for fk,dim in zip(GRAIN,['dim_date','dim_location','dim_industry','dim_occupation','dim_economic_indicator']):
        if fact.filter(F.col(fk).isNull()).limit(1).count() or fact.join(tables[dim].select(fk),fk,'left_anti').limit(1).count(): raise ValueError(f'Missing FK {fk}; rebuild shared first')
    print(f'DQ PASS: {total} observations',flush=True)

def build(dry_run=False,replace_schema=False):
    spark=spark_session()
    try:
        tables=candidate(spark); writes=[]
        for name,path in GOLD_TABLES['economic'].items():
            df=tables[name]; hp=spark._jvm.org.apache.hadoop.fs.Path(path); fs=hp.getFileSystem(spark._jsc.hadoopConfiguration())
            print(f'{name}: {df.count()} rows',flush=True)
            if fs.exists(hp):
                old=spark.read.format('delta').load(path)
                if _same_published_data(df,old): continue
                if {f.name:f.dataType for f in old.schema}!={f.name:f.dataType for f in df.schema} and not (replace_schema or dry_run): raise ValueError(f'{name}: run --dry-run then --replace-schema after backup')
            writes.append((path,df))
        if not dry_run:
            for path,df in writes: df.write.format('delta').mode('overwrite').option('overwriteSchema','true').save(path)
        print({'dry_run':dry_run,'tables_to_write':len(writes)},flush=True)
    finally: spark.stop()

def register_only():
    # Publish existing Delta tables through the same Trino API as Jobs.
    from jobs.gold.register_jobs_trino import TrinoSQL, sql_literal
    client = TrinoSQL()
    try:
        q = client.query
        q("CREATE SCHEMA IF NOT EXISTS delta.economic WITH (location = 's3://gold/economic/')")
        for name, raw_path in GOLD_TABLES['economic'].items():
            path = raw_path.replace('s3a://', 's3://', 1).rstrip('/')
            matches = q("SELECT table_type FROM delta.information_schema.tables WHERE table_schema = 'economic' AND table_name = " + sql_literal(name))
            if not matches:
                q("CALL delta.system.register_table(schema_name => 'economic', table_name => " + sql_literal(name) + ", table_location => " + sql_literal(path) + ")")
            elif matches != [['BASE TABLE']]:
                raise ValueError(f'{name}: existing object is not a base table')
            ddl = q(f'SHOW CREATE TABLE delta.economic.{name}')[0][0]
            locations = re.findall(r"\blocation\s*=\s*'([^']*)'", ddl, re.IGNORECASE)
            if len(locations) != 1 or locations[0].replace('s3a://', 's3://', 1).rstrip('/') != path:
                raise ValueError(f'{name}: registered path differs from {path}')
            q("CALL delta.system.flush_metadata_cache(schema_name => 'economic', table_name => " + sql_literal(name) + ")")
            print(f'[TRINO] delta.economic.{name}: {q(f"SELECT count(*) FROM delta.economic.{name}")[0][0]} rows', flush=True)
    finally:
        client.close()

if __name__=='__main__':
    p=argparse.ArgumentParser(); p.add_argument('--dry-run',action='store_true'); p.add_argument('--replace-schema',action='store_true'); p.add_argument('--register-only',action='store_true'); a=p.parse_args()
    if a.register_only: register_only()
    else: build(a.dry_run,a.replace_schema)
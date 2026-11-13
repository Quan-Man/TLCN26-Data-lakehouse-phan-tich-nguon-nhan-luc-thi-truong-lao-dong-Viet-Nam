"""Official 18-table Bronze Delta ingestion; safe first write, guarded overwrite."""
import csv
import hashlib
import json
import os
import re
from pathlib import Path
from datetime import datetime, timezone
from pyspark.sql import SparkSession, types as T

RAW_DIR = Path('/opt/airflow/data/economy')
BRONZE_ROOT = 's3a://bronze/economic'
PRODUCTION_ROOT = BRONZE_ROOT
EXPECTED_TOTAL_RAW_ROWS = 204
SOURCE_NUMBERS = range(1, 19)
TABLES = {
 1:'gdp',2:'gdp_growth',3:'new_enterprises',4:'investment',5:'labour_force',
 6:'employment',7:'unemployment',8:'income',9:'iip',10:'cpi',11:'fdi',
 12:'retail_sales',13:'enterprise_count',14:'underemployment',15:'occupation',
 16:'labour_structure',17:'training',18:'productivity'
}
# Retain grouping of original tested pipeline; avoid changing Gold semantics.
SOURCE_GROUPS = {1:'economic',2:'economic',3:'enterprise',4:'enterprise',5:'labour',
 6:'labour',7:'labour',8:'labour',9:'economic',10:'economic',11:'enterprise',
 12:'economic',13:'enterprise',14:'labour',15:'labour',16:'labour',17:'labour',18:'labour'}
META = ['raw_id','source_file','source_row_number','source_record_hash','data_group','ingestion_batch_id','loaded_at']

def get_source_files(input_dir):
    root=Path(input_dir)
    if not root.is_dir(): raise FileNotFoundError(root)
    grouped={}
    for p in root.glob('*.csv'):
        m=re.match(r'^(\d+)_',p.name)
        if m and int(m.group(1)) in SOURCE_NUMBERS:
            grouped.setdefault(int(m.group(1)),[]).append(p)
    missing=sorted(set(SOURCE_NUMBERS)-set(grouped))
    duplicate={k:[p.name for p in v] for k,v in grouped.items() if len(v)!=1}
    if missing or duplicate: raise ValueError(f'Expected 18 CSVs: missing={missing}, duplicates={duplicate}')
    return [(n,grouped[n][0]) for n in SOURCE_NUMBERS]

def read_source(number,path,batch_id,loaded_at):
    with path.open('r',encoding='utf-8-sig',newline='') as handle:
        reader=csv.DictReader(handle)
        if not reader.fieldnames: raise ValueError(f'No CSV header: {path.name}')
        headers=[(h or '').strip() for h in reader.fieldnames]
        valid=[h for h in headers if h]
        if len(valid)!=len(set(valid)): raise ValueError(f'Duplicate header: {path.name}')
        if set(valid)&set(META): raise ValueError(f'Source column conflicts with metadata: {path.name}')
        records=[]
        for line,row in enumerate(reader,start=2):
            if row.get(None) and any(str(v).strip() for v in row[None]):
                raise ValueError(f'Extra fields: {path.name} row {line}')
            if any(not k and v is not None and str(v).strip() for k,v in row.items()):
                raise ValueError(f'Nonempty cell under empty header: {path.name} row {line}')
            clean={k.strip():v for k,v in row.items() if k and k.strip()}
            if not any(v is not None and str(v).strip() for v in clean.values()): continue
            payload=json.dumps(clean,ensure_ascii=False,sort_keys=True)
            source_hash=hashlib.sha256(payload.encode('utf-8')).hexdigest()
            raw_id=hashlib.sha256(f'{path.name}|{line}|{source_hash}'.encode('utf-8')).hexdigest()
            records.append(tuple(clean.get(c) for c in valid)+(raw_id,path.name,line,source_hash,SOURCE_GROUPS[number],batch_id,loaded_at))
    return valid,records

def get_spark():
    access=os.getenv('MINIO_ROOT_USER') or os.getenv('AWS_ACCESS_KEY_ID')
    secret=os.getenv('MINIO_ROOT_PASSWORD') or os.getenv('AWS_SECRET_ACCESS_KEY')
    if not access or not secret: raise RuntimeError('Missing MinIO credentials')
    endpoint=os.getenv('MINIO_ENDPOINT','http://minio:9000')
    return (SparkSession.builder.appName('economic-18-raw-bronze-production')
       .config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension')
       .config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog')
       .config('spark.hadoop.fs.s3a.endpoint',endpoint)
       .config('spark.hadoop.fs.s3a.access.key',access)
       .config('spark.hadoop.fs.s3a.secret.key',secret)
       .config('spark.hadoop.fs.s3a.path.style.access','true')
       .config('spark.hadoop.fs.s3a.connection.ssl.enabled',str(endpoint.startswith('https://')).lower())
       .config('spark.hadoop.fs.s3a.aws.credentials.provider','org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
       .getOrCreate())

def load_economic_labour(input_dir=RAW_DIR, output=BRONZE_ROOT, batch_id=None,
                         allow_production_overwrite=False, **context):
    output = str(output).rstrip('/')
    if output != PRODUCTION_ROOT:
        raise ValueError(f'Unsupported output root: {output}')
    files=get_source_files(input_dir)
    run_id=context.get('run_id') or 'manual'
    batch_id=batch_id or hashlib.sha256(str(run_id).encode()).hexdigest()[:24]
    loaded_at=datetime.now(timezone.utc).replace(tzinfo=None)
    prepared=[]
    for n,path in files:
        cols,rows=read_source(n,path,batch_id,loaded_at)
        if not rows: raise ValueError(f'No data rows: {path.name}')
        prepared.append((n,path,cols,rows))
    ids=[r[-7] for _,_,_,rows in prepared for r in rows]
    if len(ids)!=len(set(ids)): raise ValueError('Duplicate raw IDs')
    total_raw_rows = sum(len(rows) for _, _, _, rows in prepared)
    if total_raw_rows != EXPECTED_TOTAL_RAW_ROWS:
        raise ValueError(f'Raw row count changed: expected {EXPECTED_TOTAL_RAW_ROWS}, got {total_raw_rows}. Review input CSVs before writing.')
    spark=get_spark()
    try:
        # Do not overwrite any existing official table unless both explicit gates are set.
        jconf = spark._jsc.hadoopConfiguration()
        jfs = spark._jvm.org.apache.hadoop.fs.FileSystem
        jpath = spark._jvm.org.apache.hadoop.fs.Path
        existing = []
        for name in TABLES.values():
            dest = f'{output}/{name}'
            path_obj = jpath(dest)
            fs = path_obj.getFileSystem(jconf)
            if fs.exists(path_obj):
                existing.append(dest)
        overwrite_approved = (
            allow_production_overwrite is True
            and os.getenv('ECONOMIC_PRODUCTION_OVERWRITE') == 'YES_I_HAVE_BACKUP'
        )
        if existing and not overwrite_approved:
            raise RuntimeError(
                'Official Bronze tables already exist. Refusing overwrite: '
                + ', '.join(existing)
                + '. Back up first; then set allow_production_overwrite=True '
                + 'and ECONOMIC_PRODUCTION_OVERWRITE=YES_I_HAVE_BACKUP.'
            )
        totals={}
        for n,path,cols,rows in prepared:
            schema=T.StructType([T.StructField(c,T.StringType(),True) for c in cols]+[
                T.StructField('raw_id',T.StringType(),False),
                T.StructField('source_file',T.StringType(),False),
                T.StructField('source_row_number',T.IntegerType(),False),
                T.StructField('source_record_hash',T.StringType(),False),
                T.StructField('data_group',T.StringType(),False),
                T.StructField('ingestion_batch_id',T.StringType(),False),
                T.StructField('loaded_at',T.TimestampType(),False)])
            dest=f'{output}/{TABLES[n]}'
            df=spark.createDataFrame(rows,schema)
            mode = 'overwrite' if overwrite_approved else 'errorifexists'
            df.write.format('delta').mode(mode).option('overwriteSchema','true').save(dest)
            actual=spark.read.format('delta').load(dest).count()
            if actual!=len(rows): raise RuntimeError(f'Count mismatch {path.name}: {actual}/{len(rows)}')
            totals[path.name]=actual
            print(f'{path.name}: {actual} raw rows -> {dest}')
        print('TOTAL RAW ROWS:',sum(totals.values()))
        return totals
    finally: spark.stop()

if __name__=='__main__':
    print(load_economic_labour())

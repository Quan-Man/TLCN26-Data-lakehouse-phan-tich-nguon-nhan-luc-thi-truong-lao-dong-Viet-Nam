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

BRONZE_ROOT = 's3a://bronze/economic'
SILVER_ROOT = 's3a://silver/economic'
MAX_OBSERVATIONS = int(os.getenv('ECONOMIC_DQ_MAX_OBSERVATIONS', '100000'))
TABLES = {1:'gdp',2:'gdp_growth',3:'new_enterprises',4:'investment',5:'labour_force',
          6:'employment',7:'unemployment',8:'income',9:'iip',10:'cpi',11:'fdi',
          12:'retail_sales',13:'enterprise_count',14:'underemployment',
          15:'occupation',16:'labour_structure',17:'training',18:'productivity'}
GROUPS = {1:'economic',2:'economic',3:'enterprise',4:'enterprise',5:'labour',
          6:'labour',7:'labour',8:'labour',9:'economic',10:'economic',
          11:'enterprise',12:'economic',13:'enterprise',14:'labour',
          15:'labour',16:'labour',17:'labour',18:'labour'}
META_COLUMNS = {'year','unit','notes','location','industry','occupation','area_type',
                'region','indicator','project_count_unit','capital_unit',
                'employment_unit','share_unit','geography_version'}
RAW_METADATA = {'raw_id','source_file','source_row_number','source_record_hash',
                'data_group','ingestion_batch_id','loaded_at'}
FIXED_MEASURES = {1:{'gdp_total_value','agriculture_value','industry_construction_value',
                     'service_value','product_tax_value','gdp_total_share',
                     'agriculture_share','industry_construction_share',
                     'service_share','product_tax_share'},
                  11:{'project_count','registered_capital_usd_million'}}
YEAR_COLUMN = re.compile(r'^(.+)_20\d{2}$')
SILVER_KEYS = {'economic':'economic_statistic_id',
               'labour':'labour_statistic_id',
               'enterprise':'enterprise_statistic_id'}
COMMON = {'indicator_id','indicator_name','location_id','location_name','year',
          'indicator_value','unit','source_raw_id','source_column','source_file',
          'ingestion_batch_id','loaded_at'}
OPTIONAL_DIMENSIONS = ('industry','occupation','residence_type')
MISSING_TOKENS = {'', 'null','none','nan','n/a','chưa có','chưa công bố','không rõ','-','--'}
SEMANTIC = {'source_location_name','location_level','is_geographic_aggregate',
            'industry_scope','occupation_scope','residence_type_scope','value_status',
            'indicator_type','aggregation_method'}
COUNTRY = {'việt nam', 'cả nước', 'toàn quốc', 'viet nam', 'ca nuoc', 'toan quoc', 'vn'}
REGIONS = {'đồng bằng sông hồng', 'trung du và miền núi phía bắc',
           'bắc trung bộ và duyên hải miền trung', 'tây nguyên',
           'đông nam bộ', 'đồng bằng sông cửu long'}
TOTALS = {'tổng số','tong so','tổng cộng','total'}


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
    if MAX_OBSERVATIONS < 1:
        fail('ECONOMIC_DQ_MAX_OBSERVATIONS must be positive')
    spark = spark_session()
    try:
        # Read each small Bronze table independently. Do not create a giant union plan.
        expected = {group:{} for group in SILVER_KEYS}
        raw_total = 0
        for number, table in TABLES.items():
            df = spark.read.format('delta').load(f'{BRONZE_ROOT}/{table}')
            missing = RAW_METADATA - set(df.columns)
            if missing:
                fail(f'Bronze {table} missing metadata: {sorted(missing)}')
            measures = [c for c in df.columns
                        if c not in RAW_METADATA and c not in META_COLUMNS]
            if not measures:
                fail(f'Bronze {table}: no measurement columns')
            for col in measures:
                if not YEAR_COLUMN.fullmatch(col) and col not in FIXED_MEASURES.get(number,set()):
                    fail(f'Bronze {table}: unexpected measurement {col}')
            # Source CSVs are small; avoid huge Spark plans by collecting one table at a time.
            rows = bounded_rows(df, f'Bronze {table}', MAX_OBSERVATIONS)
            raw_total += len(rows)
            seen_raw_ids = set()
            group = GROUPS[number]
            for row in rows:
                raw_id = row['raw_id']
                if not raw_id or raw_id in seen_raw_ids:
                    fail(f'Bronze {table}: null/duplicate raw_id: {raw_id}')
                seen_raw_ids.add(raw_id)
                if row['data_group'] != group:
                    fail(f'Bronze {table}: invalid data_group {row["data_group"]}')
                if not nonempty(row['source_file']):
                    fail(f'Bronze {table}: missing source_file')
                for col in measures:
                    value = row[col]
                    if not nonempty(value):
                        continue
                    key = (raw_id, col)
                    if key in expected[group]:
                        fail(f'Duplicate Bronze measurement: {key}')
                    expected[group][key] = {
                        'source_file': row['source_file'],
                        'ingestion_batch_id': row['ingestion_batch_id'],
                        'loaded_at': row['loaded_at'],
                        'raw_value': str(value).strip(),
                        'expected_stat_id': expected_id(raw_id, col),
                        'source_residence_type': row.get('area_type') if number == 14 else None,
                        'source_location_name': (row.get('indicator') if number == 10 else
                                                 row.get('region') if number == 14 else
                                                 row.get('location')),
                    }
                    if sum(map(len, expected.values())) > MAX_OBSERVATIONS:
                        fail(f'Bronze observations exceed {MAX_OBSERVATIONS} limit')
            print(f'BRONZE OK: {table}: rows={len(rows)}, measures={len(measures)}', flush=True)

        total = 0
        for group, key_column in SILVER_KEYS.items():
            path = f'{SILVER_ROOT}/{group}_statistics'
            df = spark.read.format('delta').load(path)
            required = COMMON | SEMANTIC | {key_column}
            if group == 'labour':
                required |= {'occupation_id','residence_type_id','industry_id'}
            if group in ('economic', 'enterprise'):
                required.add('industry_id')
            if group == 'economic':
                required |= {'period_type','geography_version'}
            missing = required - set(df.columns)
            if missing:
                fail(f'Silver {group} missing columns: {sorted(missing)}')
            # Only select necessary DQ fields to reduce Delta IO and planning.
            fields = sorted(required | {
                name for dim in ('location','indicator','industry','occupation','residence_type')
                for name in (dim+'_id',dim+'_name') if name in df.columns
            })
            rows = bounded_rows(df.select(*fields), f'Silver {group}', MAX_OBSERVATIONS)
            seen = set()
            seen_ids = set()
            null_values = 0
            null_dims = Counter()
            for row in rows:
                key = (row['source_raw_id'], row['source_column'])
                if None in key or key in seen:
                    fail(f'Silver {group}: null/duplicate source lineage: {key}')
                seen.add(key)
                statistic_id = row[key_column]
                if not statistic_id or statistic_id in seen_ids:
                    fail(f'Silver {group}: null/duplicate statistic ID: {statistic_id}')
                seen_ids.add(statistic_id)
                origin = expected[group].get(key)
                if origin is None:
                    fail(f'Silver {group}: extra lineage: {key}')
                if statistic_id != origin['expected_stat_id']:
                    fail(f'Silver {group}: incorrect stable ID for {key}')
                for field in ('source_file','ingestion_batch_id','loaded_at'):
                    if row[field] != origin[field]:
                        fail(f'Silver {group}: {field} mismatch for {key}')
                if not row['indicator_id'] or not row['indicator_name']:
                    fail(f'Silver {group}: missing indicator for {key}')
                if row['year'] is None or not 2000 <= row['year'] <= 2100:
                    fail(f'Silver {group}: invalid year for {key}')
                for dim in ('location','indicator','industry','occupation','residence_type'):
                    name_col, id_col = dim+'_name', dim+'_id'
                    if name_col in row and id_col in row:
                        if (row[name_col] is None) != (row[id_col] is None):
                            fail(f'Silver {group}: inconsistent {dim} name/id for {key}')
                if row.get('location_id') is None:
                    fail(f'Silver {group}: missing location_id for {key}')
                # Validate new semantic fields against canonical Silver rules.
                location = str(row['location_name']).strip().lower()
                level = ('COUNTRY' if location == 'việt nam' else
                         'REGION' if location in REGIONS else 'PROVINCE')
                if row['location_level'] != level:
                    fail(f'Silver {group}: location_level mismatch for {key}: '
                         f'{row["location_level"]!r}, expected {level}')
                if row['is_geographic_aggregate'] is not (level != 'PROVINCE'):
                    fail(f'Silver {group}: geographic aggregate mismatch for {key}')
                original_location = origin['source_location_name']
                original_location = (str(original_location).strip() if nonempty(original_location)
                                     else None)
                if row['source_location_name'] != original_location:
                    fail(f'Silver {group}: original location lineage mismatch for {key}')
                industry = row.get('industry_name')
                industry_scope = ('NOT_APPLICABLE' if industry is None else
                                  'ALL_INDUSTRIES' if str(industry).strip().lower() in TOTALS
                                  else 'SPECIFIC_INDUSTRY')
                if row['industry_scope'] != industry_scope:
                    fail(f'Silver {group}: industry_scope mismatch for {key}')
                occupation = row.get('occupation_name')
                occupation_scope = ('NOT_APPLICABLE' if occupation is None else
                                    'ALL_OCCUPATIONS' if str(occupation).strip().lower() in TOTALS
                                    else 'SPECIFIC_OCCUPATION')
                if row['occupation_scope'] != occupation_scope:
                    fail(f'Silver {group}: occupation_scope mismatch for {key}')
                residence = row.get('residence_type_name')
                raw_residence = origin.get('source_residence_type')
                national_residence = (origin['source_file'] == '14_thieu_viec.csv'
                                      and raw_residence is not None
                                      and str(raw_residence).strip().lower() in COUNTRY)
                residence_scope = ('ALL_RESIDENCE_TYPES' if national_residence else
                                   'NOT_APPLICABLE' if residence is None else
                                   'ALL_RESIDENCE_TYPES' if str(residence).strip().lower() in TOTALS else
                                   'SPECIFIC_RESIDENCE_TYPE')
                if row['residence_type_scope'] != residence_scope:
                    fail(f'Silver {group}: residence_type_scope mismatch for {key}: '
                         f'{row["residence_type_scope"]!r} expected {residence_scope!r}')
                expected_status = 'MISSING_SOURCE' if row['indicator_value'] is None else 'VALID'
                if row['value_status'] != expected_status:
                    fail(f'Silver {group}: value_status mismatch for {key}')
                indicator = row['indicator_name']
                indicator_type = ('SHARE' if indicator.endswith('_share') else
                                  'RATE' if 'rate' in indicator else
                                  'INDEX' if indicator in ('gdp_index','iip','cpi') else
                                  'AMOUNT' if re.search(
                                      r'capital|investment|income|gdp_total_value|_value$|retail_sales|productivity',
                                      indicator, re.I) else 'COUNT')
                if row['indicator_type'] != indicator_type:
                    fail(f'Silver {group}: indicator_type mismatch for {key}')
                aggregation = ('NONE' if indicator_type in ('RATE','SHARE','INDEX')
                               or indicator == 'productivity' else 'SUM')
                if row['aggregation_method'] != aggregation:
                    fail(f'Silver {group}: aggregation_method mismatch for {key}')
                for dim in OPTIONAL_DIMENSIONS:
                    if row.get(dim+'_id') is None:
                        null_dims[dim] += 1
                if row['indicator_value'] is None:
                    null_values += 1
                    if origin['raw_value'].lower() not in MISSING_TOKENS:
                        fail(f'Silver {group}: numeric conversion lost value for {key}: '
                             f'{origin["raw_value"]!r}')
                else:
                    # Numeric parsing is validated in Silver transformation; this
                    # DQ checks presence and source lineage without reinterpreting units.
                    try:
                        Decimal(str(row['indicator_value']))
                    except (InvalidOperation, ValueError):
                        fail(f'Silver {group}: invalid numeric value for {key}')
            absent = expected[group].keys() - seen
            if absent:
                fail(f'Silver {group}: missing {len(absent)} source measurements; '
                     f'first={next(iter(absent))}')
            if len(rows) != len(expected[group]):
                fail(f'Silver {group}: row count mismatch')
            total += len(rows)
            print(f'DQ PASS: {group}_statistics={len(rows)}; '
                  f'NULL indicator_value={null_values}; optional NULLs={dict(null_dims)}',
                  flush=True)
        print(f'DQ PASS: Bronze raw rows={raw_total}; Silver observations={total}',
              flush=True)
        return {'bronze_raw_rows': raw_total, 'silver_observations': total}
    finally:
        spark.stop()


if __name__ == '__main__':
    print(check())

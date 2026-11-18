from __future__ import annotations
import argparse
import json
import os
from pathlib import Path
import re
import sys
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'jobs'))
sys.path.insert(0, str(ROOT / 'jobs' / 'gold'))
from common.dq_rules import load_rules, evaluate_tables
from common.audit import _atomic_write
from dq.gold_integrity import evaluate as check_integrity
import build_job_bridges as bridges
import build_shared_dimensions as shared

TABLE_DIR = ROOT / 'data' / 'gold'
SHARED_ROOT = os.getenv('SHARED_GOLD_ROOT', 's3a://gold/shared')
GOLD_ROOT = os.getenv('JOBS_GOLD_ROOT', 's3a://gold/jobs')
DEFAULT_SILVER_ROOT = 's3a://silver/jobs'
DIMENSIONS = {
    'dim_date': ('date_key', ['date_key', 'full_date', 'day_of_month', 'day_of_week', 'day_name', 'week_of_year', 'month_number', 'month_name', 'quarter_number', 'year_number', 'is_weekend', 'is_holiday']),
    'dim_location': ('location_id', ['location_id', 'location_name', 'location_type', 'province_name', 'region_name', 'valid_from', 'valid_to', 'is_current']),
    'dim_company': ('company_id', ['company_id', 'company_name', 'company_name_normalized', 'company_size', 'industry_id', 'is_active', 'source_name']),
    #'dim_occupation': ('occupation_id', ['occupation_id', 'occupation_name', 'occupation_group', 'occupation_family', 'is_active']),
    #'dim_industry': ('industry_id', ['industry_id', 'industry_name', 'parent_industry_id', 'industry_level', 'is_active']),
    'dim_skill': ('skill_id', ['skill_id', 'skill_name', 'skill_category', 'skill_type', 'is_active']),
}
FACT_COLUMNS = ['job_id', 'date_key', 'company_key', 'occupation_key', 'industry_key', 'location_key', 'source_name', 'job_title', 'job_type', 'experience_level', 'education_level', 'salary_min', 'salary_max', 'salary_currency', 'salary_period', 'is_salary_disclosed', 'posting_count', 'is_active', 'crawled_at', 'valid_through']
SKILL_COLUMNS = ['job_posting_key', 'skill_key', 'evidence_field', 'matched_text', 'raw_skill_text', 'is_required', 'required_level', 'years_required', 'extraction_method', 'confidence_score', 'validation_status', 'extractor_version']
# Tạm tắt aggregate đến khi cần nạp các bảng tổng hợp.
# AGGREGATES = {
#     'agg_labor_demand': ['year_month', 'location_id', 'industry_id', 'posting_count', 'hiring_company_count', 'average_salary_min'],
#     'agg_skill_demand': ['skill_id', 'skill_name', 'skill_category', 'posting_count', 'hiring_company_count', 'required_mention_count'],
# }
REQUIRED = [
    *DIMENSIONS,
    'fact_job_posting',
    'fact_job_skill',
    # *AGGREGATES,
]


def resolve_input_candidates(name, directory):
    roots = [str(directory)]
    if name in DIMENSIONS and name != 'dim_company':
        roots.extend([SHARED_ROOT.rstrip('/'), str(ROOT / 'data' / 'gold' / 'shared')])
    else:
        roots.extend([GOLD_ROOT.rstrip('/'), str(ROOT / 'data' / 'gold' / 'jobs')])
        if name == 'dim_company':
            roots.extend([SHARED_ROOT.rstrip('/'), str(ROOT / 'data' / 'gold' / 'shared')])

    candidates = []
    for root in roots:
        if root.startswith('s3://'):
            root = 's3a://' + root[len('s3://'):]
        if root.startswith(('s3://', 's3a://')):
            root = root.rstrip('/')
            candidates.extend([f'{root}/{name}', f'{root}/{name}.parquet'])
        else:
            candidates.extend([str(Path(root) / f'{name}.parquet'), str(Path(root) / name)])
    return list(dict.fromkeys(candidates))


def read_table(path, spark=None):
    path = str(path)
    if path.startswith('s3://'):
        path = 's3a://' + path[len('s3://'):]
    if path.startswith('s3a://'):
        owns_spark = spark is None
        if owns_spark:
            spark = shared.make_spark('gold-load-postgres', use_s3=True)
        try:
            parquet = path.endswith('.parquet')
            probe = path if parquet else f'{path}/_delta_log'
            if not shared.hadoop_exists(spark, probe):
                raise FileNotFoundError(f'Missing Gold input: {probe}')
            frame = spark.read.format('parquet' if parquet else 'delta').load(path)
            rows = [row.asDict(recursive=True) for row in frame.collect()]
            if any(not isinstance(row, dict) for row in rows):
                raise ValueError(f'{path}: expected records as dictionaries')
            return rows
        finally:
            if owns_spark:
                spark.stop()

    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f'Missing Gold input: {path}')

    import pyarrow.dataset as ds

    try:
        dataset = ds.dataset(str(p), format='parquet')
        rows = dataset.to_table().to_pylist()
    except Exception as exc:
        raise ValueError(f'Cannot read Parquet dataset: {path}') from exc

    if any(not isinstance(row, dict) for row in rows):
        raise ValueError(f'{path}: expected Parquet records as dictionaries')
    return rows


def read_inputs(directory, *, silver_root=None):
    silver_root = str(silver_root or os.getenv('JOBS_SILVER_ROOT', DEFAULT_SILVER_ROOT)).rstrip('/')
    if silver_root.startswith('s3://'):
        silver_root = 's3a://' + silver_root[len('s3://'):]
    tables = {}
    spark = None
    try:
        for name in [*REQUIRED, 'bridge_job_location']:
            candidates = resolve_input_candidates(name, directory)
            for candidate in candidates:
                if candidate.startswith('s3a://') and spark is None:
                    spark = shared.make_spark('gold-load-postgres', use_s3=True)
                try:
                    tables[name] = read_table(candidate, spark=spark)
                except FileNotFoundError:
                    continue
                except Exception as exc:
                    raise RuntimeError(f'Cannot read {name} at {candidate}: {exc}') from exc
                break
            else:
                raise FileNotFoundError(f'Missing Gold input for {name}. Tried: {candidates}')

        # The integrity check compares Gold bridge rows against Silver locations.
        if spark is None:
            spark = shared.make_spark('gold-load-postgres', use_s3=silver_root.startswith('s3a://'))
        locations = bridges.read_silver_locations(
            spark, silver_root, lambda path: bridges.read_snapshot(spark, path)[0],
        )
        tables['job_locations'] = [row.asDict(recursive=True) for row in locations.collect()]
        return tables
    finally:
        if spark is not None:
            spark.stop()


def validate(tables):
    rules = load_rules(profile='gold_jobs_postgres')
    issues = check_integrity(tables)
    if not tables['fact_job_posting']:
        issues.append({'severity': 'error', 'code': 'EMPTY_JOB_SNAPSHOT', 'message': 'Refusing to replace jobs with an empty snapshot'})
    report = evaluate_tables(tables, rules, selected=REQUIRED, issues=issues)
    report['run_id'] = uuid4().hex
    report['profile'] = rules['profile']
    report_path = ROOT / 'data' / 'audit' / 'dq_job_postgres' / (report['run_id'] + '.json')
    _atomic_write(report_path, report)
    if not report['publish_allowed']:
        blocking = {'error', 'warning'} if rules['publish']['block_on_warn'] else {'error'}
        summary = ', '.join(
            f'{issue.get("table", "jobs")}.{issue["code"]} ({issue.get("count", "?")} rows)'
            for issue in report['issues'] if issue['severity'] in blocking
        )
        raise ValueError(f'Gold job DQ failed: {summary}. Report: {report_path}')
    return report


def create_statements(filename, names):
    sql = (ROOT / 'sql' / filename).read_text(encoding='utf-8-sig')
    result = []
    for name in names:
        match = re.search(r'CREATE TABLE IF NOT EXISTS gold\.' + re.escape(name) + r'\s*\(.*?\);', sql, re.S)
        if not match:
            raise ValueError(f'Missing DDL for {name}')
        result.append(match.group())
    return result


def insert(cursor, table, row, columns, conflict=None):
    selected = [c for c in columns if c in row]
    if not selected or (conflict and conflict not in selected):
        raise ValueError(f'{table}: missing insert key')
    sql = f"INSERT INTO gold.{table} ({', '.join(selected)}) VALUES ({', '.join(['%s'] * len(selected))})"
    if conflict:
        updates = [c for c in selected if c != conflict]
        sql += f' ON CONFLICT ({conflict}) ' + ('DO UPDATE SET ' + ', '.join(f'{c} = EXCLUDED.{c}' for c in updates) if updates else 'DO NOTHING')
    cursor.execute(sql, [row[c] for c in selected])


def postgres_dimension_rows(table, rows):
    # PostgreSQL's existing tables upsert by business ID. Choose the latest
    # version per ID explicitly and translate known column names at this boundary.
    if table == 'dim_date':
        return [dict(row, day_of_month=row.get('day_number', row.get('day_of_month')),
                     week_of_year=row.get('week_number', row.get('week_of_year')))
                for row in rows]
    id_column = table.removeprefix('dim_') + '_id'
    latest = {}
    for row in rows:
        identifier = row[id_column]
        if identifier not in latest or row['valid_from'] > latest[identifier]['valid_from']:
            latest[identifier] = row
    if table == 'dim_location':
        unknown = latest.get(shared.UNKNOWN_ID)
        if unknown is not None and unknown.get('location_type') is None:
            latest[shared.UNKNOWN_ID] = dict(unknown, location_type='unknown')
    if table == 'dim_skill':
        return [dict(row, skill_name=row['canonical_name']) for row in latest.values()]
    if table == 'dim_company':
        return [dict(row, company_name_normalized=row.get('normalized_name')) for row in latest.values()]
    return latest.values()


def apply(cursor, tables):
    cursor.execute('CREATE SCHEMA IF NOT EXISTS gold')
    for statement in create_statements('02_create_dimensions.sql', DIMENSIONS):
        cursor.execute(statement)
    for statement in create_statements('03_create_facts.sql', ['fact_job_posting', 'fact_job_skill']):
        cursor.execute(statement)
    qualified_tables = [f'gold.{name}' for name in REQUIRED]
    cursor.execute('GRANT USAGE ON SCHEMA gold TO bi_reader')
    cursor.execute(f"GRANT SELECT ON TABLE {', '.join(qualified_tables)} TO bi_reader")
    cursor.execute(
        "SELECT has_schema_privilege('bi_reader', 'gold', 'USAGE') "
        "AND bool_and(has_table_privilege('bi_reader', table_name, 'SELECT')) "
        "FROM unnest(%s::text[]) AS grant_targets(table_name)",
        (qualified_tables,),
    )
    if not cursor.fetchone()[0]:
        raise PermissionError('bi_reader cannot read the Gold jobs tables. Check schema and table grant permissions.')
    # cursor.execute('CREATE TABLE IF NOT EXISTS gold.agg_labor_demand (year_month integer, location_id text, industry_id text, posting_count bigint NOT NULL, hiring_company_count bigint NOT NULL, average_salary_min numeric(18,2))')
    # cursor.execute('CREATE TABLE IF NOT EXISTS gold.agg_skill_demand (skill_id text PRIMARY KEY, skill_name text NOT NULL, skill_category text, posting_count bigint NOT NULL, hiring_company_count bigint NOT NULL, required_mention_count bigint NOT NULL)')
    cursor.execute(
        "SELECT conrelid::regclass::text FROM pg_constraint WHERE contype = 'f' "
        "AND confrelid IN ("
        "'gold.fact_job_posting'::regclass, 'gold.fact_job_skill'::regclass"
        # ", 'gold.agg_labor_demand'::regclass, 'gold.agg_skill_demand'::regclass"
        ") "
        "AND NOT (confrelid = 'gold.fact_job_posting'::regclass AND conrelid = 'gold.fact_job_skill'::regclass)"
    )
    if cursor.fetchall():
        raise ValueError('Other tables reference gold.fact_job_posting; refusing snapshot replacement')
    for table in (
        # 'agg_skill_demand', 'agg_labor_demand',
        'fact_job_skill',
        'fact_job_posting',
    ):
        cursor.execute(f'DELETE FROM gold.{table}')
    for table, (key, columns) in DIMENSIONS.items():
        for row in postgres_dimension_rows(table, tables[table]):
            insert(cursor, table, row, columns, conflict=key)
    keys = {}
    for table in ('dim_company', 'dim_location', 'dim_skill'):
        key = DIMENSIONS[table][0]
        cursor.execute(f'SELECT {key}, {table[4:]}_key FROM gold.{table}')
        keys[table] = dict(cursor.fetchall())
    source_ids = {
        dim: {row[dim + '_key']: row[dim + '_id'] for row in tables['dim_' + dim]}
        for dim in ('company', 'location')
    }
    for row in tables['fact_job_posting']:
        data = dict(row, date_key=row['posted_date_key'], posting_count=1,
                    salary_min=row.get('salary_min_vnd'), salary_max=row.get('salary_max_vnd'),
                    is_salary_disclosed=row.get('has_salary'),
                    valid_through=row.get('expiration_date'))
        if row.get('has_salary'):
            data['salary_currency'] = 'VND'
        for dim in ('company', 'location'):
            identifier = source_ids[dim][row[dim + '_key']]
            data[dim + '_key'] = keys['dim_' + dim][identifier]
        # These two dimensions were disabled in the supplied PostgreSQL loader.
        # Their Delta version keys cannot be written as PostgreSQL surrogates.
        data.pop('occupation_key', None)
        data.pop('industry_key', None)
        insert(cursor, 'fact_job_posting', data, FACT_COLUMNS)
    cursor.execute('SELECT job_id, job_posting_key FROM gold.fact_job_posting')
    jobs = dict(cursor.fetchall())
    for row in tables['fact_job_skill']:
        data = dict(row, job_posting_key=jobs[row['job_id']], skill_key=keys['dim_skill'][row['skill_id']])
        insert(cursor, 'fact_job_skill', data, SKILL_COLUMNS)
    # for table, columns in AGGREGATES.items():
    #     for row in tables[table]:
    #         insert(cursor, table, row, columns)


def load(input_dir=TABLE_DIR, *, silver_root=None, validate_only=False):
    tables = read_inputs(input_dir, silver_root=silver_root)
    report = validate(tables)
    result = {'run_id': report['run_id'], 'rows': {name: len(tables[name]) for name in REQUIRED}}
    if validate_only:
        return result
    try:
        import psycopg2
    except ImportError as exc:
        raise RuntimeError('psycopg2 is required') from exc
    connection = psycopg2.connect(host=os.getenv('PGHOST', 'postgres'), port=int(os.getenv('PGPORT', '5432')),
        dbname=os.getenv('PGDATABASE', 'lakehouse'), user=os.getenv('PGUSER', 'lakehouse'),
        password=os.environ['PGPASSWORD'], connect_timeout=10)
    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT pg_advisory_xact_lock(hashtext('lakehouse.gold.job.snapshot'))")
                apply(cursor, tables)
    finally:
        connection.close()
    return result

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input-dir', default=str(TABLE_DIR))
    parser.add_argument('--silver-root', help='Jobs Silver root used by the location bridge integrity check')
    parser.add_argument('--validate-only', action='store_true', help='Validate Gold and write the DQ report without loading PostgreSQL')
    args = parser.parse_args()
    print(json.dumps(load(args.input_dir, silver_root=args.silver_root, validate_only=args.validate_only), ensure_ascii=False))


if __name__ == '__main__':
    main()
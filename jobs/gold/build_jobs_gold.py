import argparse
from datetime import date, datetime, timezone
import json
import logging
import os
from typing import Any

try:
    from . import build_shared_dimensions as shared
except ImportError:
    import build_shared_dimensions as shared

LOG = logging.getLogger(__name__)
COMPANY = shared.DimensionSpec('company', 'company_id', 'company_key', 'company_name',
    ('company_name', 'normalized_name', 'industry_key', 'location_key'), '')
OPEN_STATUSES = ('ACTIVE', 'OPEN', 'PUBLISHED', 'HIRING', 'DANG_TUYEN', 'ĐANG_TUYỂN', 'ĐANG TUYỂN')
CLOSED_STATUSES = ('CLOSED', 'EXPIRED', 'INACTIVE', 'REMOVED', 'DELETED', 'CANCELLED',
                   'CANCELED', 'ĐÃ ĐÓNG', 'ĐÃ HẾT HẠN')
REMOTE_MODELS = ('REMOTE', 'FULL_REMOTE', 'FULLY_REMOTE', 'WORK_FROM_HOME', 'LÀM VIỆC TỪ XA')
ONSITE_MODELS = ('ONSITE', 'ON_SITE', 'ON-SITE', 'OFFICE', 'HYBRID', 'TẠI VĂN PHÒNG')
NEGOTIABLE = ('NEGOTIABLE', 'UNDISCLOSED', 'NOT_DISCLOSED', 'THOA_THUAN', 'THỎA THUẬN')

TEXT_COLUMNS = ('job_id', 'source_name', 'job_title', 'company_id', 'primary_location_id',
    'industry_id', 'occupation_id', 'job_level', 'education_level', 'employment_type',
    'working_model', 'record_status', 'salary_status', 'source_url')
NUMBER_COLUMNS = ('salary_min_vnd', 'salary_max_vnd', 'salary_midpoint_vnd',
                  'experience_min_years', 'experience_max_years')
RAW_COLUMNS = (*TEXT_COLUMNS, 'posted_date', 'expiration_date', *NUMBER_COLUMNS, 'is_remote')
FK_COLUMNS = ('location_key', 'company_key')


def build_job_dimensions(spark=None, *, gold_root=None, shared_root=None,
                         skill_source=None, as_of=None, dry_run=False,
                         max_reference_rows=100000, return_rows=False):
    gold_root = (gold_root or os.getenv('JOBS_GOLD_ROOT', 's3a://gold/jobs')).rstrip('/')
    shared_root = (shared_root or os.getenv('SHARED_GOLD_ROOT', 's3a://gold/shared')).rstrip('/')
    mapping_root = os.getenv('SHARED_MAPPING_ROOT', '/opt/airflow/configs/mappings').rstrip('/')
    skill_source = skill_source or f'{mapping_root}/skill_dictionary.csv'
    path = f'{gold_root}/dim_skill'
    legacy_path = f'{shared_root}/dim_skill'
    if path == legacy_path or any(p.startswith('s3://') for p in (path, legacy_path, skill_source)):
        raise ValueError('Use distinct Gold roots and Spark s3a:// paths')
    spec = shared.SPECS['skill']
    schema = shared.dimension_schema(spec)
    as_of = shared.parse_date(as_of, 'as_of') or date.today()
    owns = spark is None
    spark = spark or shared.make_spark('gold-job-dimensions', any(p.startswith('s3a://') for p in (path, legacy_path, skill_source)))
    try:
        source = shared.bounded_rows(shared.read_catalog(spark, skill_source, spec), max_reference_rows)
        target = shared.existing_delta(spark, path, schema)
        old = shared.bounded_rows(target, max_reference_rows) if target is not None else None
        legacy = shared.existing_delta(spark, legacy_path, schema) if old is None else None
        previous = shared.bounded_rows(legacy, max_reference_rows) if legacy is not None else (old or [])
        rows = shared.reconcile_dimension(source, previous, spec, as_of)
        if len(rows) > max_reference_rows:
            raise ValueError('dim_skill: history exceeds max_reference_rows')
        written = False if dry_run else shared.merge_rows(spark, path, rows, schema, spec.key_column, old)
        report = {'dim_skill': {'rows': len(rows), 'path': path, 'written': written,
                'dry_run': dry_run, 'seeded_from_legacy': legacy is not None}}
        return (report, rows) if return_rows else report
    finally:
        if owns:
            spark.stop()



def normalize_s3a_root(path):
    value = str(path).strip()
    if not value:
        return value
    if value.startswith('s3://'):
        value = 's3a://' + value[5:]
    if value.startswith('s3a://'):
        return value.rstrip('/')
    return value.rstrip('/')


def resolve_silver_table_path(silver_root, table_name, exists):
    root = normalize_s3a_root(silver_root).rstrip('/')
    bare = table_name.removesuffix('.parquet').removesuffix('.delta')
    candidates = [
        f'{root}/current/{bare}.parquet',
        f'{root}/current/{bare}',
        f'{root}/{bare}.parquet',
        f'{root}/{bare}',
    ]
    for candidate in candidates:
        if exists(candidate):
            return candidate
    raise FileNotFoundError(
        f'Missing Silver table {bare!r} under {silver_root}. Expected one of: {candidates}'
    )


def load_silver_table(spark, silver_root, table_name, expected_schema=None):
    path = resolve_silver_table_path(
        silver_root,
        table_name,
        lambda candidate: shared.hadoop_exists(spark, candidate),
    )
    if path.endswith('.parquet'):
        frame = spark.read.format('parquet').load(path)
    else:
        from delta.tables import DeltaTable
        if not DeltaTable.isDeltaTable(spark, path):
            raise ValueError(f'Silver table exists but is not Delta: {path}')
        frame = spark.read.format('delta').load(path)
    if expected_schema is not None:
        expected = {field.name: field.dataType for field in expected_schema}
        actual = {field.name: field.dataType for field in frame.schema}
        if actual != expected:
            raise ValueError(f'Silver table {path} schema mismatch: expected {expected}, got {actual}')
    return frame


def require_columns(frame, columns, label):
    missing = set(columns) - set(frame.columns)
    if missing:
        raise ValueError(f'{label}: missing columns {sorted(missing)}')


def fail_if(frame, condition, message):
    if frame.filter(condition).limit(1).count():
        raise ValueError(message)


def unique(frame, columns, label):
    from pyspark.sql import functions as F
    if frame.groupBy(*columns).count().filter(F.col('count') > 1).limit(1).count():
        raise ValueError(f'{label}: conflicting duplicate key {columns}')


def clean(column):
    from pyspark.sql import functions as F
    value = F.trim(F.regexp_replace(column.cast('string'), r'\s+', ' '))
    return F.when(value != '', value)


def token(column):
    from pyspark.sql import functions as F
    return F.upper(clean(column))


def try_cast(column_name, datatype):
    from pyspark.sql import functions as F
    escaped = column_name.replace('`', '``')
    return F.expr(f'try_cast(`{escaped}` as {datatype})')


def job_key(column):
    from pyspark.sql import functions as F
    return F.sha2(F.to_json(F.array(F.lit('job_posting'), column.cast('string'))), 256)


def prepare_jobs(frame):
    """Strict typed Silver boundary. No salary/experience extraction from text."""
    from pyspark.sql import functions as F, types as T
    require_columns(frame, ('job_id', 'source_name', 'posted_date', 'expiration_date'), 'job_postings')
    if not isinstance(frame.schema['job_id'].dataType, T.StringType):
        raise ValueError('job_postings.job_id must be STRING; preserve source identity/leading zeros')
    if 'salary_type' in frame.columns:
        if 'salary_status' not in frame.columns:
            frame = frame.withColumnRenamed('salary_type', 'salary_status')
        else:
            frame = frame.withColumn('salary_status', F.coalesce(clean(F.col('salary_status')), clean(F.col('salary_type'))))
    frame = frame.select(*dict.fromkeys([c for c in RAW_COLUMNS if c in frame.columns]))
    for column in RAW_COLUMNS:
        if column not in frame.columns:
            frame = frame.withColumn(column, F.lit(None).cast('string'))
    for column in TEXT_COLUMNS:
        frame = frame.withColumn(column, clean(F.col(column)))
    fail_if(frame, F.col('job_id').isNull() | F.col('source_name').isNull(),
            'job_id and source_name must be nonempty')
    for column in ('posted_date', 'expiration_date'):
        frame = frame.withColumn('_parsed', try_cast(column, 'date'))
        fail_if(frame, clean(F.col(column)).isNotNull() & F.col('_parsed').isNull(),
                f'{column}: invalid Silver date; use ISO dates or NULL')
        frame = frame.drop(column).withColumnRenamed('_parsed', column)
    frame = frame.withColumn('salary_status', token(F.col('salary_status')))
    for column in NUMBER_COLUMNS:
        dtype = 'decimal(20,2)' if column.startswith('salary_') else 'decimal(8,2)'
        frame = frame.withColumn('_parsed', try_cast(column, dtype))
        fail_if(frame, clean(F.col(column)).isNotNull() & F.col('_parsed').isNull(),
                f'{column}: invalid/overflowing numeric Silver value')
        frame = frame.drop(column).withColumnRenamed('_parsed', column)
    # The status is authoritative for disclosed vs negotiable salary.
    for column in ('salary_min_vnd', 'salary_max_vnd', 'salary_midpoint_vnd'):
        frame = frame.withColumn(column, F.when(F.col('salary_status').isin(*NEGOTIABLE),
            F.lit(None).cast('decimal(20,2)')).otherwise(F.col(column)))
    frame = frame.withColumn('_remote', try_cast('is_remote', 'boolean'))
    fail_if(frame, clean(F.col('is_remote')).isNotNull() & F.col('_remote').isNull(),
            'is_remote must be a boolean or NULL')
    frame = frame.drop('is_remote').withColumnRenamed('_remote', 'is_remote')

    for prefix in ('salary', 'experience'):
        lo = 'salary_min_vnd' if prefix == 'salary' else 'experience_min_years'
        hi = 'salary_max_vnd' if prefix == 'salary' else 'experience_max_years'
        fail_if(frame, (F.col(lo) < 0) | (F.col(hi) < 0) | (F.col(lo) > F.col(hi)),
                f'{prefix}: negative or reversed bounds')
    # Compute midpoint only from a full interval, never from a one-sided bound.
    frame = frame.withColumn('salary_midpoint_vnd',
        F.when(F.col('salary_min_vnd').isNotNull() & F.col('salary_max_vnd').isNotNull(),
               ((F.col('salary_min_vnd') + F.col('salary_max_vnd')) / 2).cast('decimal(20,2)')))
    frame = frame.select(*RAW_COLUMNS).distinct()
    unique(frame, ['job_id'], 'job_postings')
    return frame


def prepare_skills(frame):
    from pyspark.sql import functions as F, types as T
    require_columns(frame, ('job_id', 'skill_id', 'is_required'), 'job_skills')
    for column in ('job_id', 'skill_id'):
        if not isinstance(frame.schema[column].dataType, T.StringType):
            raise ValueError(f'job_skills.{column} must be STRING')
    frame = frame.select(clean(F.col('job_id')).alias('job_id'),
                         clean(F.col('skill_id')).alias('skill_id'), 'is_required')
    frame = frame.withColumn('_required', try_cast('is_required', 'boolean'))
    fail_if(frame, F.col('job_id').isNull() | F.col('skill_id').isNull(),
            'job_skills: NULL/blank job_id or skill_id')
    fail_if(frame, clean(F.col('is_required')).isNotNull() & F.col('_required').isNull(),
            'job_skills.is_required must be true/false/NULL')
    frame = frame.drop('is_required').withColumnRenamed('_required', 'is_required').distinct()
    unique(frame, ['job_id', 'skill_id'], 'job_skills (conflicting required flags)')
    return frame


def activity_columns(frame, reference_date):
    from pyspark.sql import functions as F
    status = token(F.col('record_status'))
    ref = F.lit(reference_date).cast('date')
    # No missing-date sentinel. Expiry is inclusive: expiry == reference is valid.
    reason = (F.when(status.isin(*CLOSED_STATUSES), 'SOURCE_CLOSED')
        .when(F.col('expiration_date') < ref, 'EXPIRED_BY_DATE')
        .when(F.col('posted_date') > ref, 'NOT_YET_POSTED')
        .when(~F.col('seen_in_silver'), 'NOT_IN_SILVER_SNAPSHOT')
        .when(status.isin(*OPEN_STATUSES), 'SOURCE_OPEN')
        .otherwise('INSUFFICIENT_EVIDENCE'))
    frame = frame.withColumn('activity_status', reason)
    return frame.withColumn('is_active',
        F.when(F.col('activity_status').isin('SOURCE_CLOSED', 'EXPIRED_BY_DATE', 'NOT_YET_POSTED'), False)
        .when(F.col('activity_status') == 'SOURCE_OPEN', True)
        .otherwise(F.lit(None).cast('boolean')))


def lookup(frame, dimension, spec, source_id, date_column, output_key):
    """Versioned LEFT lookup, stable cardinality after dimension validation."""
    from pyspark.sql import functions as F
    left, right = frame.alias('_job'), dimension.alias('_dim')
    identity = F.col(f'_job.`{source_id}`') == F.col(f'_dim.`{spec.id_column}`')
    day = F.col(f'_job.`{date_column}`').cast('date')
    condition = identity if spec.name == 'location' else identity & (day >= F.col('_dim.valid_from')) & (day < F.col('_dim.valid_to'))
    return left.join(right, condition, 'left').select('_job.*',
        F.coalesce(F.col(f'_dim.`{spec.key_column}`'), F.lit('0')).alias(output_key))


def validated_shared(spark, root, max_reference_rows):
    frames = {}
    for name, spec in shared.SHARED_SPECS.items():
        path = f'{root}/dim_{name}'
        frame = shared.existing_delta(spark, path, shared.dimension_schema(spec))
        if frame is None:
            raise FileNotFoundError(f'Run build_shared_dimensions.py first: {path}')
        shared.validate_dimension(shared.bounded_rows(frame, max_reference_rows), spec)
        frames[name] = frame
    calendar = shared.existing_delta(spark, f'{root}/dim_date', shared.date_schema())
    if calendar is None:
        raise FileNotFoundError('Missing shared dim_date')
    rows = shared.bounded_rows(calendar, 100002)
    unique(calendar, ['date_key'], 'dim_date')
    known = [r for r in rows if r['date_key'] != 0]
    if not known or len([r for r in rows if r['date_key'] == 0 and r['full_date'] == shared.UNKNOWN_START]) != 1:
        raise ValueError('dim_date: invalid calendar or missing unknown member')
    for row in known:
        day = row['full_date']
        if not isinstance(day, date) or row['date_key'] != day.year*10000+day.month*100+day.day:
            raise ValueError('dim_date: key/date mismatch')
    if len({r['full_date'] for r in rows}) != len(rows):
        raise ValueError('dim_date: duplicate full_date')
    frames['date'] = calendar
    return frames


def company_candidates(spark, source, dimensions, old_rows, reference_date, maximum):
    from pyspark.sql import functions as F, types as T
    require_columns(source, ('company_id', 'company_name'), 'companies')
    if not isinstance(source.schema['company_id'].dataType, T.StringType):
        raise ValueError('companies.company_id must be STRING')
    selected = ['company_id', 'company_name', 'industry_id', 'location_id',
                'valid_from', 'valid_to', 'is_current']
    for column in selected:
        if column not in source.columns:
            source = source.withColumn(column, F.lit(None).cast('string'))
    source = source.select(*selected).distinct()
    for column in ('company_id', 'company_name', 'industry_id', 'location_id'):
        source = source.withColumn(column, clean(F.col(column)))
    source = source.withColumn('_effective',
        F.coalesce(try_cast('valid_from', 'date'), F.lit(reference_date).cast('date')))
    for name in ('location',):
        if name not in dimensions:
            continue
        source = lookup(source, dimensions[name], shared.SPECS[name], f'{name}_id', '_effective', f'{name}_key')
    if 'industry_key' not in source.columns:
        source = source.withColumn('industry_key', F.lit('0'))
    records = shared.bounded_rows(source, maximum)
    for row in records:
        name = shared.clean_text(row['company_name'])
        row['normalized_name'] = name.lower() if name else None
    if not records:
        result = old_rows or [shared.unknown_member(COMPANY)]
        shared.validate_dimension(result, COMPANY)
        return result
    return shared.reconcile_dimension(records, old_rows or [], COMPANY, reference_date)


def build_fact(spark, jobs, skills, dimensions, company_dimension, reference_date,
               dimension_date_policy, old_fact=None):
    from pyspark.sql import functions as F
    jobs = prepare_jobs(jobs).withColumn('seen_in_silver', F.lit(True))
    jobs = jobs.withColumn('skill_count_as_of', F.lit(reference_date).cast('date'))
    retained_count = 0
    if old_fact is not None:
        removed = {'industry_id', 'occupation_id', 'job_level', 'employment_type',
                   'working_model', 'salary_status', 'is_remote'}
        require_columns(old_fact, (*(c for c in RAW_COLUMNS if c not in removed),
                                   'skill_count_as_of'), 'existing fact')
        retained = old_fact.join(jobs.select('job_id'), 'job_id', 'left_anti')
        retained_count = retained.count()
        raw = [F.col(c) if c in retained.columns else
               F.lit(None).cast(jobs.schema[c].dataType).alias(c) for c in RAW_COLUMNS]
        retained = retained.select(*raw, F.lit(False).alias('seen_in_silver'),
                    'skill_count_as_of')
        jobs = jobs.unionByName(retained)
    skills = prepare_skills(skills)
    orphan = skills.select('job_id').distinct().join(jobs.select('job_id'), 'job_id', 'left_anti')
    if orphan.limit(1).count():
        raise ValueError('job_skills contains a job_id absent from Silver and retained Gold')
    jobs = jobs.withColumn('reference_date', F.lit(reference_date).cast('date'))
    jobs = activity_columns(jobs, reference_date)
    jobs = jobs.withColumn('has_salary', F.col('salary_min_vnd').isNotNull() | F.col('salary_max_vnd').isNotNull())
    jobs = jobs.withColumn('job_fact_key', job_key(F.col('job_id')))
    if retained_count:
        for column in ('posted_date', 'expiration_date'):
            missing_dates = jobs.where(~F.col('seen_in_silver')).select(F.col(column).alias('full_date')).where(F.col('full_date').isNotNull()).distinct().join(dimensions['date'].select('full_date'), 'full_date', 'left_anti')
            if missing_dates.limit(1).count():
                raise ValueError(f'Retained Gold jobs reference {column} absent from observed dim_date; restore source history or migrate retained facts explicitly')
    jobs = shared.attach_date_key(jobs, dimensions['date'], 'posted_date', 'posted_date_key')
    jobs = shared.attach_date_key(jobs, dimensions['date'], 'expiration_date', 'expiration_date_key')
    jobs = jobs.withColumn('_lookup_date', F.col('posted_date') if dimension_date_policy == 'posted'
                           else F.col('reference_date'))
    for name, identity in [('location', 'primary_location_id')]:
        if name not in dimensions:
            jobs = jobs.withColumn(f'{name}_key', F.lit('0'))
            continue
        jobs = lookup(jobs, dimensions[name], shared.SPECS[name], identity, '_lookup_date', f'{name}_key')
    jobs = lookup(jobs, company_dimension, COMPANY, 'company_id', '_lookup_date', 'company_key')
    jobs = jobs.withColumn('dimension_date_policy', F.lit(dimension_date_policy))
    # UTC load time is changed only when a row's business state changes at merge.
    jobs = jobs.withColumn('loaded_at', F.lit(datetime.now(timezone.utc)).cast('timestamp'))
    columns = ['job_fact_key', 'job_id', 'posted_date_key', 'expiration_date_key', *FK_COLUMNS,
        'source_name', 'job_title', 'education_level',
        # 'job_level', 'employment_type', 'working_model',
        *NUMBER_COLUMNS, 'has_salary', 'reference_date',
        # 'is_remote', 'is_active', 'skill_count',
        'skill_count_as_of', 'dimension_date_policy', 'activity_status', 'seen_in_silver',
        'record_status', 'posted_date', 'expiration_date', 'company_id', 'primary_location_id',
        # 'industry_id', 'occupation_id', 'salary_status',
        'source_url', 'loaded_at']
    return jobs.select(*columns), retained_count


def compatible_fact(spark, path, schema):
    from delta.tables import DeltaTable
    if not shared.hadoop_exists(spark, path):
        return None
    if not DeltaTable.isDeltaTable(spark, path):
        raise ValueError(f'Existing target is not Delta: {path}')
    frame = spark.read.format('delta').load(path)
    if schema is not None and {f.name: f.dataType for f in frame.schema} != {f.name: f.dataType for f in schema}:
        raise ValueError('Existing fact schema is incompatible; explicit migration is required')
    return frame


def validate_fact(fact, dimensions, company_dimension):
    from pyspark.sql import functions as F
    fail_if(fact, clean(F.col('job_id')).isNull() | clean(F.col('source_name')).isNull(),
            'fact_job_posting: NULL/blank identity or source')
    unique(fact, ['job_id'], 'fact_job_posting')
    unique(fact, ['job_fact_key'], 'fact_job_posting')
    fail_if(fact, F.col('job_fact_key') != job_key(F.col('job_id')),
            'fact_job_posting: incompatible key algorithm')
    checks = [('posted_date_key', dimensions['date']), ('expiration_date_key', dimensions['date']),
              ('location_key', dimensions.get('location')), ('industry_key', dimensions.get('industry')),
              ('occupation_key', dimensions.get('occupation')), ('company_key', company_dimension)]
    for key, target in checks:
        if target is None:
            continue
        target_key = 'date_key' if key.endswith('date_key') else key
        missing = fact.select(F.col(key).alias('_fk')).distinct().join(
            target.select(F.col(target_key).alias('_fk')), '_fk', 'left_anti')
        if missing.limit(1).count():
            raise ValueError(f'Orphan or NULL FK: {key}')


def upsert_fact(spark, path, fact, old):
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F
    if old is None:
        fact.write.format('delta').mode('errorifexists').save(path)
        return True
    condition = ' OR '.join(f'NOT (t.`{c}` <=> s.`{c}`)' for c in fact.columns if c != 'loaded_at')
    differences = fact.alias('s').join(old.alias('t'), F.col('s.job_id') == F.col('t.job_id'), 'left')
    if not differences.where(F.col('t.job_id').isNull() | F.expr(condition)).limit(1).count():
        return False
    (DeltaTable.forPath(spark, path).alias('t').merge(fact.alias('s'), 't.job_id = s.job_id')
        .whenMatchedUpdateAll(condition=condition).whenNotMatchedInsertAll().execute())
    return True


def reconcile_skill_fact(fact_jobs, fact_skills, silver_skills, dim_skill):
    from pyspark.sql import functions as F
    require_columns(fact_jobs, ('job_fact_key', 'job_id', 'seen_in_silver'), 'fact_job_posting')
    require_columns(fact_skills, ('job_fact_key', 'skill_key'), 'fact_job_skill')
    unique(fact_jobs, ['job_fact_key'], 'fact_job_posting')
    unique(fact_skills, ['job_fact_key', 'skill_key'], 'fact_job_skill')
    if fact_skills.where(F.col('skill_key') == '0').limit(1).count():
        raise ValueError('Skill has unknown key; retain unresolved evidence in Silver')
    if fact_skills.join(fact_jobs.select('job_fact_key'), 'job_fact_key', 'left_anti').limit(1).count():
        raise ValueError('fact_job_skill has orphan job_fact_key')
    if fact_skills.join(dim_skill.select('skill_key'), 'skill_key', 'left_anti').limit(1).count():
        raise ValueError('fact_job_skill has orphan skill_key')
    context = [c for c in ('posted_date_key', 'location_key', 'industry_key', 'occupation_key')
               if c in fact_skills.columns and c in fact_jobs.columns]
    if context or ('date_key' in fact_skills.columns and 'posted_date_key' in fact_jobs.columns):
        joined = fact_skills.alias('s').join(fact_jobs.alias('j'), 'job_fact_key')
        mismatch = ' OR '.join([*(f'NOT (s.`{c}` <=> j.`{c}`)' for c in context), *(['NOT (s.date_key <=> j.posted_date_key)'] if 'date_key' in fact_skills.columns and 'posted_date_key' in fact_jobs.columns else [])])
        if joined.where(mismatch).limit(1).count():
            raise ValueError('fact_job_skill context keys differ from parent fact')
    # The child can carry raw IDs for audit. Avoid two ambiguous skill_id
    # columns, and verify those IDs rather than trusting duplicated metadata.
    child = (fact_skills.withColumnRenamed('skill_id', '_evidence_skill_id')
             if 'skill_id' in fact_skills.columns else fact_skills)
    linked = child.join(dim_skill.select('skill_key', 'skill_id'), 'skill_key', 'inner')
    if '_evidence_skill_id' in linked.columns:
        fail_if(linked, ~F.col('_evidence_skill_id').eqNullSafe(F.col('skill_id')),
                'fact_job_skill raw skill_id differs from its dimension key')
    if 'job_id' in fact_skills.columns:
        identities = fact_skills.alias('s').join(fact_jobs.alias('j'), 'job_fact_key')
        fail_if(identities, ~F.col('s.job_id').eqNullSafe(F.col('j.job_id')),
                'fact_job_skill raw job_id differs from its parent key')
    unique(linked, ['job_fact_key', 'skill_id'], 'fact_job_skill (multiple versions of same skill)')
    # Exact-set check for jobs represented in this Silver snapshot. Retained
    # absent jobs keep the previously published skill set and its reference date.
    seen = fact_jobs.where(F.col('seen_in_silver')).select('job_id', 'job_fact_key')
    checked_silver = prepare_skills(silver_skills)
    if checked_silver.join(fact_jobs.select('job_id'), 'job_id', 'left_anti').limit(1).count():
        raise ValueError('Silver job_skills has orphan job_id')
    expected = checked_silver.join(seen, 'job_id', 'inner').select('job_fact_key', 'skill_id').distinct()
    observed = linked.join(seen.select('job_fact_key'), 'job_fact_key')
    observed = observed.select('job_fact_key', 'skill_id').distinct()
    if expected.join(observed, ['job_fact_key', 'skill_id'], 'left_anti').limit(1).count() or \
       observed.join(expected, ['job_fact_key', 'skill_id'], 'left_anti').limit(1).count():
        raise ValueError('Skill ID sets differ between Silver and fact_job_skill')
    return {'status': 'passed', 'fact_jobs': fact_jobs.count(), 'job_skill_pairs': fact_skills.count()}


def build_jobs_gold(spark=None, *, silver_root=None, shared_root=None, gold_root=None,
                    reference_date=None, dimension_date_policy='as_of', dry_run=False,
                    max_reference_rows=100000, reconcile_only=False, include_bridges=False, replace_fact_schema=False):
    from pyspark.sql import functions as F
    from pyspark import StorageLevel
    reference_date = shared.parse_date(reference_date, 'reference_date') or date.today()
    if max_reference_rows < 1:
        raise ValueError('max_reference_rows must be positive')
    if dimension_date_policy not in ('as_of', 'posted'):
        raise ValueError('dimension_date_policy must be as_of or posted')
    silver_root = normalize_s3a_root(silver_root or os.getenv('JOBS_SILVER_ROOT', 's3a://silver/jobs'))
    shared_root = normalize_s3a_root(shared_root or os.getenv('SHARED_GOLD_ROOT', 's3a://gold/shared'))
    gold_root = normalize_s3a_root(gold_root or os.getenv('JOBS_GOLD_ROOT', 's3a://gold/jobs'))
    roots = [silver_root, shared_root, gold_root]
    if any(path.startswith('s3://') for path in roots):
        raise ValueError('Spark paths must use s3a://, not s3://')
    owns_spark = spark is None
    spark = spark or shared.make_spark('gold-job-postings', any(path.startswith('s3a://') for path in roots))
    cached = None
    bridge_report = None
    try:
        dimensions = validated_shared(spark, shared_root, max_reference_rows)
        skills = load_silver_table(spark, silver_root, 'job_skills')
        fact_path = f'{gold_root}/fact_job_posting'
        skill_spec = shared.SPECS['skill']
        skill_path = f'{gold_root}/dim_skill'
        if reconcile_only:
            dimensions['skill'] = shared.existing_delta(spark, skill_path, shared.dimension_schema(skill_spec))
            if dimensions['skill'] is None:
                raise FileNotFoundError(skill_path)
            shared.validate_dimension(shared.bounded_rows(dimensions['skill'], max_reference_rows), skill_spec)
            result = reconcile_skill_fact(spark.read.format('delta').load(fact_path),
                spark.read.format('delta').load(f'{gold_root}/fact_job_skill'), skills, dimensions['skill'])
            LOG.info('Skill reconciliation: %s', result)
            return result
        skill_report, skill_rows = build_job_dimensions(spark, gold_root=gold_root, shared_root=shared_root, as_of=reference_date, dry_run=True, max_reference_rows=max_reference_rows, return_rows=True)
        skill_dimension = spark.createDataFrame(skill_rows, shared.dimension_schema(skill_spec))
        jobs = load_silver_table(spark, silver_root, 'job_postings')
        companies = load_silver_table(spark, silver_root, 'companies')
        company_path, company_schema = f'{gold_root}/dim_company', shared.dimension_schema(COMPANY)
        old_company = shared.existing_delta(spark, company_path, company_schema)
        old_company_rows = shared.bounded_rows(old_company, max_reference_rows) if old_company is not None else None
        company_rows = company_candidates(spark, companies, dimensions, old_company_rows,
                                          reference_date, max_reference_rows)
        if len(company_rows) > max_reference_rows:
            raise ValueError('Company history exceeds max_reference_rows')
        company_dimension = spark.createDataFrame(company_rows, company_schema)
        old_fact = compatible_fact(spark, fact_path, None)
        if old_fact is not None:
            require_columns(old_fact, ('job_id', 'job_fact_key', 'reference_date'), 'existing fact (migration required)')
            unique(old_fact, ['job_id'], 'existing fact')
            unique(old_fact, ['job_fact_key'], 'existing fact')
            fail_if(old_fact, F.col('job_fact_key').isNull() | (F.col('job_fact_key') != job_key(F.col('job_id'))),
                    'Existing fact has an incompatible job_fact_key')
            fail_if(old_fact, F.col('reference_date').isNull() | (F.col('reference_date') > F.lit(reference_date).cast('date')),
                    'Cannot rerun a current-state fact at an earlier reference_date')
        candidate, retained_count = build_fact(spark, jobs, skills, dimensions, company_dimension,
                                               reference_date, dimension_date_policy, old_fact)
        old_types = {f.name: f.dataType for f in old_fact.schema} if old_fact is not None else {}
        new_types = {f.name: f.dataType for f in candidate.schema}
        migrate = old_fact is not None and old_types != new_types
        if migrate:
            removed = set(old_types) - set(new_types)
            allowed = {'industry_key', 'occupation_key', 'industry_id', 'occupation_id',
                       'job_level', 'employment_type', 'working_model', 'is_remote',
                       'is_active', 'skill_count', 'salary_status'}
            if not removed <= allowed or any(old_types.get(k) != v for k, v in new_types.items()):
                raise ValueError('Unexpected fact schema change; manual migration required')
            if not (dry_run or replace_fact_schema):
                raise ValueError('Run --dry-run then --replace-fact-schema for the planned column removal')
        cached = candidate.persist(StorageLevel.MEMORY_AND_DISK)
        count = cached.count()
        if old_fact is not None:
            identity_check = cached.alias('s').join(old_fact.alias('t'), 'job_id', 'inner')
            fail_if(identity_check, ~F.col('s.source_name').eqNullSafe(F.col('t.source_name')),
                    'job_id reused across source_name values; fix the global Silver ID')
        validate_fact(cached, dimensions, company_dimension)
        # Preflight skill coverage before writing any Gold table.
        evidence = prepare_skills(skills).join(cached.select('job_id', 'posted_date', 'reference_date', 'seen_in_silver'), 'job_id', 'inner').where(F.col('seen_in_silver'))
        evidence = evidence.withColumn('_skill_date', F.col('posted_date' if dimension_date_policy == 'posted' else 'reference_date'))
        coverage = lookup(evidence, skill_dimension, skill_spec, 'skill_id', '_skill_date', 'skill_key')
        fail_if(coverage, F.col('skill_key') == '0', 'Unresolved skill ID/effective date; fix skill catalog before Gold writes')
        metrics = cached.agg(*[
            F.sum(F.when(F.col(key) == '0', 1).otherwise(0)).alias(f'unknown_{key}') for key in FK_COLUMNS
        ]).first().asDict()
        company_written = fact_written = False
        if not dry_run:
            skill_report = build_job_dimensions(spark, gold_root=gold_root, shared_root=shared_root, as_of=reference_date, max_reference_rows=max_reference_rows)
            company_written = shared.merge_rows(spark, company_path, company_rows, company_schema,
                                                  'company_key', old_company_rows)
            if migrate:
                cached.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(fact_path)
                fact_written = True
            else:
                fact_written = upsert_fact(spark, fact_path, cached, old_fact)
        if include_bridges and not dry_run:
            try:
                from . import build_job_bridges as bridges
            except ImportError:
                import build_job_bridges as bridges
            bridge_report = bridges.build_job_bridges(
                spark=spark,
                silver_root=silver_root,
                shared_root=shared_root,
                gold_root=gold_root,
                reference_date=reference_date.isoformat(),
                max_reference_rows=max_reference_rows,
                dry_run=dry_run,
            )
        result = {'fact_rows': count, 'retained_jobs': retained_count, 'company_versions': len(company_rows),
            'reference_date': reference_date.isoformat(), 'dimension_date_policy': dimension_date_policy,
            'company_written': company_written, 'fact_written': fact_written, 'dry_run': dry_run,
            'skill_fact_reconciliation': 'pending: run after build_fact_job_skill.py',
            'fact_path': fact_path, 'company_path': company_path, 'schema_migration': migrate,
            'bridge_report': bridge_report, 'skill_dimension': skill_report,
            **{key: value or 0 for key, value in metrics.items()}}
        LOG.info('Gold recruitment: %s', json.dumps(result, ensure_ascii=False))
        return result
    finally:
        if cached is not None:
            cached.unpersist()
        if owns_spark:
            spark.stop()


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--silver-root', default=os.getenv('JOBS_SILVER_ROOT', 's3a://silver/jobs'))
    parser.add_argument('--shared-root', default=os.getenv('SHARED_GOLD_ROOT', 's3a://gold/shared'))
    parser.add_argument('--gold-root', default=os.getenv('JOBS_GOLD_ROOT', 's3a://gold/jobs'))
    parser.add_argument('--reference-date', default=date.today().isoformat())
    parser.add_argument('--dimension-date-policy', choices=['as_of', 'posted'], default='as_of')
    parser.add_argument('--max-reference-rows', type=int, default=100000)
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--replace-fact-schema', action='store_true', help='Approve planned fact column removal')
    parser.add_argument('--reconcile-only', action='store_true')
    parser.add_argument('--include-bridges', action='store_true', help='Optionally run bridge after parent commit')
    parser.add_argument('--no-bridges', action='store_true', help='Skip bridge_job_location generation')
    args = parser.parse_args()
    if args.dry_run and args.reconcile_only:
        parser.error('--dry-run and --reconcile-only are mutually exclusive')
    args.include_bridges = args.include_bridges and not args.no_bridges
    delattr(args, 'no_bridges')
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    print(json.dumps(build_jobs_gold(**vars(args)), ensure_ascii=False, indent=2))

if __name__ == '__main__':
    main()
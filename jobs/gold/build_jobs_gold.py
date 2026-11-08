
from __future__ import annotations

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
FK_COLUMNS = ('location_key', 'industry_key', 'occupation_key', 'company_key')


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
    fail_if(frame, F.col('expiration_date') < F.col('posted_date'),
            'expiration_date precedes posted_date')
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
    condition = identity & (day >= F.col('_dim.valid_from')) & (day < F.col('_dim.valid_to'))
    return left.join(right, condition, 'left').select('_job.*',
        F.coalesce(F.col(f'_dim.`{spec.key_column}`'), F.lit('0')).alias(output_key))


def validated_shared(spark, root, max_reference_rows):
    frames = {}
    for name, spec in shared.SPECS.items():
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
    if not known or len([r for r in rows if r['date_key'] == 0 and r['full_date'] is None]) != 1:
        raise ValueError('dim_date: invalid calendar or missing unknown member')
    for row in known:
        day = row['full_date']
        if not isinstance(day, date) or row['date_key'] != day.year*10000+day.month*100+day.day:
            raise ValueError('dim_date: key/date mismatch')
    if len({r['full_date'] for r in known}) != len(known):
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
    for name in ('industry', 'location'):
        source = lookup(source, dimensions[name], shared.SPECS[name], f'{name}_id', '_effective', f'{name}_key')
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
    jobs = jobs.withColumn('_prior_skill_count', F.lit(None).cast('int'))
    jobs = jobs.withColumn('skill_count_as_of', F.lit(reference_date).cast('date'))
    retained_count = 0
    if old_fact is not None:
        require_columns(old_fact, (*RAW_COLUMNS, 'skill_count', 'skill_count_as_of'), 'existing fact')
        retained = old_fact.join(jobs.select('job_id'), 'job_id', 'left_anti')
        retained_count = retained.count()
        retained = retained.select(*RAW_COLUMNS,
            F.lit(False).alias('seen_in_silver'), F.col('skill_count').alias('_prior_skill_count'),
            'skill_count_as_of')
        jobs = jobs.unionByName(retained)
    skills = prepare_skills(skills)
    orphan = skills.select('job_id').distinct().join(jobs.select('job_id'), 'job_id', 'left_anti')
    if orphan.limit(1).count():
        raise ValueError('job_skills contains a job_id absent from Silver and retained Gold')
    counts = (skills.where(F.col('is_required') == True).groupBy('job_id')
              .agg(F.countDistinct('skill_id').cast('int').alias('_required_count')))
    jobs = jobs.join(counts, 'job_id', 'left').withColumn('skill_count',
        F.when(F.col('seen_in_silver'), F.coalesce(F.col('_required_count'), F.lit(0)))
         .otherwise(F.col('_prior_skill_count')).cast('int'))
    jobs = jobs.withColumn('reference_date', F.lit(reference_date).cast('date'))
    jobs = activity_columns(jobs, reference_date)
    model = token(F.col('working_model'))
    inferred = (F.when(model.isin(*REMOTE_MODELS), True).when(model.isin(*ONSITE_MODELS), False)
                .otherwise(F.lit(None).cast('boolean')))
    jobs = jobs.withColumn('_model_remote', inferred)
    fail_if(jobs, F.col('is_remote').isNotNull() & F.col('_model_remote').isNotNull()
            & (F.col('is_remote') != F.col('_model_remote')), 'is_remote contradicts working_model')
    jobs = jobs.withColumn('is_remote', F.coalesce('is_remote', '_model_remote'))
    jobs = jobs.withColumn('has_salary', F.col('salary_min_vnd').isNotNull() | F.col('salary_max_vnd').isNotNull())
    jobs = jobs.withColumn('job_fact_key', job_key(F.col('job_id')))
    jobs = shared.attach_date_key(jobs, dimensions['date'], 'posted_date', 'posted_date_key')
    jobs = shared.attach_date_key(jobs, dimensions['date'], 'expiration_date', 'expiration_date_key')
    jobs = jobs.withColumn('_lookup_date', F.col('posted_date') if dimension_date_policy == 'posted'
                           else F.col('reference_date'))
    for name, identity in [('location', 'primary_location_id'), ('industry', 'industry_id'),
                           ('occupation', 'occupation_id')]:
        jobs = lookup(jobs, dimensions[name], shared.SPECS[name], identity, '_lookup_date', f'{name}_key')
    jobs = lookup(jobs, company_dimension, COMPANY, 'company_id', '_lookup_date', 'company_key')
    jobs = jobs.withColumn('dimension_date_policy', F.lit(dimension_date_policy))
    # UTC load time is changed only when a row's business state changes at merge.
    jobs = jobs.withColumn('loaded_at', F.lit(datetime.now(timezone.utc)).cast('timestamp'))
    columns = ['job_fact_key', 'job_id', 'posted_date_key', 'expiration_date_key', *FK_COLUMNS,
        'source_name', 'job_title', 'job_level', 'education_level', 'employment_type', 'working_model',
        *NUMBER_COLUMNS, 'has_salary', 'is_remote', 'is_active', 'skill_count', 'reference_date',
        'skill_count_as_of', 'dimension_date_policy', 'activity_status', 'seen_in_silver',
        'record_status', 'posted_date', 'expiration_date', 'company_id', 'primary_location_id',
        'industry_id', 'occupation_id', 'salary_status', 'source_url', 'loaded_at']
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
    fail_if(fact, F.col('skill_count').isNull() | (F.col('skill_count') < 0), 'Invalid skill_count')
    for key, target in [('posted_date_key', dimensions['date']), ('expiration_date_key', dimensions['date']),
                        ('location_key', dimensions['location']), ('industry_key', dimensions['industry']),
                        ('occupation_key', dimensions['occupation']), ('company_key', company_dimension)]:
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
    """Read-only: count + exact required skill IDs, no write to either fact."""
    from pyspark.sql import functions as F, types as T
    require_columns(fact_jobs, ('job_fact_key', 'job_id', 'skill_count', 'seen_in_silver'), 'fact_job_posting')
    require_columns(fact_skills, ('job_fact_key', 'skill_key', 'is_required'), 'fact_job_skill')
    if not isinstance(fact_skills.schema['is_required'].dataType, T.BooleanType):
        raise ValueError('fact_job_skill.is_required must be BOOLEAN')
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
    if context:
        joined = fact_skills.alias('s').join(fact_jobs.alias('j'), 'job_fact_key')
        mismatch = ' OR '.join(f'NOT (s.`{c}` <=> j.`{c}`)' for c in context)
        if joined.where(mismatch).limit(1).count():
            raise ValueError('fact_job_skill context keys differ from parent fact')
    # The child now carries skill_id too; keep one unambiguous identity column.
    child = fact_skills.drop('skill_id') if 'skill_id' in fact_skills.columns else fact_skills
    linked = child.join(dim_skill.select('skill_key', 'skill_id'), 'skill_key', 'inner')
    unique(linked, ['job_fact_key', 'skill_id'], 'fact_job_skill (multiple versions of same skill)')
    actual = linked.where(F.col('is_required') == True).groupBy('job_fact_key').agg(
        F.countDistinct('skill_id').cast('int').alias('_actual'))
    comparison = fact_jobs.join(actual, 'job_fact_key', 'left')
    fail_if(comparison, F.col('skill_count').isNull() | (F.col('skill_count') != F.coalesce(F.col('_actual'), F.lit(0))),
            'skill_count does not match required skills in fact_job_skill')
    # Exact-set check for jobs represented in this Silver snapshot. Retained
    # absent jobs keep the previously published skill set and its reference date.
    seen = fact_jobs.where(F.col('seen_in_silver')).select('job_id', 'job_fact_key')
    checked_silver = prepare_skills(silver_skills)
    if checked_silver.join(fact_jobs.select('job_id'), 'job_id', 'left_anti').limit(1).count():
        raise ValueError('Silver job_skills has orphan job_id')
    expected = checked_silver.where(F.col('is_required') == True)
    expected = expected.join(seen, 'job_id', 'inner').select('job_fact_key', 'skill_id').distinct()
    observed = linked.where(F.col('is_required') == True).join(seen.select('job_fact_key'), 'job_fact_key')
    observed = observed.select('job_fact_key', 'skill_id').distinct()
    if expected.join(observed, ['job_fact_key', 'skill_id'], 'left_anti').limit(1).count() or \
       observed.join(expected, ['job_fact_key', 'skill_id'], 'left_anti').limit(1).count():
        raise ValueError('Required skill ID sets differ between Silver and fact_job_skill')
    return {'status': 'passed', 'fact_jobs': fact_jobs.count(), 'job_skill_pairs': fact_skills.count()}


def build_jobs_gold(spark=None, *, silver_root=None, shared_root=None, gold_root=None,
                    reference_date=None, dimension_date_policy='as_of', dry_run=False,
                    max_reference_rows=100000, reconcile_only=False):
    from pyspark.sql import functions as F
    from pyspark import StorageLevel
    reference_date = shared.parse_date(reference_date, 'reference_date') or date.today()
    if max_reference_rows < 1:
        raise ValueError('max_reference_rows must be positive')
    if dimension_date_policy not in ('as_of', 'posted'):
        raise ValueError('dimension_date_policy must be as_of or posted')
    silver_root = (silver_root or os.getenv('JOBS_SILVER_ROOT', 's3a://silver/jobs')).rstrip('/')
    shared_root = (shared_root or os.getenv('SHARED_GOLD_ROOT', 's3a://gold/shared')).rstrip('/')
    gold_root = (gold_root or os.getenv('JOBS_GOLD_ROOT', 's3a://gold/jobs')).rstrip('/')
    roots = [silver_root, shared_root, gold_root]
    if any(path.startswith('s3://') for path in roots):
        raise ValueError('Spark paths must use s3a://, not s3://')
    owns_spark = spark is None
    spark = spark or shared.make_spark('gold-job-postings', any(path.startswith('s3a://') for path in roots))
    cached = None
    try:
        dimensions = validated_shared(spark, shared_root, max_reference_rows)
        skills = spark.read.format('delta').load(f'{silver_root}/job_skills')
        fact_path = f'{gold_root}/fact_job_posting'
        if reconcile_only:
            result = reconcile_skill_fact(spark.read.format('delta').load(fact_path),
                spark.read.format('delta').load(f'{gold_root}/fact_job_skill'), skills, dimensions['skill'])
            LOG.info('Skill reconciliation: %s', result)
            return result
        jobs = spark.read.format('delta').load(f'{silver_root}/job_postings')
        companies = spark.read.format('delta').load(f'{silver_root}/companies')
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
        compatible_fact(spark, fact_path, candidate.schema)
        cached = candidate.persist(StorageLevel.MEMORY_AND_DISK)
        count = cached.count()
        if old_fact is not None:
            identity_check = cached.alias('s').join(old_fact.alias('t'), 'job_id', 'inner')
            fail_if(identity_check, ~F.col('s.source_name').eqNullSafe(F.col('t.source_name')),
                    'job_id reused across source_name values; fix the global Silver ID')
        validate_fact(cached, dimensions, company_dimension)
        metrics = cached.agg(*[
            F.sum(F.when(F.col(key) == '0', 1).otherwise(0)).alias(f'unknown_{key}') for key in FK_COLUMNS
        ], F.sum(F.when(F.col('is_active').isNull(), 1).otherwise(0)).alias('unknown_activity')).first().asDict()
        company_written = fact_written = False
        if not dry_run:
            company_written = shared.merge_rows(spark, company_path, company_rows, company_schema,
                                                  'company_key', old_company_rows)
            fact_written = upsert_fact(spark, fact_path, cached, old_fact)
        result = {'fact_rows': count, 'retained_jobs': retained_count, 'company_versions': len(company_rows),
            'reference_date': reference_date.isoformat(), 'dimension_date_policy': dimension_date_policy,
            'company_written': company_written, 'fact_written': fact_written, 'dry_run': dry_run,
            'skill_fact_reconciliation': 'pending: run after build_fact_job_skill.py',
            'fact_path': fact_path, 'company_path': company_path,
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
    parser.add_argument('--reconcile-only', action='store_true')
    args = parser.parse_args()
    if args.dry_run and args.reconcile_only:
        parser.error('--dry-run and --reconcile-only are mutually exclusive')
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    print(json.dumps(build_jobs_gold(**vars(args)), ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()

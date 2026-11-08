
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import json
import logging
import os
from pathlib import Path
import sys

JOBS_ROOT = Path(__file__).resolve().parents[1]
if str(JOBS_ROOT) not in sys.path:
    sys.path.insert(0, str(JOBS_ROOT))

try:
    from . import build_jobs_gold as jobs
    from . import build_shared_dimensions as shared
except ImportError:
    import build_jobs_gold as jobs
    import build_shared_dimensions as shared

LOG = logging.getLogger(__name__)
PAIR = ['job_fact_key', 'skill_key']
EVIDENCE = ('job_id', 'skill_id', 'is_required', 'required_level', 'years_required', 'confidence_score')
CONTEXT = {'date_key': 'posted_date_key', 'location_key': 'location_key',
           'occupation_key': 'occupation_key', 'industry_key': 'industry_key'}


def fact_schema():
    from pyspark.sql import types as T
    return T.StructType([
        T.StructField('job_fact_key', T.StringType(), True),
        T.StructField('skill_key', T.StringType(), True),
        T.StructField('date_key', T.IntegerType(), True),
        T.StructField('location_key', T.StringType(), True),
        T.StructField('occupation_key', T.StringType(), True),
        T.StructField('industry_key', T.StringType(), True),
        T.StructField('is_required', T.BooleanType(), True),
        T.StructField('required_level', T.StringType(), True),
        T.StructField('years_required', T.DecimalType(8, 2), True),
        T.StructField('confidence_score', T.DoubleType(), True),
        T.StructField('job_id', T.StringType(), True),
        T.StructField('skill_id', T.StringType(), True),
        T.StructField('reference_date', T.DateType(), True),
        T.StructField('skill_source_as_of', T.DateType(), True),
        T.StructField('loaded_at', T.TimestampType(), True),
    ])


def check_schema(frame, expected, label):
    actual = {f.name: f.dataType for f in frame.schema}
    wanted = {f.name: f.dataType for f in expected}
    if actual != wanted:
        raise ValueError(f'{label}: incompatible schema; explicit migration is required')


def read_snapshot(spark, path, expected_schema=None, optional=False):
    """Pin one Delta version. Missing, non-Delta and access errors stay distinct."""
    from delta.tables import DeltaTable
    if not shared.hadoop_exists(spark, path):
        if optional:
            return None, None
        raise FileNotFoundError(f'Missing Delta input: {path}')
    if not DeltaTable.isDeltaTable(spark, path):
        raise ValueError(f'Path exists but is not Delta: {path}')
    version = int(DeltaTable.forPath(spark, path).history(1).first()['version'])
    frame = spark.read.format('delta').option('versionAsOf', version).load(path)
    if expected_schema is not None:
        check_schema(frame, expected_schema, path)
    return frame, version


def assert_version(spark, path, version):
    from delta.tables import DeltaTable
    if version is None:
        if shared.hadoop_exists(spark, path):
            raise ValueError(f'Target appeared during preflight: {path}')
        return
    current = int(DeltaTable.forPath(spark, path).history(1).first()['version'])
    if current != version:
        raise ValueError(f'Delta snapshot advanced during the job: {path}; retry the pipeline batch')


def prepare_evidence(source):
    """Dedup only identical canonical evidence; contradictory pairs are errors."""
    from pyspark.sql import functions as F, types as T
    jobs.require_columns(source, ('job_id', 'skill_id', 'is_required'), 'Silver job_skills')
    for column in ('job_id', 'skill_id'):
        if not isinstance(source.schema[column].dataType, T.StringType):
            raise ValueError(f'Silver job_skills.{column} must be STRING')
    for column in ('required_level', 'years_required', 'confidence_score'):
        if column not in source.columns:
            source = source.withColumn(column, F.lit(None).cast('string'))
    source = source.select(*EVIDENCE)
    for column in ('job_id', 'skill_id', 'required_level'):
        source = source.withColumn(column, jobs.clean(F.col(column)))
    jobs.fail_if(source, F.col('job_id').isNull() | F.col('skill_id').isNull(),
                 'Silver job_skills: NULL/blank job_id or skill_id')
    for column, dtype in [('is_required', 'boolean'), ('years_required', 'decimal(8,2)'),
                           ('confidence_score', 'double')]:
        source = source.withColumn('_parsed', jobs.try_cast(column, dtype))
        jobs.fail_if(source, jobs.clean(F.col(column)).isNotNull() & F.col('_parsed').isNull(),
                     f'Silver job_skills: malformed {column}')
        source = source.drop(column).withColumnRenamed('_parsed', column)
    jobs.fail_if(source, F.col('years_required') < 0, 'years_required must be nonnegative or NULL')
    jobs.fail_if(source, F.isnan('confidence_score') | ~F.col('confidence_score').between(0, 1),
                 'confidence_score must be in [0,1] or NULL')
    source = source.select(*EVIDENCE).distinct()
    jobs.unique(source, ['job_id', 'skill_id'], 'Silver job_skills evidence')
    return source


def validate_parent(parent, dimensions, reference_date=None):
    from pyspark.sql import functions as F, types as T
    required = ('job_fact_key', 'job_id', 'posted_date_key', 'location_key', 'occupation_key',
        'industry_key', 'skill_count', 'seen_in_silver', 'reference_date', 'skill_count_as_of',
        'dimension_date_policy', 'posted_date')
    jobs.require_columns(parent, required, 'fact_job_posting: run build_jobs_gold.py first')
    for column, dtype in [('job_id', T.StringType), ('job_fact_key', T.StringType),
        ('posted_date_key', T.IntegerType), ('seen_in_silver', T.BooleanType),
        ('skill_count', T.IntegerType), ('location_key', T.StringType),
        ('occupation_key', T.StringType), ('industry_key', T.StringType),
        ('dimension_date_policy', T.StringType), ('reference_date', T.DateType),
        ('skill_count_as_of', T.DateType), ('posted_date', T.DateType)]:
        if not isinstance(parent.schema[column].dataType, dtype):
            raise ValueError(f'fact_job_posting.{column}: incompatible type')
    jobs.unique(parent, ['job_id'], 'fact_job_posting')
    jobs.unique(parent, ['job_fact_key'], 'fact_job_posting')
    jobs.fail_if(parent, jobs.clean(F.col('job_id')).isNull() | F.col('job_fact_key').isNull()
        | (F.col('job_fact_key') != jobs.job_key(F.col('job_id'))), 'Invalid parent identity/key convention')
    dates = parent.select('reference_date').distinct().limit(2).collect()
    if reference_date is None:
        if len(dates) != 1 or dates[0]['reference_date'] is None:
            raise ValueError('Cannot infer one parent reference_date; pass it explicitly or rerun parent')
        reference_date = dates[0]['reference_date']
    reference_date = shared.parse_date(reference_date, 'reference_date')
    if reference_date is None:
        raise ValueError('reference_date must be explicit for empty parent tables')
    jobs.fail_if(parent, F.col('reference_date').isNull() | (F.col('reference_date') != F.lit(reference_date)),
                 'Parent reference_date differs; run build_jobs_gold for this batch first')
    jobs.fail_if(parent, F.col('seen_in_silver').isNull() | F.col('skill_count').isNull()
        | (F.col('skill_count') < 0) | F.col('skill_count_as_of').isNull()
        | (F.col('skill_count_as_of') > F.lit(reference_date))
        | (F.col('seen_in_silver') & (F.col('skill_count_as_of') != F.lit(reference_date))),
        'Invalid parent snapshot flags or skill_count_as_of')
    jobs.fail_if(parent, F.col('dimension_date_policy').isNull()
        | ~F.col('dimension_date_policy').isin('as_of', 'posted'), 'Invalid parent dimension_date_policy')
    for child_key, parent_key in CONTEXT.items():
        dimension = dimensions['date' if child_key == 'date_key' else child_key.removesuffix('_key')]
        dimension_key = 'date_key' if child_key == 'date_key' else child_key
        missing = parent.select(F.col(parent_key).alias('_fk')).distinct().join(
            dimension.select(F.col(dimension_key).alias('_fk')), '_fk', 'left_anti')
        if missing.limit(1).count():
            raise ValueError(f'Invalid parent context FK: {parent_key}')
    return reference_date


def validate_child(frame, parent, dim_skill, check_context=True):
    from pyspark.sql import functions as F
    check_schema(frame, fact_schema(), 'fact_job_skill')
    jobs.unique(frame, PAIR, 'fact_job_skill')
    jobs.unique(frame, ['job_id', 'skill_id'], 'fact_job_skill business grain')
    jobs.fail_if(frame, F.col('job_id').isNull() | F.col('skill_id').isNull()
        | F.col('skill_key').isNull() | (F.col('skill_key') == '0'), 'Child has missing/unknown skill identity')
    joined = frame.alias('c').join(parent.alias('p'),
        F.col('c.job_fact_key') == F.col('p.job_fact_key'), 'left')
    context = dict(CONTEXT, reference_date='reference_date', skill_source_as_of='skill_count_as_of')
    mismatch = (' OR '.join(f'NOT (c.`{c}` <=> p.`{p}`)' for c, p in context.items())
                if check_context else 'false')
    jobs.fail_if(joined, F.col('p.job_fact_key').isNull()
        | ~F.col('c.job_id').eqNullSafe(F.col('p.job_id')) | F.expr(mismatch),
        'Child job_id or context keys differ from parent')
    linked = frame.alias('c').join(dim_skill.alias('s'),
        F.col('c.skill_key') == F.col('s.skill_key'), 'left')
    jobs.fail_if(linked, F.col('s.skill_key').isNull()
        | ~F.col('c.skill_id').eqNullSafe(F.col('s.skill_id')), 'Child skill ID/key mapping is invalid')
    jobs.fail_if(frame, (F.col('years_required') < 0) | F.isnan('confidence_score')
        | ~F.col('confidence_score').between(0, 1), 'Invalid child evidence values')


def desired_snapshot(spark, evidence, parent, dim_skill, old_child):
    from pyspark.sql import functions as F
    seen = parent.where(F.col('seen_in_silver')).select('job_id')
    if evidence.select('job_id').distinct().join(parent.select('job_id'), 'job_id', 'left_anti').limit(1).count():
        raise ValueError('Silver job_skills has job_id missing from fact_job_posting')
    fresh = evidence.join(seen, 'job_id', 'inner').select(*EVIDENCE)
    retained = parent.where(~F.col('seen_in_silver')).select('job_id', 'job_fact_key')
    if old_child is None:
        if retained.limit(1).count():
            raise ValueError('Retained parents exist but no prior fact_job_skill; cannot reconstruct old evidence')
        base = fresh
    else:
        # Orphans in a prior child table require repair, not an implicit delete.
        validate_child(old_child, parent, dim_skill, check_context=False)
        prior = old_child.join(retained.select('job_fact_key'), 'job_fact_key', 'inner').select(*EVIDENCE)
        base = fresh.unionByName(prior)
    # Join every row to the parent once; primary location and date are not re-inferred.
    contextual = base.alias('e').join(parent.alias('p'), F.col('e.job_id') == F.col('p.job_id'), 'inner')
    contextual = contextual.select(*[F.col(f'e.{c}').alias(c) for c in EVIDENCE],
        F.col('p.job_fact_key'), F.col('p.posted_date_key').alias('date_key'),
        F.col('p.location_key'), F.col('p.occupation_key'), F.col('p.industry_key'),
        F.col('p.reference_date'), F.col('p.skill_count_as_of').alias('skill_source_as_of'),
        F.when(F.col('p.dimension_date_policy') == 'posted', F.col('p.posted_date'))
         .otherwise(F.col('p.reference_date')).alias('_lookup_date'))
    keyed = jobs.lookup(contextual, dim_skill, shared.SPECS['skill'], 'skill_id', '_lookup_date', 'skill_key')
    jobs.fail_if(keyed, F.col('skill_key') == '0',
        'Unresolved skill ID/effective date: keep evidence in Silver and fix mapping; do not publish unknown skill pairs')
    keyed = keyed.withColumn('loaded_at', F.lit(datetime.now(timezone.utc)).cast('timestamp'))
    return keyed.select(*[f.name for f in fact_schema()])


def change_counts(desired, old):
    from pyspark.sql import functions as F
    if old is None:
        return {'inserted': desired.count(), 'updated': 0, 'deleted': 0}
    inserted = desired.select(*PAIR).join(old.select(*PAIR), PAIR, 'left_anti').count()
    deleted = old.select(*PAIR).join(desired.select(*PAIR), PAIR, 'left_anti').count()
    joined = desired.alias('s').join(old.alias('t'), PAIR, 'inner')
    condition = ' OR '.join(f'NOT (s.`{c}` <=> t.`{c}`)' for c in desired.columns if c not in (*PAIR, 'loaded_at'))
    return {'inserted': inserted, 'deleted': deleted, 'updated': joined.where(condition).count()}


def synchronize(spark, path, desired, old, changes):
    from delta.tables import DeltaTable
    if old is None:
        # Create an empty typed Delta table too, so a no-skills snapshot is explicit.
        desired.write.format('delta').mode('errorifexists').save(path)
        return True
    if not any(changes.values()):
        return False
    condition = ' OR '.join(f'NOT (s.`{c}` <=> t.`{c}`)' for c in desired.columns if c not in (*PAIR, 'loaded_at'))
    (DeltaTable.forPath(spark, path).alias('t')
        .merge(desired.alias('s'), 't.job_fact_key = s.job_fact_key AND t.skill_key = s.skill_key')
        .whenMatchedUpdateAll(condition=condition).whenNotMatchedInsertAll()
        .whenNotMatchedBySourceDelete().execute())
    return True


def build_fact_job_skill(spark=None, *, silver_root=None, shared_root=None, gold_root=None,
                        reference_date=None, max_reference_rows=100000, dry_run=False,
                        dq_max_rows=100000):
    from dq.check_job_skill_gold import check_frames, require_pass
    from pyspark import StorageLevel
    from pyspark.sql import functions as F
    silver_root = (silver_root or os.getenv('JOBS_SILVER_ROOT', 's3a://silver/jobs')).rstrip('/')
    shared_root = (shared_root or os.getenv('SHARED_GOLD_ROOT', 's3a://gold/shared')).rstrip('/')
    gold_root = (gold_root or os.getenv('JOBS_GOLD_ROOT', 's3a://gold/jobs')).rstrip('/')
    roots = [silver_root, shared_root, gold_root]
    if any(p.startswith('s3://') for p in roots) or max_reference_rows < 1:
        raise ValueError('Use s3a:// for Spark and a positive max_reference_rows')
    owns_spark = spark is None
    spark = spark or shared.make_spark('gold-fact-job-skill', any(p.startswith('s3a://') for p in roots))
    cached = []
    pinned = {}
    try:
        def load(path, schema=None, optional=False):
            frame, version = read_snapshot(spark, path, schema, optional)
            pinned[path] = version
            return frame
        dimensions = {}
        for name, spec in shared.SPECS.items():
            frame = load(f'{shared_root}/dim_{name}', shared.dimension_schema(spec))
            shared.validate_dimension(shared.bounded_rows(frame, max_reference_rows), spec)
            dimensions[name] = frame
        dimensions['date'] = load(f'{shared_root}/dim_date', shared.date_schema())
        jobs.unique(dimensions['date'], ['date_key'], 'dim_date')
        if dimensions['date'].where('date_key = 0 AND full_date IS NULL').count() != 1:
            raise ValueError('dim_date must have one unknown date member')
        parent = load(f'{gold_root}/fact_job_posting')
        reference_date = validate_parent(parent, dimensions, reference_date)
        raw = load(f'{silver_root}/job_skills')
        evidence = prepare_evidence(raw).persist(StorageLevel.MEMORY_AND_DISK)
        cached.append(evidence)
        evidence.count()
        path = f'{gold_root}/fact_job_skill'
        old = load(path, fact_schema(), optional=True)
        if old is not None:
            old = old.persist(StorageLevel.MEMORY_AND_DISK)
            cached.append(old)
            old.count()
        desired = desired_snapshot(spark, evidence, parent, dimensions['skill'], old)
        desired = desired.persist(StorageLevel.MEMORY_AND_DISK)
        cached.append(desired)
        rows = desired.count()
        validate_child(desired, parent, dimensions['skill'])
        dq_report = check_frames(desired, parent, evidence, dimensions, previous=old, max_rows=dq_max_rows)
        require_pass(dq_report)
        changes = change_counts(desired, old)
        written = False
        if not dry_run:
            # Detect known upstream/target changes before committing. Orchestration
            # still must serialize writers: this is not a distributed lock.
            for input_path, version in pinned.items():
                assert_version(spark, input_path, version)
            written = synchronize(spark, path, desired, old, changes)
        output_version = pinned[path]
        if not dry_run:
            stored, output_version = read_snapshot(spark, path, fact_schema())
            dq_report = check_frames(stored, parent, evidence, dimensions, previous=old, max_rows=dq_max_rows)
            require_pass(dq_report)
            for input_path, version in pinned.items():
                if input_path != path:
                    assert_version(spark, input_path, version)
            assert_version(spark, path, output_version)
        retained_jobs = parent.where(~F.col('seen_in_silver')).count()
        report = {'table': 'gold_jobs.fact_job_skill', 'path': path, 'rows': rows,
            'reference_date': reference_date.isoformat(), 'retained_jobs': retained_jobs,
            'changes': changes, 'written': written, 'dry_run': dry_run,
            'reconciliation': dq_report['status'], 'dq': dq_report,
            'publish_allowed': not dry_run and dq_report['publish_allowed'], 'output_version': output_version,
            'input_versions': {p: v for p, v in pinned.items() if p != path}}
        LOG.info('Job skill Gold: %s', json.dumps(report, ensure_ascii=False))
        return report
    finally:
        for frame in reversed(cached):
            frame.unpersist()
        if owns_spark:
            spark.stop()


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--silver-root', default=os.getenv('JOBS_SILVER_ROOT', 's3a://silver/jobs'))
    parser.add_argument('--shared-root', default=os.getenv('SHARED_GOLD_ROOT', 's3a://gold/shared'))
    parser.add_argument('--gold-root', default=os.getenv('JOBS_GOLD_ROOT', 's3a://gold/jobs'))
    parser.add_argument('--reference-date', help='Must equal parent snapshot date; inferred when omitted')
    parser.add_argument('--max-reference-rows', type=int, default=100000)
    parser.add_argument('--dq-max-rows', type=int, default=100000,
                        help='Maximum rows per DQ input; exceeding this limit blocks the job')
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    print(json.dumps(build_fact_job_skill(**vars(args)), ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()

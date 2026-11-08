"""Synchronize ONLY bridge_job_location for jobs with at least two locations.

Requires sibling build_shared_dimensions.py and build_jobs_gold.py. Read full
Delta snapshots of Silver job_locations(job_id STRING, location_id STRING),
Gold fact_job_posting and shared dim_location. No bridge_job_skill is created;
fact_job_skill keeps the primary location inherited from its parent posting.

Grain/merge key: (job_fact_key, location_id). Preserve raw IDs with unknown FK
'0' when a location has no applicable SCD version. Do not collapse two unmapped
places to one unknown row. Location versions follow the parent's as_of/posted
policy. A missing posted_date under posted policy maps to unknown, not current.

For seen_in_silver jobs Silver is the complete current location set INCLUDING
the known primary place, even for single-location jobs. For retained parents,
keep prior bridge evidence and refresh dimension keys. A job changing from
multiple locations to one/zero loses its bridge rows. No fabricated weights.

Validate/cache the whole desired table before one Delta MERGE with deletions.
Use a single writer and publish only after all Gold tasks succeed. Version
checks detect changes before commit but do not replace an orchestration lock.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import logging
import os

try:
    from . import build_jobs_gold as jobs
    from . import build_shared_dimensions as shared
except ImportError:
    import build_jobs_gold as jobs
    import build_shared_dimensions as shared

LOG = logging.getLogger(__name__)
PAIR = ['job_fact_key', 'location_id']


def bridge_schema():
    from pyspark.sql import types as T
    return T.StructType([T.StructField(name, dtype, True) for name, dtype in [
        ('job_fact_key', T.StringType()), ('location_key', T.StringType()),
        ('job_id', T.StringType()), ('location_id', T.StringType()),
        ('is_primary', T.BooleanType()), ('reference_date', T.DateType()),
        ('location_source_as_of', T.DateType()), ('loaded_at', T.TimestampType())]])


def read_snapshot(spark, path, schema=None, optional=False):
    from delta.tables import DeltaTable
    if not shared.hadoop_exists(spark, path):
        if optional:
            return None, None
        raise FileNotFoundError(f'Missing Delta input: {path}')
    if not DeltaTable.isDeltaTable(spark, path):
        raise ValueError(f'Path exists but is not Delta: {path}')
    version = int(DeltaTable.forPath(spark, path).history(1).first()['version'])
    frame = spark.read.format('delta').option('versionAsOf', version).load(path)
    if schema is not None and {f.name: f.dataType for f in frame.schema} != {f.name: f.dataType for f in schema}:
        raise ValueError(f'{path}: incompatible schema; explicit migration is required')
    return frame, version


def assert_version(spark, path, version):
    from delta.tables import DeltaTable
    if version is None:
        if shared.hadoop_exists(spark, path):
            raise ValueError(f'Target appeared during preflight: {path}')
    elif int(DeltaTable.forPath(spark, path).history(1).first()['version']) != version:
        raise ValueError(f'Delta snapshot advanced during preflight: {path}; retry batch')


def prepare_locations(source):
    from pyspark.sql import functions as F, types as T
    jobs.require_columns(source, ('job_id', 'location_id'), 'Silver job_locations')
    for name in ('job_id', 'location_id'):
        if not isinstance(source.schema[name].dataType, T.StringType):
            raise ValueError(f'Silver job_locations.{name} must be STRING')
    source = source.select(*[jobs.clean(F.col(c)).alias(c) for c in ('job_id', 'location_id')])
    jobs.fail_if(source, F.col('job_id').isNull() | F.col('location_id').isNull()
        | (F.col('location_id') == shared.UNKNOWN_ID), 'job_locations: blank/NULL/reserved business ID')
    return source.distinct()


def lookup_date():
    from pyspark.sql import functions as F
    return F.when(F.col('dimension_date_policy') == 'posted', F.col('posted_date')).otherwise(F.col('reference_date'))


def validate_parent(parent, location, reference_date=None):
    from pyspark.sql import functions as F, types as T
    expected = {'job_fact_key': T.StringType, 'job_id': T.StringType,
        'primary_location_id': T.StringType, 'location_key': T.StringType,
        'seen_in_silver': T.BooleanType, 'reference_date': T.DateType,
        'dimension_date_policy': T.StringType, 'posted_date': T.DateType}
    jobs.require_columns(parent, expected, 'fact_job_posting: run build_jobs_gold.py first')
    for name, dtype in expected.items():
        if not isinstance(parent.schema[name].dataType, dtype):
            raise ValueError(f'fact_job_posting.{name}: incompatible type')
    jobs.unique(parent, ['job_id'], 'fact_job_posting')
    jobs.unique(parent, ['job_fact_key'], 'fact_job_posting')
    jobs.fail_if(parent, jobs.clean(F.col('job_id')).isNull()
        | ~F.col('job_id').eqNullSafe(jobs.clean(F.col('job_id')))
        | ~F.col('job_fact_key').eqNullSafe(jobs.job_key(F.col('job_id')))
        | F.col('seen_in_silver').isNull()
        | F.col('dimension_date_policy').isNull()
        | ~F.col('dimension_date_policy').isin('posted', 'as_of')
        | ~F.col('primary_location_id').eqNullSafe(jobs.clean(F.col('primary_location_id')))
        | (F.col('primary_location_id') == shared.UNKNOWN_ID), 'Invalid parent identity/policy')
    dates = parent.select('reference_date').distinct().limit(2).collect()
    if reference_date is None:
        if len(dates) != 1 or dates[0]['reference_date'] is None:
            raise ValueError('Cannot infer one reference_date; pass explicitly for an empty parent')
        reference_date = dates[0]['reference_date']
    reference_date = shared.parse_date(reference_date, 'reference_date')
    if reference_date is None:
        raise ValueError('reference_date is required')
    jobs.fail_if(parent, F.col('reference_date').isNull() | (F.col('reference_date') != F.lit(reference_date)),
                 'Parent reference_date differs; rerun build_jobs_gold.py for this batch')
    checked = jobs.lookup(parent.withColumn('_lookup_date', lookup_date()), location,
        shared.SPECS['location'], 'primary_location_id', '_lookup_date', '_expected_key')
    jobs.fail_if(checked, ~F.col('location_key').eqNullSafe(F.col('_expected_key')),
                 'Parent location_key is stale/invalid for its effective-date policy; rerun parent')
    return reference_date


def validate_bridge(frame, parent, location, check_context=True):
    from pyspark.sql import functions as F
    jobs.unique(frame, PAIR, 'bridge_job_location')
    jobs.unique(frame, ['job_id', 'location_id'], 'bridge_job_location business grain')
    jobs.fail_if(frame, jobs.clean(F.col('job_id')).isNull() | jobs.clean(F.col('location_id')).isNull()
        | ~F.col('location_id').eqNullSafe(jobs.clean(F.col('location_id')))
        | (F.col('location_id') == shared.UNKNOWN_ID)
        | F.col('location_key').isNull() | F.col('location_source_as_of').isNull()
        | F.col('reference_date').isNull() | (F.col('location_source_as_of') > F.col('reference_date')),
        'Invalid bridge identity/source dates')
    jobs.fail_if(frame.groupBy('job_fact_key').count(), F.col('count') < 2,
                 'Bridge must contain only jobs with multiple distinct location IDs')
    linked = frame.alias('b').join(parent.alias('p'),
        F.col('b.job_fact_key') == F.col('p.job_fact_key'), 'left')
    jobs.fail_if(linked, F.col('p.job_fact_key').isNull()
        | ~F.col('b.job_id').eqNullSafe(F.col('p.job_id')), 'Bridge has orphan/mismatched parent')
    fk = frame.alias('b').join(location.alias('d'), F.col('b.location_key') == F.col('d.location_key'), 'left')
    jobs.fail_if(fk, F.col('d.location_key').isNull() | ((F.col('b.location_key') != '0')
        & ~F.col('b.location_id').eqNullSafe(F.col('d.location_id'))), 'Bridge has invalid location FK')
    if check_context:
        expected_primary = F.when(F.col('p.primary_location_id').isNotNull(),
                                   F.col('b.location_id') == F.col('p.primary_location_id'))
        jobs.fail_if(linked, ~F.col('b.reference_date').eqNullSafe(F.col('p.reference_date'))
            | ~F.col('b.is_primary').eqNullSafe(expected_primary)
            | (F.col('p.seen_in_silver') & ~F.col('b.location_source_as_of').eqNullSafe(F.col('p.reference_date'))),
            'Bridge context differs from parent')
        mapped = linked.select('b.*', F.col('p.dimension_date_policy'), F.col('p.posted_date'))
        mapped = jobs.lookup(mapped.withColumn('_lookup_date', lookup_date()), location,
            shared.SPECS['location'], 'location_id', '_lookup_date', '_expected_key')
        jobs.fail_if(mapped, ~F.col('location_key').eqNullSafe(F.col('_expected_key')),
                     'Bridge location version differs from parent policy')


def desired_snapshot(evidence, parent, location, old):
    from pyspark.sql import functions as F
    if evidence.select('job_id').distinct().join(parent.select('job_id'), 'job_id', 'left_anti').limit(1).count():
        raise ValueError('Silver job_locations contains job_id missing from fact_job_posting')
    seen = parent.where(F.col('seen_in_silver'))
    fresh = evidence.join(seen.select('job_id', 'reference_date'), 'job_id', 'inner')
    # Complete Silver sets include primary; otherwise a partial source could erase evidence.
    primary = seen.where(F.col('primary_location_id').isNotNull()).select('job_id',
        F.col('primary_location_id').alias('location_id'))
    if primary.join(fresh.select('job_id', 'location_id'), ['job_id', 'location_id'], 'left_anti').limit(1).count():
        raise ValueError('Silver job_locations is missing a parent primary location; require complete sets')
    fresh = fresh.withColumnRenamed('reference_date', 'location_source_as_of')
    retained = parent.where(~F.col('seen_in_silver')).select('job_id')
    if old is None:
        if retained.limit(1).count():
            raise ValueError('Retained parents exist but no previous bridge; cannot reconstruct prior multi-location sets')
        base = fresh
    else:
        validate_bridge(old, parent, location, check_context=False)
        held = old.join(retained, 'job_id', 'inner').select('job_id', 'location_id', 'location_source_as_of')
        base = fresh.unionByName(held)
    multi = base.groupBy('job_id').count().where('count >= 2').select('job_id')
    base = base.join(multi, 'job_id', 'inner')
    contextual = base.join(parent.select('job_id', 'job_fact_key', 'primary_location_id',
        'reference_date', 'posted_date', 'dimension_date_policy'), 'job_id', 'inner')
    contextual = contextual.withColumn('_lookup_date', lookup_date())
    contextual = jobs.lookup(contextual, location, shared.SPECS['location'],
        'location_id', '_lookup_date', 'location_key')
    return (contextual.withColumn('is_primary', F.when(F.col('primary_location_id').isNotNull(),
                    F.col('location_id') == F.col('primary_location_id')))
        .withColumn('loaded_at', F.lit(datetime.now(timezone.utc)).cast('timestamp'))
        .select(*[f.name for f in bridge_schema()]))


def change_counts(desired, old):
    if old is None:
        return {'inserted': desired.count(), 'updated': 0, 'deleted': 0}
    condition = ' OR '.join(f'NOT (s.`{c}` <=> t.`{c}`)' for c in desired.columns if c not in (*PAIR, 'loaded_at'))
    return {'inserted': desired.select(*PAIR).join(old.select(*PAIR), PAIR, 'left_anti').count(),
        'deleted': old.select(*PAIR).join(desired.select(*PAIR), PAIR, 'left_anti').count(),
        'updated': desired.alias('s').join(old.alias('t'), PAIR, 'inner').where(condition).count()}


def synchronize(spark, path, desired, old, changes):
    from delta.tables import DeltaTable
    if old is None:
        desired.write.format('delta').mode('errorifexists').save(path)
        return True
    if not any(changes.values()):
        return False
    condition = ' OR '.join(f'NOT (s.`{c}` <=> t.`{c}`)' for c in desired.columns if c not in (*PAIR, 'loaded_at'))
    (DeltaTable.forPath(spark, path).alias('t').merge(desired.alias('s'),
        't.job_fact_key = s.job_fact_key AND t.location_id = s.location_id')
        .whenMatchedUpdateAll(condition=condition).whenNotMatchedInsertAll()
        .whenNotMatchedBySourceDelete().execute())
    return True


def build_job_bridges(spark=None, *, silver_root=None, shared_root=None, gold_root=None,
                      reference_date=None, max_reference_rows=100000, dry_run=False):
    from pyspark import StorageLevel
    from pyspark.sql import functions as F
    from delta.tables import DeltaTable
    silver_root = (silver_root or os.getenv('JOBS_SILVER_ROOT', 's3a://silver/jobs')).rstrip('/')
    shared_root = (shared_root or os.getenv('SHARED_GOLD_ROOT', 's3a://gold/shared')).rstrip('/')
    gold_root = (gold_root or os.getenv('JOBS_GOLD_ROOT', 's3a://gold/jobs')).rstrip('/')
    roots = [silver_root, shared_root, gold_root]
    if max_reference_rows < 1 or any(p.startswith('s3://') for p in roots):
        raise ValueError('Use s3a:// and a positive max_reference_rows')
    owns_spark = spark is None
    spark = spark or shared.make_spark('gold-job-location-bridge', any(p.startswith('s3a://') for p in roots))
    cached, pinned = [], {}
    try:
        def load(path, schema=None, optional=False):
            frame, version = read_snapshot(spark, path, schema, optional)
            pinned[path] = version
            return frame
        location = load(f'{shared_root}/dim_location', shared.dimension_schema(shared.SPECS['location']))
        shared.validate_dimension(shared.bounded_rows(location, max_reference_rows), shared.SPECS['location'])
        parent = load(f'{gold_root}/fact_job_posting')
        reference_date = validate_parent(parent, location, reference_date)
        evidence = prepare_locations(load(f'{silver_root}/job_locations')).persist(StorageLevel.MEMORY_AND_DISK)
        cached.append(evidence)
        evidence.count()
        path = f'{gold_root}/bridge_job_location'
        old = load(path, bridge_schema(), optional=True)
        if old is not None:
            old = old.persist(StorageLevel.MEMORY_AND_DISK)
            cached.append(old)
            old.count()
        desired = desired_snapshot(evidence, parent, location, old).persist(StorageLevel.MEMORY_AND_DISK)
        cached.append(desired)
        rows = desired.count()
        validate_bridge(desired, parent, location)
        changes = change_counts(desired, old)
        written = False
        if not dry_run:
            for input_path, version in pinned.items():
                assert_version(spark, input_path, version)
            written = synchronize(spark, path, desired, old, changes)
        report = {'table': 'gold_jobs.bridge_job_location', 'path': path, 'rows': rows,
            'multi_location_jobs': desired.select('job_fact_key').distinct().count(),
            'unknown_location_rows': desired.where("location_key = '0'").count(),
            'retained_jobs': parent.where(~F.col('seen_in_silver')).count(),
            'reference_date': reference_date.isoformat(), 'changes': changes, 'written': written,
            'dry_run': dry_run, 'validation': 'passed',
            'output_version': int(DeltaTable.forPath(spark, path).history(1).first()['version']) if written else pinned[path],
            'input_versions': {p: v for p, v in pinned.items() if p != path}}
        LOG.info('Job location bridge: %s', json.dumps(report, ensure_ascii=False))
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
    parser.add_argument('--reference-date', help='Must match parent; inferred if parent is nonempty')
    parser.add_argument('--max-reference-rows', type=int, default=100000)
    parser.add_argument('--dry-run', action='store_true')
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    print(json.dumps(build_job_bridges(**vars(parser.parse_args())), ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()


from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import logging
import os
from functools import reduce
from operator import or_

try:
    from . import build_jobs_gold as jobs
    from . import build_shared_dimensions as shared
    from . import build_fact_job_skill as skills
except ImportError:
    import build_jobs_gold as jobs
    import build_shared_dimensions as shared
    import build_fact_job_skill as skills

LOG = logging.getLogger(__name__)
TABLES = ('agg_job_posting_monthly', 'agg_job_skill_monthly')
GROUPS = ['month_key', 'month_start', 'occupation_id', 'province_name']
COMMON_COUNTS = ('active_count', 'inactive_count', 'activity_unknown_count', 'remote_count',
    'non_remote_count', 'remote_unknown_count', 'retained_count', 'salary_disclosed_count',
    'salary_valid_count', 'salary_excluded_disclosed_count')


def counter_names(kind):
    return (['posting_count', 'job_skill_pair_count', 'required_skill_pair_count'] if kind == 'posting'
        else ['job_skill_pair_count', 'required_pair_count', 'preferred_pair_count', 'requirement_unknown_count']) + list(COMMON_COUNTS)


def aggregate_schema(kind):
    from pyspark.sql import types as T
    fields = [('aggregate_key', T.StringType()), ('month_key', T.IntegerType()),
        ('month_start', T.DateType()), ('occupation_id', T.StringType()), ('province_name', T.StringType())]
    if kind == 'skill':
        fields.append(('skill_id', T.StringType()))
    fields.extend((name, T.LongType()) for name in counter_names(kind))
    fields.extend([('salary_sum_vnd', T.DecimalType(38, 2)), ('salary_avg_vnd', T.DecimalType(38, 6)),
        ('salary_coverage', T.DoubleType()), ('salary_disclosure_coverage', T.DoubleType()),
        ('reference_date', T.DateType()), ('loaded_at', T.TimestampType())])
    return T.StructType([T.StructField(name, dtype, True) for name, dtype in fields])


def count_if(condition, name):
    from pyspark.sql import functions as F
    return F.sum(F.when(condition, 1).otherwise(0)).cast('long').alias(name)


def metric_expressions(kind):
    from pyspark.sql import functions as F
    result = [F.count('*').alias('posting_count' if kind == 'posting' else 'job_skill_pair_count')]
    if kind == 'posting':
        result += [F.sum('_pair_count').cast('long').alias('job_skill_pair_count'),
                   F.sum('skill_count').cast('long').alias('required_skill_pair_count')]
    else:
        result += [count_if(F.col('is_required') == True, 'required_pair_count'),
                   count_if(F.col('is_required') == False, 'preferred_pair_count'),
                   count_if(F.col('is_required').isNull(), 'requirement_unknown_count')]
    for name, condition in [
        ('active_count', F.col('is_active') == True), ('inactive_count', F.col('is_active') == False),
        ('activity_unknown_count', F.col('is_active').isNull()), ('remote_count', F.col('is_remote') == True),
        ('non_remote_count', F.col('is_remote') == False), ('remote_unknown_count', F.col('is_remote').isNull()),
        ('retained_count', ~F.col('seen_in_silver')), ('salary_disclosed_count', F.col('has_salary')),
        ('salary_valid_count', F.col('_salary_valid')),
        ('salary_excluded_disclosed_count', F.col('has_salary') & ~F.col('_salary_valid'))]:
        result.append(count_if(condition, name))
    result.append(F.coalesce(F.sum('_valid_salary'), F.lit(0).cast('decimal(38,2)')).alias('salary_sum_vnd'))
    return result


def validate_inputs(parent, child, dimensions, reference_date):
    from pyspark.sql import functions as F, types as T
    reference_date = skills.validate_parent(parent, dimensions, reference_date)
    skills.validate_child(child, parent, dimensions['skill'])
    expected = {'salary_min_vnd': T.DecimalType(20,2), 'salary_max_vnd': T.DecimalType(20,2),
        'salary_midpoint_vnd': T.DecimalType(20,2), 'has_salary': T.BooleanType(),
        'is_active': T.BooleanType(), 'is_remote': T.BooleanType(), 'salary_status': T.StringType()}
    jobs.require_columns(parent, expected, 'fact_job_posting')
    for name, dtype in expected.items():
        if parent.schema[name].dataType != dtype:
            raise ValueError(f'fact_job_posting.{name}: incompatible type')
    jobs.fail_if(parent, F.col('has_salary').isNull(), 'has_salary must not be NULL')
    counts = child.groupBy('job_fact_key').agg(F.count('*').alias('_pair_count'),
        F.sum(F.when(F.col('is_required') == True, 1).otherwise(0)).alias('_required_count'))
    checked = parent.join(counts, 'job_fact_key', 'left')
    jobs.fail_if(checked, F.col('skill_count') != F.coalesce(F.col('_required_count'), F.lit(0)),
                 'Parent skill_count differs from required pairs in fact_job_skill')
    # Child FK existence alone cannot detect a valid but stale SCD version.
    mapped = child.select('job_fact_key', 'skill_id', 'skill_key').join(parent.select(
        'job_fact_key', 'dimension_date_policy', 'posted_date', 'reference_date'), 'job_fact_key')
    mapped = mapped.withColumn('_lookup_date', F.when(F.col('dimension_date_policy') == 'posted',
        F.col('posted_date')).otherwise(F.col('reference_date')))
    mapped = jobs.lookup(mapped, dimensions['skill'], shared.SPECS['skill'], 'skill_id', '_lookup_date', '_expected_skill')
    jobs.fail_if(mapped, ~F.col('skill_key').eqNullSafe(F.col('_expected_skill')), 'Stale skill version; rerun child fact')
    dates = dimensions['date']
    jobs.unique(dates, ['date_key'], 'dim_date')
    jobs.fail_if(dates, F.col('date_key').isNull() | ((F.col('date_key') != 0) &
        (F.col('full_date').isNull() | (F.col('date_key') != F.date_format('full_date', 'yyyyMMdd').cast('int')))),
        'Invalid dim_date key/date')
    if dates.where('date_key = 0 AND full_date IS NULL').count() != 1:
        raise ValueError('dim_date must contain one unknown date')
    calendar = parent.join(dates.select(F.col('date_key').alias('posted_date_key'), 'full_date'), 'posted_date_key')
    jobs.fail_if(calendar, ~F.col('posted_date').eqNullSafe(F.col('full_date')), 'Posted date and calendar FK differ')
    return reference_date, counts


def prepare_population(parent, child_counts, dimensions):
    from pyspark.sql import functions as F
    # Use the mapped dimension business ID, not the raw possibly-unmapped ID.
    population = parent.drop('occupation_id').join(child_counts.select('job_fact_key', '_pair_count'), 'job_fact_key', 'left')
    population = population.fillna({'_pair_count': 0})
    for name, fields in [('date', ['date_key', 'full_date']), ('occupation', ['occupation_key', 'occupation_id']),
                          ('location', ['location_key', 'province_name'])]:
        dimension = dimensions[name].select(*fields)
        if name == 'date':
            dimension = dimension.withColumnRenamed('date_key', 'posted_date_key')
        key = 'posted_date_key' if name == 'date' else f'{name}_key'
        population = population.join(dimension, key, 'left')
    population = (population.withColumn('month_key', F.coalesce(F.date_format('full_date','yyyyMM').cast('int'), F.lit(0)))
        .withColumn('month_start', F.trunc('full_date', 'month'))
        .withColumn('occupation_id', F.coalesce(F.col('occupation_id'), F.lit(shared.UNKNOWN_ID)))
        .withColumn('province_name', F.coalesce(jobs.clean(F.col('province_name')), F.lit(shared.UNKNOWN_ID))))
    lo, hi, mid = [F.col(c) for c in ['salary_min_vnd', 'salary_max_vnd', 'salary_midpoint_vnd']]
    disclosed = F.col('has_salary') & ~F.coalesce(jobs.token(F.col('salary_status')).isin(*jobs.NEGOTIABLE), F.lit(False))
    valid = F.coalesce(disclosed & (lo > 0) & (hi > 0) & (lo <= hi) & mid.isNotNull()
        & (mid == ((lo + hi) / 2).cast('decimal(20,2)')), F.lit(False))
    return (population.withColumn('_salary_valid', valid)
        .withColumn('_valid_salary', F.when(F.col('_salary_valid'), mid).cast('decimal(20,2)')))


def aggregate_population(population, kind, reference_date):
    from pyspark.sql import functions as F
    keys = GROUPS + (['skill_id'] if kind == 'skill' else [])
    result = population.groupBy(*keys).agg(*metric_expressions(kind))
    count = F.col('posting_count' if kind == 'posting' else 'job_skill_pair_count')
    result = (result.withColumn('salary_avg_vnd', F.when(F.col('salary_valid_count') > 0,
                    F.col('salary_sum_vnd') / F.col('salary_valid_count')).cast('decimal(38,6)'))
        .withColumn('salary_coverage', F.when(count > 0, F.col('salary_valid_count') / count).cast('double'))
        .withColumn('salary_disclosure_coverage', F.when(count > 0, F.col('salary_disclosed_count') / count).cast('double')))
    identity = [F.lit(f'jobs_{kind}_monthly'), F.col('month_key').cast('string'),
                F.col('occupation_id'), F.col('province_name')]
    if kind == 'skill':
        identity.append(F.col('skill_id'))
    result = (result.withColumn('aggregate_key', F.sha2(F.to_json(F.array(*identity)),256))
        .withColumn('reference_date', F.lit(reference_date).cast('date'))
        .withColumn('loaded_at', F.lit(datetime.now(timezone.utc)).cast('timestamp')))
    return result.select(*[f.name for f in aggregate_schema(kind)])


def reconcile(result, source, kind):
    """Reconcile additive components to the distinct source population, not rates."""
    from pyspark.sql import functions as F
    keys = GROUPS + (['skill_id'] if kind == 'skill' else [])
    jobs.unique(result, ['aggregate_key'], f'{kind} aggregate key')
    jobs.unique(result, keys, f'{kind} aggregate grain')
    skills.check_schema(result, aggregate_schema(kind), f'{kind} aggregate')
    measures = counter_names(kind) + ['salary_sum_vnd']
    expected = source.agg(*metric_expressions(kind)).first().asDict()
    actual = result.agg(*[F.sum(c).alias(c) for c in measures]).first().asDict()
    for name in measures:
        if (actual[name] or 0) != (expected[name] or 0):
            raise ValueError(f'{kind} aggregate does not reconcile: {name}')
    count = F.col('posting_count' if kind == 'posting' else 'job_skill_pair_count')
    invalid = [F.col(c).isNull() | (F.col(c) < 0) for c in measures]
    invalid += [(count <= 0), F.col('aggregate_key').isNull(),
        F.col('active_count') + F.col('inactive_count') + F.col('activity_unknown_count') != count,
        F.col('remote_count') + F.col('non_remote_count') + F.col('remote_unknown_count') != count,
        F.col('salary_valid_count') + F.col('salary_excluded_disclosed_count') != F.col('salary_disclosed_count'),
        F.col('salary_disclosed_count') > count, F.col('retained_count') > count,
        ~F.col('salary_coverage').eqNullSafe(F.col('salary_valid_count') / count),
        ~F.col('salary_disclosure_coverage').eqNullSafe(F.col('salary_disclosed_count') / count),
        ~F.col('salary_avg_vnd').eqNullSafe(F.when(F.col('salary_valid_count') > 0,
            F.col('salary_sum_vnd') / F.col('salary_valid_count')).cast('decimal(38,6)'))]
    if kind == 'skill':
        invalid.append(F.col('required_pair_count') + F.col('preferred_pair_count') + F.col('requirement_unknown_count') != count)
    jobs.fail_if(result, reduce(or_, invalid), f'{kind} aggregate has invalid metric components')
    return {'status': 'passed', 'source_rows': int(expected['posting_count' if kind == 'posting' else 'job_skill_pair_count'] or 0),
        'salary_valid_count': int(expected['salary_valid_count'] or 0), 'salary_sum_vnd': str(expected['salary_sum_vnd'])}


def change_counts(desired, old):
    if old is None:
        return {'inserted': desired.count(), 'updated': 0, 'deleted': 0}
    key = ['aggregate_key']
    condition = ' OR '.join(f'NOT (s.`{c}` <=> t.`{c}`)' for c in desired.columns if c not in (*key, 'loaded_at'))
    return {'inserted': desired.select(*key).join(old.select(*key), key, 'left_anti').count(),
        'deleted': old.select(*key).join(desired.select(*key), key, 'left_anti').count(),
        'updated': desired.alias('s').join(old.alias('t'), key).where(condition).count()}


def synchronize(spark, path, desired, old, changes):
    from delta.tables import DeltaTable
    if old is None:
        desired.write.format('delta').mode('errorifexists').save(path)
        return True
    if not any(changes.values()):
        return False
    condition = ' OR '.join(f'NOT (s.`{c}` <=> t.`{c}`)' for c in desired.columns if c not in ('aggregate_key', 'loaded_at'))
    (DeltaTable.forPath(spark, path).alias('t').merge(desired.alias('s'), 't.aggregate_key = s.aggregate_key')
        .whenMatchedUpdateAll(condition=condition).whenNotMatchedInsertAll().whenNotMatchedBySourceDelete().execute())
    return True


def build_aggregates(spark=None, *, gold_root=None, shared_root=None, output_root=None,
                     reference_date=None, max_reference_rows=100000, dry_run=False):
    from pyspark import StorageLevel
    from delta.tables import DeltaTable
    gold_root = (gold_root or os.getenv('JOBS_GOLD_ROOT', 's3a://gold/jobs')).rstrip('/')
    shared_root = (shared_root or os.getenv('SHARED_GOLD_ROOT', 's3a://gold/shared')).rstrip('/')
    output_root = (output_root or os.getenv('JOBS_AGGREGATE_ROOT', gold_root)).rstrip('/')
    roots = [gold_root, shared_root, output_root]
    if max_reference_rows < 1 or any(p.startswith('s3://') for p in roots):
        raise ValueError('Use s3a:// and a positive max_reference_rows')
    owns_spark = spark is None
    spark = spark or shared.make_spark('gold-jobs-aggregates', any(p.startswith('s3a://') for p in roots))
    cached, pinned = [], {}
    try:
        def load(path, schema=None, optional=False):
            frame, version = skills.read_snapshot(spark, path, schema, optional)
            pinned[path] = version
            return frame
        def cache(frame):
            frame = frame.persist(StorageLevel.MEMORY_AND_DISK)
            cached.append(frame)
            frame.count()
            return frame
        dimensions = {}
        for name, spec in shared.SPECS.items():
            dimensions[name] = load(f'{shared_root}/dim_{name}', shared.dimension_schema(spec))
            shared.validate_dimension(shared.bounded_rows(dimensions[name], max_reference_rows), spec)
        dimensions['date'] = load(f'{shared_root}/dim_date', shared.date_schema())
        parent = load(f'{gold_root}/fact_job_posting')
        child = load(f'{gold_root}/fact_job_skill', skills.fact_schema())
        reference_date, counts = validate_inputs(parent, child, dimensions, reference_date)
        population = cache(prepare_population(parent, counts, dimensions))
        # Child uniqueness was checked before this join; one salary sample per
        # job within a skill, regardless of aliases or the number of locations.
        skill_population = cache(child.select('job_fact_key', 'skill_id', 'is_required')
                                  .join(population, 'job_fact_key', 'inner'))
        prepared, report = [], {}
        for kind, table, source in [('posting', TABLES[0], population), ('skill', TABLES[1], skill_population)]:
            path = f'{output_root}/{table}'
            old = load(path, aggregate_schema(kind), optional=True)
            if old is not None:
                jobs.unique(old, ['aggregate_key'], f'existing {table}')
                old = cache(old)
            desired = cache(aggregate_population(source, kind, reference_date))
            reconciliation = reconcile(desired, source, kind)
            changes = change_counts(desired, old)
            prepared.append((table, path, desired, old, changes))
            report[table] = {'path': path, 'rows': desired.count(), 'changes': changes,
                'reference_date': reference_date.isoformat(), 'reconciliation': reconciliation,
                'dry_run': dry_run, 'written': False, 'output_version': pinned[path]}
        # Both outputs preflight before any mutation. Still requires one writer.
        if not dry_run:
            for path, version in pinned.items():
                skills.assert_version(spark, path, version)
            for table, path, desired, old, changes in prepared:
                written = synchronize(spark, path, desired, old, changes)
                report[table]['written'] = written
                if written:
                    report[table]['output_version'] = int(DeltaTable.forPath(spark, path).history(1).first()['version'])
        output_paths = {item[1] for item in prepared}
        report['input_versions'] = {p: v for p, v in pinned.items() if p not in output_paths}
        LOG.info('Recruitment aggregates: %s', json.dumps(report, ensure_ascii=False))
        return report
    finally:
        for frame in reversed(cached):
            frame.unpersist()
        if owns_spark:
            spark.stop()


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--gold-root', default=os.getenv('JOBS_GOLD_ROOT', 's3a://gold/jobs'))
    parser.add_argument('--shared-root', default=os.getenv('SHARED_GOLD_ROOT', 's3a://gold/shared'))
    parser.add_argument('--output-root', default=os.getenv('JOBS_AGGREGATE_ROOT'))
    parser.add_argument('--reference-date', help='Must match parent snapshot; inferred for a nonempty parent')
    parser.add_argument('--max-reference-rows', type=int, default=100000)
    parser.add_argument('--dry-run', action='store_true')
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    print(json.dumps(build_aggregates(**vars(parser.parse_args())), ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()

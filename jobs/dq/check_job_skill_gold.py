"""Validate job/skill Gold snapshots and block publication on any violation.

The local-batch adapter bounds every input before collecting it on the driver.
An exact current Silver pair-set comparison includes optional skills and jobs
whose edited skill set is empty. Historical input also checks retained jobs.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
import json
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT / 'jobs') not in sys.path:
    sys.path.insert(0, str(ROOT / 'jobs'))

from common.dq_rules import blank, load_rules, number

CONTEXT = {'date_key': 'posted_date_key', 'location_key': 'location_key',
           'occupation_key': 'occupation_key', 'industry_key': 'industry_key',
           'reference_date': 'reference_date', 'skill_source_as_of': 'skill_count_as_of'}
CHILD_COLUMNS = {'job_fact_key', 'skill_key', 'job_id', 'skill_id', 'is_required',
                 'confidence_score', 'years_required', *CONTEXT}
PARENT_COLUMNS = {'job_fact_key', 'job_id', 'skill_count', 'seen_in_silver',
                  'dimension_date_policy', 'posted_date', *CONTEXT.values()}
DIMENSIONS = ('skill', 'date', 'location', 'occupation', 'industry')


def required_columns():
    return {'fact_job_skill': CHILD_COLUMNS, 'fact_job_posting': PARENT_COLUMNS,
            'job_skills': {'job_id', 'skill_id', 'is_required'},
            **{'dim_' + name: ({'skill_key', 'skill_id', 'valid_from', 'valid_to'} if name == 'skill'
                               else {name + '_key'}) for name in DIMENSIONS}}


def day(value):
    if isinstance(value, datetime):
        return value.date()
    return value if isinstance(value, date) else date.fromisoformat(str(value))


def boolean(value):
    if value is None or isinstance(value, bool):
        return value
    value = str(value).strip().lower()
    if value in {'true', '1'}:
        return True
    if value in {'false', '0'}:
        return False
    raise ValueError('Expected boolean or null')


def evaluate(fact, parent, silver, dimensions, *, previous=None, columns=None, issues=(), rules=None):
    """Read-only checks. skill_count counts only is_required=True, as in the builder.

    ``previous`` is the pre-update fact snapshot, not a fabricated empty input.
    ``columns`` supplies actual schemas so empty Delta inputs are validated too.
    All listed checks are mandatory and cannot be downgraded to warnings.
    """
    rules = rules or load_rules()
    findings = list(issues)
    checks = []

    def record(code, count):
        checks.append({'code': code, 'violations': count, 'status': 'failed' if count else 'passed'})
        if count:
            findings.append({'severity': 'error', 'code': code, 'count': count})

    inputs = {'fact_job_skill': fact, 'fact_job_posting': parent, 'job_skills': silver,
              **{'dim_' + name: dimensions.get(name) for name in DIMENSIONS}}
    for name, required in required_columns().items():
        rows = inputs[name]
        record('MISSING_INPUT:' + name, int(rows is None))
        if rows is None:
            continue
        if columns is not None:
            record('MISSING_COLUMNS:' + name, len(required - set(columns.get(name, ()))))
        record('MISSING_FIELDS:' + name, sum(not required.issubset(row) for row in rows))
    if any(item['severity'] == 'error' for item in findings):
        return _report(rules, findings, checks)

    def unique(rows, fields, label):
        counts = Counter(tuple(row.get(f) for f in fields) for row in rows)
        record('UNIQUE:' + label, sum(count - 1 for count in counts.values()))

    for fields in [('job_fact_key', 'skill_key'), ('job_id', 'skill_id')]:
        unique(fact, fields, 'fact_job_skill.' + '+'.join(fields))
    unique(parent, ['job_fact_key'], 'parent.job_fact_key')
    unique(parent, ['job_id'], 'parent.job_id')
    for name in DIMENSIONS:
        unique(dimensions[name], [name + '_key'], 'dim_' + name)

    record('NULL_IDENTITY', sum(any(blank(row.get(k)) for k in ('job_fact_key', 'skill_key', 'job_id', 'skill_id'))
                                for row in fact))
    record('NULL_PARENT_IDENTITY', sum(any(blank(row.get(k)) for k in ('job_fact_key', 'job_id')) for row in parent))
    record('NULL_SILVER_IDENTITY', sum(any(blank(row.get(k)) for k in ('job_id', 'skill_id')) for row in silver))
    record('PARENT_SNAPSHOT_FLAGS', sum(type(row['seen_in_silver']) is not bool or
        type(row['skill_count']) is not int or row['skill_count'] < 0 or
        row['dimension_date_policy'] not in {'posted', 'as_of'} for row in parent))
    by_job = {row['job_fact_key']: row for row in parent}
    by_id = {row['job_id']: row for row in parent}
    dim_keys = {name: {row[name + '_key'] for row in dimensions[name]} for name in DIMENSIONS}
    record('FK:job', sum(blank(row['job_fact_key']) or row['job_fact_key'] not in by_job for row in fact))
    for name in DIMENSIONS:
        key = name + '_key'
        record('FK:' + name, sum(blank(row[key]) or row[key] not in dim_keys[name] for row in fact))
    record('UNKNOWN_SKILL', sum(row['skill_key'] == '0' for row in fact))
    record('FK:silver_job', sum(row['job_id'] not in by_id for row in silver))
    linked = [(row, by_job[row['job_fact_key']]) for row in fact if row['job_fact_key'] in by_job]
    record('PARENT_JOB_ID', sum(row['job_id'] != job['job_id'] for row, job in linked))
    for child_field, parent_field in CONTEXT.items():
        record('PARENT_CONTEXT:' + child_field,
               sum(row[child_field] != job[parent_field] for row, job in linked))

    skill_by_key = {row['skill_key']: row for row in dimensions['skill']}
    skills_by_id = defaultdict(list)
    for row in dimensions['skill']:
        skills_by_id[row['skill_id']].append(row)
    invalid_mapping = invalid_version = 0
    for row, job in linked:
        skill = skill_by_key.get(row['skill_key'])
        invalid_mapping += skill is None or skill['skill_id'] != row['skill_id']
        try:
            lookup = day(job['posted_date'] if job['dimension_date_policy'] == 'posted' else job['reference_date'])
            active = [s['skill_key'] for s in skills_by_id[row['skill_id']]
                      if day(s['valid_from']) <= lookup < day(s['valid_to'])]
            invalid_version += active != [row['skill_key']]
        except (ValueError, TypeError):
            invalid_version += 1
    record('SKILL_ID_KEY_MAPPING', invalid_mapping)
    record('SKILL_EFFECTIVE_VERSION', invalid_version)

    for field, maximum in [('confidence_score', Decimal(1)), ('years_required', None)]:
        invalid = 0
        for row in fact:
            if row[field] is None:
                continue
            try:
                value = number(row[field])
                invalid += value < 0 or (maximum is not None and value > maximum)
            except (ValueError, TypeError, InvalidOperation):
                invalid += 1
        record('RANGE:' + field, invalid)
    record('TYPE:is_required', sum(row['is_required'] is not None and type(row['is_required']) is not bool
                                   for row in fact))

    seen = {row['job_id'] for row in parent if row['seen_in_silver'] is True}
    retained = {row['job_id'] for row in parent if row['seen_in_silver'] is False}
    expected = {(row['job_id'], row['skill_id']) for row in silver if row['job_id'] in seen}
    observed = {(row['job_id'], row['skill_id']) for row in fact if row['job_id'] in seen}
    record('PAIR_SET_MISSING', len(expected - observed))
    record('PAIR_SET_EXTRA_OR_STALE', len(observed - expected))
    flags = {}
    flag_sets = defaultdict(set)
    invalid_flags = 0
    for row in silver:
        try:
            pair = (row['job_id'], row['skill_id'])
            flags[pair] = boolean(row['is_required'])
            flag_sets[pair].add(flags[pair])
        except ValueError:
            invalid_flags += 1
    record('SILVER_REQUIRED_FLAG', invalid_flags)
    # Repeated identical Silver evidence is canonicalized by the builder too.
    record('SILVER_CONFLICTING_PAIR', sum(len(values) > 1 for values in flag_sets.values()))
    record('REQUIRED_FLAG_MISMATCH', sum(row['is_required'] != flags.get((row['job_id'], row['skill_id']))
        for row in fact if row['job_id'] in seen and (row['job_id'], row['skill_id']) in expected))
    counts = Counter(row['job_fact_key'] for row in fact if row['is_required'] is True)
    record('SKILL_COUNT', sum(row['skill_count'] != counts[row['job_fact_key']] for row in parent))

    deletion = {'baseline_available': previous is not None, 'obsolete_pairs': None, 'remaining_pairs': None}
    if previous is not None:
        old = {(row['job_id'], row['skill_id']) for row in previous}
        all_current = {(row['job_id'], row['skill_id']) for row in fact}
        obsolete = {pair for pair in old if pair[0] in seen} - expected
        remaining = obsolete & all_current
        record('STALE_SKILL_NOT_DELETED', len(remaining))
        old_retained = {pair for pair in old if pair[0] in retained}
        new_retained = {pair for pair in all_current if pair[0] in retained}
        record('RETAINED_PAIR_SET_CHANGED', len(old_retained ^ new_retained))
        deletion.update(obsolete_pairs=len(obsolete), remaining_pairs=len(remaining))
    result = _report(rules, findings, checks)
    result.update(rows=len(fact), expected_current_pairs=len(expected), observed_current_pairs=len(observed),
                  retained_jobs=len(retained), deletion_check=deletion)
    return result


def _report(rules, issues, checks):
    allowed = not any(item['severity'] == 'error' for item in issues)
    return dict(table='fact_job_skill', rules_version=rules['version'], rules_checksum=rules['checksum'],
                status='passed' if allowed else 'failed', publish_allowed=allowed,
                checks=checks, issues=issues)


def require_pass(report):
    if not report['publish_allowed']:
        codes = ', '.join(item['code'] for item in report['issues'])
        raise ValueError('Job skill Gold DQ failed; publish blocked: ' + codes)


def check_frames(fact, parent, silver, dimensions, *, previous=None, max_rows=100000):
    """Bound collection; exceeding the batch limit fails instead of sampling."""
    if max_rows < 1:
        raise ValueError('max_rows must be positive')
    frames = {'fact_job_skill': fact, 'fact_job_posting': parent, 'job_skills': silver,
              **{'dim_' + name: dimensions.get(name) for name in DIMENSIONS}}
    columns, data, issues = {}, {}, []
    if previous is not None:
        frames['previous'] = previous
    for name, frame in frames.items():
        if frame is None:
            data[name] = None
            continue
        columns[name] = frame.columns
        # Project only validation fields; no raw descriptions/URLs in driver memory.
        wanted = required_columns().get(name, {'job_id', 'skill_id'})
        rows = frame.select(*sorted(set(frame.columns) & wanted)).limit(max_rows + 1).collect()
        if len(rows) > max_rows:
            issues.append(dict(severity='error', code='ROW_LIMIT:' + name, count=len(rows)))
            data[name] = []
        else:
            data[name] = [row.asDict(recursive=True) for row in rows]
    return evaluate(data['fact_job_skill'], data['fact_job_posting'], data['job_skills'],
                    {name: data['dim_' + name] for name in DIMENSIONS}, previous=data.get('previous'),
                    columns=columns, issues=issues)


def check(spark, *, silver_root=None, shared_root=None, gold_root=None,
          previous_version=None, max_rows=100000):
    from gold.build_fact_job_skill import read_snapshot, assert_version
    from delta.tables import DeltaTable
    silver_root = (silver_root or os.getenv('JOBS_SILVER_ROOT', 's3a://silver/jobs')).rstrip('/')
    shared_root = (shared_root or os.getenv('SHARED_GOLD_ROOT', 's3a://gold/shared')).rstrip('/')
    gold_root = (gold_root or os.getenv('JOBS_GOLD_ROOT', 's3a://gold/jobs')).rstrip('/')
    paths = {'fact_job_skill': f'{gold_root}/fact_job_skill', 'fact_job_posting': f'{gold_root}/fact_job_posting',
             'job_skills': f'{silver_root}/job_skills',
             **{'dim_' + name: f'{shared_root}/dim_{name}' for name in DIMENSIONS}}
    frames, versions = {}, {}
    for name, path in paths.items():
        frames[name], versions[path] = read_snapshot(spark, path)
    path = paths['fact_job_skill']
    current = versions[path]
    if previous_version is None:
        # Read-only idempotent reruns need not have committed a new version.
        history = DeltaTable.forPath(spark, path).history().select('version').where(f'version < {current}')
        row = history.orderBy('version', ascending=False).first()
        previous_version = int(row['version']) if row else None
    if previous_version is not None and not 0 <= previous_version < current:
        raise ValueError('previous_version must identify an earlier Delta version')
    previous = (spark.read.format('delta').option('versionAsOf', previous_version).load(path)
                if previous_version is not None else None)
    result = check_frames(frames['fact_job_skill'], frames['fact_job_posting'], frames['job_skills'],
                          {name: frames['dim_' + name] for name in DIMENSIONS},
                          previous=previous, max_rows=max_rows)
    for input_path, version in versions.items():
        assert_version(spark, input_path, version)
    result.update(input_versions=versions, previous_version=previous_version,
                  checked_at=datetime.now(timezone.utc).isoformat())
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--silver-root')
    parser.add_argument('--shared-root')
    parser.add_argument('--gold-root')
    parser.add_argument('--previous-version', type=int)
    parser.add_argument('--max-rows', type=int, default=100000)
    args = parser.parse_args()
    from gold.build_shared_dimensions import make_spark
    spark = None
    try:
        roots = [args.silver_root or os.getenv('JOBS_SILVER_ROOT', 's3a://silver/jobs'),
                 args.shared_root or os.getenv('SHARED_GOLD_ROOT', 's3a://gold/shared'),
                 args.gold_root or os.getenv('JOBS_GOLD_ROOT', 's3a://gold/jobs')]
        spark = make_spark('dq-job-skill-gold', any(p.startswith('s3a://') for p in roots))
        report = check(spark, **vars(args))
    except Exception as exc:
        # Do not echo endpoints, connection strings or Spark exception payloads.
        report = dict(status='failed', publish_allowed=False,
                      issues=[dict(severity='error', code='DQ_TASK_FAILED', error_type=type(exc).__name__)])
    finally:
        if spark is not None:
            spark.stop()
    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    return 0 if report['publish_allowed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())

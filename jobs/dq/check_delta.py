"""Check actual Delta snapshots before publish (local batch sizes fit driver memory)."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT / 'jobs') not in sys.path:
    sys.path.insert(0, str(ROOT / 'jobs'))

from common.dq_rules import load_rules, load_references, evaluate_tables
from common.spark_session import create_spark_session
from common.delta_io import delta_location
from dq.check_job_skill_gold import evaluate as evaluate_job_skills
from dq.gold_integrity import evaluate as evaluate_gold_integrity
from common.audit import _atomic_write
from uuid import uuid4


def check(spark, rules_path=None):
    from delta.tables import DeltaTable
    config = load_rules(rules_path)
    tables, versions, issues, references, columns = {}, {}, [], {}, {}
    try:
        references = load_references(config)
    except (OSError, ValueError, KeyError) as exc:
        issues.append(dict(severity='error', code='REFERENCE_READ_FAILED', message=str(exc)))
    for name in config['tables']:
        try:
            path = delta_location(name)
            version = int(DeltaTable.forPath(spark, path).history(1).first()['version'])
            df = spark.read.format('delta').option('versionAsOf', version).load(path)
            columns[name] = df.columns
            rule = config['tables'][name]
            required = set(rule['primary_key']) | set(rule['grain']['columns']) | set(rule['not_null'])
            required.update(rule['types'])
            required.update(rule['ranges'])
            for fk in rule['foreign_keys']:
                required.update(fk['columns'])
            required.update(c['column'] for c in rule['mapping_coverage'])
            missing = required - set(df.columns)
            if missing:
                issues.append(dict(table=name, severity='error', code='MISSING_COLUMNS',
                                   message=', '.join(sorted(missing))))
            tables[name] = [json.loads(json.dumps(row.asDict(recursive=True), default=str, allow_nan=False))
                            for row in df.toLocalIterator()]
            versions[name] = version
        except Exception as exc:
            issues.append(dict(table=name, severity='error', code='DELTA_READ_FAILED',
                               message=type(exc).__name__))
    job_skill_report = evaluate_job_skills(tables.get('fact_job_skill'), tables.get('fact_job_posting'),
        tables.get('job_skills'), {name: tables.get('dim_' + name)
                                  for name in ('skill', 'date', 'location', 'occupation', 'industry')},
        columns=columns, rules=config)
    issues.extend(job_skill_report['issues'])
    issues.extend(evaluate_gold_integrity(tables))
    result = evaluate_tables(tables, config, references=references, issues=issues)
    result['job_skill_gold'] = job_skill_report
    result['delta_versions'] = versions
    result['checked_at'] = datetime.now(timezone.utc).isoformat()
    result['run_id'] = uuid4().hex
    _atomic_write(ROOT / 'data' / 'audit' / 'dq_gold' / (result['run_id'] + '.json'), result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rules', type=Path)
    args = parser.parse_args()
    spark = create_spark_session('dq-delta-publish')
    try:
        result = check(spark, args.rules)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0 if result['publish_allowed'] else 1
    finally:
        spark.stop()


if __name__ == '__main__':
    raise SystemExit(main())

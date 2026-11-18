"""Validate Gold tables using the versioned DQ configuration."""
import argparse
import json
from datetime import datetime, timezone
from uuid import uuid4
from pathlib import Path
from _common import ROOT, read_jsonl
from common.dq_rules import load_rules, evaluate_tables
from common.audit import _atomic_write
from dq.gold_integrity import evaluate as evaluate_gold_integrity


def check(input_dir: Path, rules_path=None, report_dir=None) -> int:
    config = load_rules(rules_path)
    names = [name for name, rule in config["tables"].items() if rule["layer"] == "gold"]
    issues = []
    tables = {name: read_jsonl(input_dir / f"{name}.jsonl", issues, name) for name in names}
    for name in ('bridge_job_location', 'job_locations'):
        path = input_dir / f'{name}.jsonl'
        if path.is_file():
            tables[name] = read_jsonl(path, issues, name)
    issues.extend(evaluate_gold_integrity(tables))
    result = evaluate_tables(tables, config, selected=names,
                             issues=[i for i in issues if i["code"] != "EMPTY_DATASET"])
    result['run_id'] = uuid4().hex
    result['checked_at'] = datetime.now(timezone.utc).isoformat()
    report_path = Path(report_dir or ROOT / 'data' / 'audit' / 'dq_gold') / (result['run_id'] + '.json')
    _atomic_write(report_path, result)
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0 if result["publish_allowed"] else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, default=ROOT / "data/gold/tables")
    parser.add_argument("--rules", type=Path)
    parser.add_argument("--report-dir", type=Path)
    args = parser.parse_args()
    return check(args.input_dir, args.rules, args.report_dir)


if __name__ == "__main__":
    raise SystemExit(main())

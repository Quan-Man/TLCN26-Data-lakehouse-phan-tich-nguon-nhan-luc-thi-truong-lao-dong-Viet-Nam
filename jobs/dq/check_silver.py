"""Validate a frozen Silver snapshot against versioned DQ rules."""
import argparse
import csv
import json
from pathlib import Path
from _common import ROOT, read_jsonl
from common.dq_rules import load_rules, evaluate_tables


def check(root: Path, rules_path=None) -> int:
    config = load_rules(rules_path)
    names = [name for name, rule in config["tables"].items() if rule["layer"] == "silver"]
    issues, tables, references = [], {}, {}
    try:
        pointer = json.loads((root / "latest.json").read_text(encoding="utf-8-sig"))
        snapshot = (root / pointer["snapshot"]).resolve()
        if not snapshot.is_relative_to(root.resolve()):
            raise ValueError("Snapshot path escapes Silver root")
        manifest = json.loads((snapshot / "manifest.json").read_text(encoding="utf-8-sig"))
        if manifest.get("dataset") != "silver.jobs" or manifest.get("status") != "completed":
            raise ValueError("Silver snapshot is not completed")
        tables = {name: read_jsonl(snapshot / f"{name}.jsonl", issues, name) for name in names}
        for name, ref in config["references"].items():
            with (snapshot / "references" / Path(ref["path"]).name).open(encoding="utf-8-sig", newline="") as stream:
                references[name] = list(csv.DictReader(stream))
    except (OSError, ValueError, KeyError) as exc:
        issues.append(dict(severity="error", code="SNAPSHOT_READ_FAILED", message=str(exc)))
    result = evaluate_tables(tables, config, references=references, selected=names,
                             issues=[i for i in issues if i["code"] != "EMPTY_DATASET"])
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0 if result["publish_allowed"] else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, default=ROOT / "data/silver/jobs")
    parser.add_argument("--rules", type=Path)
    args = parser.parse_args()
    return check(args.input_dir, args.rules)


if __name__ == "__main__":
    raise SystemExit(main())

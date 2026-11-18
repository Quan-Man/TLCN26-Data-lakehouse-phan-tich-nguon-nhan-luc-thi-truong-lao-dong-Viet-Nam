"""Validate Bronze manifests, record shape, types, and within-run duplicate keys."""
from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from pathlib import Path

from _common import ROOT, add, check_rows, report

sys.path.insert(0, str(ROOT / "jobs"))
from silver.process_job_silver import read_bronze  # noqa: E402


def check(input_dir: Path) -> int:
    issues: list[dict] = []
    checked = {"bronze_records": 0}
    try:
        records, manifests = read_bronze(input_dir)
    except Exception as exc:
        add(issues, "error", "BRONZE_READ_FAILED", str(exc))
        return report("bronze", checked, issues)

    checked["bronze_records"] = len(records)
    checked["completed_manifests"] = len(manifests)
    if not records:
        add(issues, "warning", "EMPTY_DATASET", "Bronze contains no job records", table="job_posting")

    run_keys: dict[str, dict[tuple, int]] = defaultdict(dict)
    for index, envelope in enumerate(records, 1):
        payload = envelope.get("raw_payload")
        metadata = envelope.get("_metadata")
        if not isinstance(payload, dict) or not isinstance(metadata, dict):
            add(issues, "error", "INVALID_ENVELOPE", "Bronze row needs object raw_payload and _metadata",
                table="job_posting", examples=[{"row": index}])
            continue
        required = []
        if not payload.get("source_name"):
            required.append("source_name")
        if not any(payload.get(name) for name in ("job_title", "job_title_raw", "title")):
            required.append("job_title/job_title_raw/title")
        for field in required:
            add(issues, "error", "MISSING_VALUE", f"Missing required Bronze value: {field}",
                table="job_posting", examples=[{"row": index}])

        for field in ("source_name", "job_title", "job_title_raw", "description", "requirements"):
            value = payload.get(field)
            if value is not None and not isinstance(value, str):
                add(issues, "error", "INVALID_TYPE", f"{field} must be a string when present",
                    table="job_posting", examples=[{"row": index, "value_type": type(value).__name__}])
        skills = payload.get("skills", payload.get("skills_raw"))
        if skills is not None and not isinstance(skills, (str, list)):
            add(issues, "error", "INVALID_TYPE", "skills must be a string or list when present",
                table="job_posting", examples=[{"row": index, "value_type": type(skills).__name__}])
        if isinstance(skills, list) and any(not isinstance(value, str) for value in skills):
            add(issues, "error", "INVALID_TYPE", "skills list must contain only strings",
                table="job_posting", examples=[{"row": index}])

        source_id = payload.get("job_id", payload.get("source_job_id"))
        if source_id is not None:
            if not isinstance(source_id, (str, int)) or isinstance(source_id, bool):
                add(issues, "error", "INVALID_TYPE", "job_id must be a string or integer",
                    table="job_posting", examples=[{"row": index}])
            else:
                run_id = str(metadata.get("run_id", ""))
                key = (str(payload.get("source_name", "")).casefold(), str(source_id))
                if key in run_keys[run_id]:
                    add(issues, "error", "DUPLICATE_KEY", "Duplicate source job key within Bronze run",
                        table="job_posting", examples=[{"run_id": run_id, "key": key,
                                                          "rows": [run_keys[run_id][key], index]}])
                else:
                    run_keys[run_id][key] = index

    # Metadata types are checked separately from raw, source-specific payloads.
    metadata_rows = [row.get("_metadata", {}) for row in records if isinstance(row.get("_metadata"), dict)]
    check_rows(issues, "bronze_metadata", metadata_rows,
        required=("source_name", "source_file", "row_number", "run_id", "record_sha256"),
        types={"source_name": "string", "source_file": "string", "row_number": "integer",
               "run_id": "string", "record_sha256": "string"})
    return report("bronze", checked, issues)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, default=ROOT / "data" / "bronze" / "job_posting")
    args = parser.parse_args()
    return check(args.input_dir)


if __name__ == "__main__":
    raise SystemExit(main())

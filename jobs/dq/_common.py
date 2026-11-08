"""Shared data-quality checks and JSON reporting."""
from __future__ import annotations

import json
from collections import Counter
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
import sys
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT / "jobs") not in sys.path:
    sys.path.insert(0, str(ROOT / "jobs"))


def add(issues: list[dict], severity: str, code: str, message: str,
        *, table: str | None = None, count: int = 1, examples: list[Any] | None = None) -> None:
    issues.append({"severity": severity, "code": code, "table": table,
                   "count": count, "message": message, "examples": (examples or [])[:5]})


def read_jsonl(path: Path, issues: list[dict], table: str) -> list[dict]:
    if not path.is_file():
        add(issues, "error", "MISSING_DATASET", f"Missing data file: {path}", table=table)
        return []
    rows = []
    with path.open(encoding="utf-8-sig") as stream:
        for line_number, line in enumerate(stream, 1):
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError as exc:
                add(issues, "error", "INVALID_JSON", f"Invalid JSON at {path}:{line_number}: {exc.msg}", table=table)
                continue
            if not isinstance(value, dict):
                add(issues, "error", "INVALID_ROW", f"Expected JSON object at {path}:{line_number}", table=table)
                continue
            rows.append(value)
    if not rows:
        add(issues, "warning", "EMPTY_DATASET", "Dataset contains no records", table=table)
    return rows


def _matches_type(value: Any, kind: str) -> bool:
    if kind == "string":
        return isinstance(value, str) and bool(value.strip())
    if kind == "date":
        if not isinstance(value, str):
            return False
        try:
            date.fromisoformat(value)
            return True
        except ValueError:
            return False
    if kind == "timestamp":
        if not isinstance(value, str):
            return False
        try:
            datetime.fromisoformat(value.replace("Z", "+00:00"))
            return True
        except ValueError:
            return False
    if kind == "boolean":
        return type(value) is bool
    if kind == "integer":
        return type(value) is int
    if kind == "numeric":
        return type(value) in {int, float} or isinstance(value, Decimal) or (
            isinstance(value, str) and _decimal_string(value)
        )
    if kind == "array":
        return isinstance(value, list)
    return True


def _decimal_string(value: str) -> bool:
    try:
        Decimal(value)
        return True
    except Exception:
        return False


def check_rows(issues: list[dict], table: str, rows: list[dict], *,
               required: tuple[str, ...] = (), types: dict[str, str] | None = None,
               primary_key: tuple[str, ...] = ()) -> None:
    types = types or {}
    for index, row in enumerate(rows, 1):
        for field in required:
            if field not in row or row[field] is None or row[field] == "":
                add(issues, "error", "MISSING_VALUE", f"Required field {field} is missing", table=table,
                    examples=[{"row": index, "key": [row.get(k) for k in primary_key]}])
        for field, kind in types.items():
            value = row.get(field)
            if value is not None and not _matches_type(value, kind):
                add(issues, "error", "INVALID_TYPE", f"Field {field} must be {kind}", table=table,
                    examples=[{"row": index, "value": repr(value)}])
        if primary_key and any(row.get(field) is None or row.get(field) == "" for field in primary_key):
            add(issues, "error", "MISSING_KEY", f"Primary key fields {primary_key} must be populated", table=table,
                examples=[{"row": index}])

    if primary_key:
        seen: dict[tuple, int] = {}
        duplicates = []
        for index, row in enumerate(rows, 1):
            key = tuple(row.get(field) for field in primary_key)
            if any(part is None or part == "" for part in key):
                continue
            if key in seen:
                duplicates.append({"key": key, "rows": [seen[key], index]})
            else:
                seen[key] = index
        if duplicates:
            add(issues, "error", "DUPLICATE_KEY", f"Duplicate primary key in {table}", table=table,
                count=len(duplicates), examples=duplicates)


def check_foreign_key(issues: list[dict], child_name: str, children: list[dict],
                      child_field: str, parent_name: str, parents: list[dict],
                      parent_field: str, *, nullable: bool = True) -> None:
    parent_values = {row.get(parent_field) for row in parents if row.get(parent_field) is not None}
    invalid = []
    for index, row in enumerate(children, 1):
        value = row.get(child_field)
        if value is None or value == "":
            if not nullable:
                invalid.append({"row": index, "value": value})
        elif value not in parent_values:
            invalid.append({"row": index, "value": value})
    if invalid:
        add(issues, "error", "INVALID_REFERENCE",
            f"{child_name}.{child_field} references missing {parent_name}.{parent_field}",
            table=child_name, count=len(invalid), examples=invalid)


def report(layer: str, checked: dict[str, int], issues: list[dict]) -> int:
    counts = Counter(issue["severity"] for issue in issues)
    result = {"layer": layer, "status": "failed" if counts["error"] else "passed",
              "checked": checked, "issue_counts": dict(counts), "issues": issues}
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 1 if counts["error"] else 0


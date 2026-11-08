"""Shared file and snapshot helpers for the Gold jobs."""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
import sys
if str(ROOT / "jobs") not in sys.path:
    sys.path.insert(0, str(ROOT / "jobs"))
from common.table_catalog import table_spec
SILVER_ROOT = ROOT / "data" / "silver" / "jobs"
GOLD_ROOT = ROOT / "data" / "gold"
TABLE_DIR = GOLD_ROOT / "tables"


def read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open(encoding="utf-8-sig") as stream:
        return [json.loads(line) for line in stream if line.strip()]


def read_silver_snapshot(root: Path = SILVER_ROOT) -> tuple[Path, dict[str, list[dict]]]:
    pointer_path = root / "latest.json"
    if not pointer_path.is_file():
        raise FileNotFoundError(f"Silver latest pointer not found: {pointer_path}")
    pointer = json.loads(pointer_path.read_text(encoding="utf-8-sig"))
    relative = Path(pointer["snapshot"])
    snapshot = (root / relative).resolve()
    if not snapshot.is_relative_to(root.resolve()):
        raise ValueError("Silver snapshot path escapes its root")
    manifest_path = snapshot / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8-sig"))
    if manifest.get("dataset") != "silver.jobs" or manifest.get("status") != "completed":
        raise ValueError("Silver snapshot manifest is not completed silver.jobs")
    tables = {
        name: read_jsonl(snapshot / f"{name}.jsonl")
        for name in ("job_postings", "companies", "job_locations", "job_skills")
    }
    return snapshot, tables


def write_table(name: str, rows: list[dict], output_dir: Path = TABLE_DIR) -> Path:
    table_spec(name)
    output_dir.mkdir(parents=True, exist_ok=True)
    target = output_dir / f"{name}.jsonl"
    temporary = target.with_suffix(".jsonl.tmp")
    with temporary.open("w", encoding="utf-8", newline="\n") as stream:
        for row in rows:
            stream.write(json.dumps(row, ensure_ascii=False, allow_nan=False) + "\n")
    temporary.replace(target)
    return target


def read_table(name: str, input_dir: Path = TABLE_DIR) -> list[dict]:
    table_spec(name)
    return read_jsonl(input_dir / f"{name}.jsonl")


def int_date(value: str | None) -> int | None:
    if not value:
        return None
    return int(value.replace("-", ""))

"""Build Gold dimensions from Silver job snapshots and project mappings."""
from __future__ import annotations

import argparse
import csv
from datetime import date
from pathlib import Path

from _common import ROOT, TABLE_DIR, read_silver_snapshot, write_table, int_date


def csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.is_file():
        return []
    with path.open(encoding="utf-8-sig", newline="") as stream:
        return [{k: (v or "").strip() for k, v in row.items()}
                for row in csv.DictReader(stream)]


def build(mapping_dir: Path, silver_dir: Path, output_dir: Path) -> dict[str, int]:
    _, tables = read_silver_snapshot(silver_dir)
    skills = csv_rows(mapping_dir / "skill_dictionary.csv")
    locations = csv_rows(mapping_dir / "location_dictionary.csv")
    company_rows = {r["company_id"]: r for r in tables["companies"]}
    dates = sorted({r["posted_date"] for r in tables["job_postings"] if r.get("posted_date")})

    dim_dates = []
    for value in dates:
        d = date.fromisoformat(value)
        dim_dates.append(dict(date_key=int_date(value), full_date=value,
            day_of_month=d.day, day_of_week=d.isoweekday(), day_name=d.strftime("%A"),
            week_of_year=int(d.strftime("%V")), month_number=d.month,
            month_name=d.strftime("%B"), quarter_number=(d.month - 1) // 3 + 1,
            year_number=d.year, is_weekend=d.isoweekday() >= 6, is_holiday=False))

    dimensions = {
        "dim_date": dim_dates,
        "dim_location": [dict(location_id=r["location_id"], location_name=r["location_name"],
            location_type=r.get("location_type") or "unknown", province_name=r["location_name"],
            country_code="VN", is_current=True) for r in locations],
        "dim_company": [dict(company_id=cid, company_name=r["company_name"],
            company_name_normalized=r.get("company_name_normalized"), source_name=r.get("source_name"),
            is_active=True) for cid, r in sorted(company_rows.items())],
        "dim_skill": [dict(skill_id=r["skill_id"], skill_name=r["skill_name"],
            skill_category=r.get("skill_category"), skill_type=r.get("skill_type"),
            is_active=r.get("is_active", "1").lower() not in {"0", "false"}) for r in skills],
        "dim_occupation": [], "dim_industry": [], "dim_major": [],
        "dim_university": [], "dim_economic_indicator": [],
    }
    for name, rows in dimensions.items():
        write_table(name, rows, output_dir)
    return {name: len(rows) for name, rows in dimensions.items()}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mapping-dir", type=Path, default=ROOT / "configs" / "mappings")
    parser.add_argument("--silver-dir", type=Path, default=ROOT / "data" / "silver" / "jobs")
    parser.add_argument("--output-dir", type=Path, default=TABLE_DIR)
    args = parser.parse_args()
    print(build(args.mapping_dir, args.silver_dir, args.output_dir))


if __name__ == "__main__":
    main()

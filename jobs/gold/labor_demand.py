"""Create monthly job-demand aggregates from Gold facts."""
from __future__ import annotations

import argparse
from collections import defaultdict
from pathlib import Path

from _common import ROOT, TABLE_DIR, read_table, write_table


def build(input_dir: Path, output_dir: Path) -> int:
    groups = defaultdict(lambda: {"posting_count": 0, "companies": set(), "salary_sum": 0, "salary_count": 0})
    for row in read_table("fact_job_posting", input_dir):
        if not row.get("is_active", True):
            continue
        date_key = row.get("date_key")
        key = (date_key // 100 if date_key else None, row.get("location_id"), row.get("industry_id"))
        group = groups[key]
        group["posting_count"] += 1
        if row.get("company_id"):
            group["companies"].add(row["company_id"])
        if row.get("salary_min") is not None:
            group["salary_sum"] += float(row["salary_min"])
            group["salary_count"] += 1
    rows = [dict(year_month=month, location_id=location, industry_id=industry,
        posting_count=data["posting_count"], hiring_company_count=len(data["companies"]),
        average_salary_min=(data["salary_sum"] / data["salary_count"] if data["salary_count"] else None))
        for (month, location, industry), data in sorted(groups.items(), key=lambda x: str(x[0]))]
    write_table("agg_labor_demand", rows, output_dir)
    return len(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, default=TABLE_DIR)
    parser.add_argument("--output-dir", type=Path, default=TABLE_DIR)
    args = parser.parse_args()
    print(f"Wrote {build(args.input_dir, args.output_dir)} labor-demand rows")


if __name__ == "__main__":
    main()

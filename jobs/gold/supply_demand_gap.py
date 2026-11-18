"""Compare monthly posting demand with reported unemployed labor supply."""
from __future__ import annotations

import argparse
from collections import defaultdict
from pathlib import Path

from _common import TABLE_DIR, read_table, write_table


def build(input_dir: Path, output_dir: Path) -> int:
    supply = {}
    for row in read_table("agg_labor_supply", input_dir):
        key = (row.get("year_month"), row.get("location_id"))
        if None not in key and row.get("unemployed_count") is not None:
            supply[key] = int(row["unemployed_count"])

    demand = defaultdict(int)
    for row in read_table("agg_labor_demand", input_dir):
        key = (row.get("year_month"), row.get("location_id"))
        if None not in key:
            demand[key] += int(row["posting_count"])

    rows = []
    for (month, location), postings in sorted(demand.items()):
        available = supply.get((month, location))
        rows.append(dict(year_month=month, location_id=location, industry_id=None,
            posting_count=postings, unemployed_count=available,
            posting_minus_unemployed=(postings - available if available is not None else None)))
    write_table("agg_supply_demand_gap", rows, output_dir)
    return len(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, default=TABLE_DIR)
    parser.add_argument("--output-dir", type=Path, default=TABLE_DIR)
    args = parser.parse_args()
    print(f"Wrote {build(args.input_dir, args.output_dir)} supply-demand-gap rows")


if __name__ == "__main__":
    main()

"""Build labor-supply aggregates from labour-market Gold facts."""
from __future__ import annotations

import argparse
from pathlib import Path

from _common import TABLE_DIR, read_table, write_table


def build(input_dir: Path, output_dir: Path) -> int:
    locations = {r["location_id"]: r["location_name"] for r in read_table("dim_location", input_dir)}
    rows = []
    for fact in read_table("fact_labour_market", input_dir):
        period_start = fact.get("period_start")
        rows.append(dict(year_month=(int(period_start[:4] + period_start[5:7]) if period_start else None),
            period_start=period_start, period_end=fact.get("period_end"),
            period_label=fact.get("period_label"), location_id=fact.get("location_id"),
            location_name=locations.get(fact.get("location_id")),
            labor_force_count=fact.get("labor_force_count"), employed_count=fact.get("employed_count"),
            unemployed_count=fact.get("unemployed_count"), unemployment_rate=fact.get("unemployment_rate")))
    write_table("agg_labor_supply", rows, output_dir)
    return len(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, default=TABLE_DIR)
    parser.add_argument("--output-dir", type=Path, default=TABLE_DIR)
    args = parser.parse_args()
    print(f"Wrote {build(args.input_dir, args.output_dir)} labor-supply rows")


if __name__ == "__main__":
    main()

"""Aggregate active job postings by normalized skill."""
from __future__ import annotations

import argparse
from collections import defaultdict
from pathlib import Path

from _common import ROOT, TABLE_DIR, read_table, write_table


def build(input_dir: Path, output_dir: Path) -> int:
    skills = {r["skill_id"]: r for r in read_table("dim_skill", input_dir)}
    jobs = {r["job_id"]: r for r in read_table("fact_job_posting", input_dir)}
    groups = defaultdict(lambda: {"jobs": set(), "companies": set(), "required": 0})
    for mention in read_table("fact_job_skill", input_dir):
        job = jobs.get(mention["job_id"])
        if not job or not job.get("is_active", True):
            continue
        group = groups[mention["skill_id"]]
        group["jobs"].add(job["job_id"])
        if job.get("company_id"):
            group["companies"].add(job["company_id"])
        group["required"] += int(mention.get("is_required") is True)
    rows = []
    for skill_id, data in sorted(groups.items()):
        dimension = skills.get(skill_id, {})
        rows.append(dict(skill_id=skill_id, skill_name=dimension.get("skill_name", skill_id),
            skill_category=dimension.get("skill_category"), posting_count=len(data["jobs"]),
            hiring_company_count=len(data["companies"]), required_mention_count=data["required"]))
    write_table("agg_skill_demand", rows, output_dir)
    return len(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, default=TABLE_DIR)
    parser.add_argument("--output-dir", type=Path, default=TABLE_DIR)
    args = parser.parse_args()
    print(f"Wrote {build(args.input_dir, args.output_dir)} skill-demand rows")


if __name__ == "__main__":
    main()

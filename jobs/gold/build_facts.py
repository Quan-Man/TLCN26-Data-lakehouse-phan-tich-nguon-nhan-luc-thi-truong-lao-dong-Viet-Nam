"""Build Gold job-posting and job-skill facts from the latest Silver snapshot."""
from __future__ import annotations

import argparse
from pathlib import Path

from _common import ROOT, TABLE_DIR, read_silver_snapshot, write_table, int_date


def build(silver_dir: Path, output_dir: Path) -> dict[str, int]:
    _, tables = read_silver_snapshot(silver_dir)
    jobs = tables["job_postings"]
    locations = {}
    for row in tables["job_locations"]:
        if row.get("is_primary"):
            locations[row["job_id"]] = row["location_id"]

    facts = []
    job_key = {}
    for job in jobs:
        if job["job_id"] in job_key:
            raise ValueError(f"Duplicate Silver job_id: {job['job_id']}")
        job_key[job["job_id"]] = job["job_id"]
        facts.append(dict(job_id=job["job_id"], date_key=int_date(job.get("posted_date")),
            company_id=job.get("company_id"), occupation_id=job.get("occupation_id"),
            industry_id=None, location_id=locations.get(job["job_id"]),
            source_name=job.get("source_name"), job_title=job["job_title"],
            job_type=job.get("employment_type"), experience_level=job.get("experience_type"),
            education_level=job.get("education_level"), salary_min=job.get("salary_min_vnd"),
            salary_max=job.get("salary_max_vnd"), salary_currency=job.get("salary_currency"),
            salary_period=job.get("salary_type"),
            is_salary_disclosed=job.get("salary_min_vnd") is not None or job.get("salary_max_vnd") is not None,
            posting_count=1, is_active=job.get("record_status") != "EXPIRED",
            crawled_at=job.get("crawled_at"), valid_through=job.get("expiration_date")))

    skill_facts = []
    for row in tables["job_skills"]:
        if row["job_id"] not in job_key:
            raise ValueError(f"Skill row references missing job: {row['job_id']}")
        skill_facts.append(dict(job_id=row["job_id"], skill_id=row["skill_id"],
            evidence_field="requirements_raw", matched_text=row.get("raw_skill_text") or row["skill_id"],
            raw_skill_text=row.get("raw_skill_text"), is_required=row.get("is_required"),
            required_level=row.get("required_level"), years_required=row.get("years_required"),
            extraction_method=row.get("extraction_method") or "SILVER",
            confidence_score=row.get("confidence_score"), validation_status="PENDING",
            extractor_version="silver-jobs-v1"))

    write_table("fact_job_posting", facts, output_dir)
    write_table("fact_job_skill", skill_facts, output_dir)
    for name in ("fact_admission", "fact_economic_indicator", "fact_labour_market", "fact_enterprise"):
        write_table(name, [], output_dir)
    return {"fact_job_posting": len(facts), "fact_job_skill": len(skill_facts)}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--silver-dir", type=Path, default=ROOT / "data" / "silver" / "jobs")
    parser.add_argument("--output-dir", type=Path, default=TABLE_DIR)
    args = parser.parse_args()
    print(build(args.silver_dir, args.output_dir))


if __name__ == "__main__":
    main()



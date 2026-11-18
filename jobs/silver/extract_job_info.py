#!/usr/bin/env python3
"""Extract skill mentions from requirements and descriptions in Bronze jobs."""
from __future__ import annotations

import argparse
import json
import logging
import tempfile
from pathlib import Path

from process_job_silver import (
    SkillNormalizer,
    download_bronze,
    extract_job_info,
    identify,
    read_bronze,
)


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_BRONZE = ROOT / "data" / "bronze" / "job_posting"
DEFAULT_MAPPING = ROOT / "configs" / "mappings"
DEFAULT_OUTPUT = ROOT / "data" / "silver" / "job_skills.jsonl"
LOGGER = logging.getLogger("silver.extract_job_info")


def extract(records: list[dict], normalizer: SkillNormalizer) -> list[dict]:
    extracted = []
    for envelope in records:
        job_id, _, _, _, _ = identify(envelope)
        extracted.extend(extract_job_info(job_id, envelope["raw_payload"], normalizer))
    return extracted


def write_jsonl(rows: list[dict], output: Path) -> None:
    output = output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(output.name + ".tmp")
    try:
        with temporary.open("w", encoding="utf-8", newline="\n") as stream:
            for row in rows:
                stream.write(json.dumps(row, ensure_ascii=False, allow_nan=False) + "\n")
        temporary.replace(output)
    finally:
        temporary.unlink(missing_ok=True)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Find skill aliases in Bronze job requirements and descriptions."
    )
    parser.add_argument("--input-dir", type=Path, default=DEFAULT_BRONZE,
                        help="Local Bronze job_posting directory")
    parser.add_argument("--bronze-uri", help="MinIO URI, for example s3://bronze/job_posting")
    parser.add_argument("--mapping-dir", type=Path, default=DEFAULT_MAPPING,
                        help="Directory containing skill_dictionary.csv and skill_aliases.csv")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT,
                        help="Output JSONL path; one row per job and matched skill")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    normalizer = SkillNormalizer(
        args.mapping_dir / "skill_dictionary.csv",
        args.mapping_dir / "skill_aliases.csv",
    )
    if args.bronze_uri:
        with tempfile.TemporaryDirectory(prefix="bronze_job_info_") as temporary:
            bronze_dir = Path(temporary)
            download_bronze(args.bronze_uri, bronze_dir)
            records, _ = read_bronze(bronze_dir)
    else:
        records, _ = read_bronze(args.input_dir)

    rows = extract(records, normalizer)
    write_jsonl(rows, args.output)
    LOGGER.info("Read %d Bronze jobs and wrote %d job-skill rows to %s",
                len(records), len(rows), args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Verify a completed local snapshot and all its SHA256 checksums."""
import argparse
import json
from pathlib import Path
import pandas as pd
from simple_pipeline import check, digest, validate


def verify(root):
    m = json.loads((root / 'manifest.json').read_text(encoding='utf-8'))
    check(m['status'] in ['SUCCESS', 'SUCCESS_LOCAL_ONLY'], f"Run status: {m['status']}")
    check(bool(m['files']), 'Manifest không có file')
    for f in m['files']:
        path = root / f['path']
        check(path.is_file(), f"Thiếu file: {f['path']}")
        check(digest(path) == f['sha256'], f"Checksum sai: {f['path']}")
    silver = {n: pd.read_parquet(root / 'silver' / f'{n}.parquet') for n in ['admissions', 'jobs', 'labour']}
    gold = {n: pd.read_parquet(root / 'gold' / f'{n}.parquet') for n in ['admission_summary', 'job_demand_summary', 'labour_yearly']}
    validate(silver, gold)
    for name, df in silver.items():
        c = m['counts'][name]
        check(len(df) == c['silver_rows'], f'Count mismatch: {name}')
        check(c['input_rows'] == c['silver_rows'] + c['rejected_rows'] + c['duplicate_rows_removed'], f'Row reconciliation mismatch: {name}')
    for name, df in gold.items():
        check(len(df) == m['counts'][name]['gold_rows'], f'Count mismatch: {name}')
    print(f"PASS {root.name}; local files verified; minio_verified_at_run={m['minio_verified']}")


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--run-dir', type=Path)
    p.add_argument('--output-dir', type=Path, default=Path('/app/output'))
    a = p.parse_args()
    if a.run_dir:
        root = a.run_dir
    else:
        manifests = list(a.output_dir.glob('*/manifest.json'))
        check(bool(manifests), f'Không có manifest trong {a.output_dir}')
        root = max(manifests, key=lambda f: f.stat().st_mtime_ns).parent
    verify(root)

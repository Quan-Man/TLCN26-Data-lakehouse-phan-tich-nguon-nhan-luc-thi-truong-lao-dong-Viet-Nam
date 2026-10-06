"""CSV/JSON/XLSX -> immutable Bronze snapshot -> Silver/Gold Parquet.
Run: python jobs/simple_pipeline.py --data-dir data --output-dir output --local-only
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import logging
import os
from pathlib import Path
import re
import shutil
import uuid

import pandas as pd

LOG = logging.getLogger('simple_pipeline')
ADMISSION_COLUMNS = dict(zip(
    ['Mã trường', 'Trường', 'Loại trường', 'Tỉnh/TP', 'Mã ngành', 'Ngành',
     'Trình độ', 'Tên chương trình', 'Loại CT', 'Ngôn ngữ', 'Số năm', 'Năm TS',
     'Chỉ tiêu', 'Phương thức', 'Học phí', 'Mã nhóm', 'Nhóm ngành'],
    ['university_code', 'university_name', 'university_type', 'location',
     'major_code', 'major_name', 'education_level', 'program_name', 'program_type',
     'language', 'duration_years', 'admission_year', 'quota', 'admission_method',
     'tuition_fee_raw', 'group_code', 'group_name']))
LOCATION_ALIASES = {
    'tp. hcm': 'TP. Hồ Chí Minh', 'tp.hcm': 'TP. Hồ Chí Minh',
    'hồ chí minh': 'TP. Hồ Chí Minh', 'tp. hồ chí minh': 'TP. Hồ Chí Minh',
    'cả nước': 'Cả nước',
}


def clean(value):
    if value is None or (not isinstance(value, (list, dict)) and pd.isna(value)):
        return None
    if isinstance(value, (list, dict)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    value = re.sub(r'\s+', ' ', str(value)).strip()
    return value or None


def location(value):
    value = clean(value)
    return LOCATION_ALIASES.get(value.casefold(), value) if value else None


def require(frame, columns, source):
    missing = sorted(set(columns) - set(frame.columns))
    if missing:
        raise ValueError(f'{source}: thiếu cột {missing}')


def split_invalid(frame, invalid, reason):
    bad = frame.loc[invalid].copy()
    bad['reject_reason'] = reason
    return frame.loc[~invalid].copy(), bad


def admissions(paths):
    parts = []
    for path in paths:
        d = pd.read_csv(path, dtype='string', encoding='utf-8-sig')
        d.columns = [c.strip() for c in d.columns]
        require(d, ADMISSION_COLUMNS, path.name)
        d = d.rename(columns=ADMISSION_COLUMNS)
        for col in d.columns:
            d[col] = d[col].map(clean)
        d['source_file'] = path.name
        d['quota_raw'] = d.quota
        for col in ['quota', 'admission_year', 'duration_years']:
            d[col] = pd.to_numeric(d[col], errors='coerce')
        d['location'] = d.location.map(location)
        invalid = (d.university_code.isna() | d.major_code.isna() |
                   d.admission_year.isna() | d.admission_year.mod(1).ne(0) |
                   ~d.admission_year.between(1900, 2100) |
                   (d.quota_raw.notna() & d.quota.isna()) |
                   (d.quota.notna() & (d.quota.lt(0) | d.quota.mod(1).ne(0))))
        parts.append((d, invalid))
    frame = pd.concat([p[0] for p in parts], ignore_index=True)
    invalid = pd.concat([p[1] for p in parts], ignore_index=True)
    good, bad = split_invalid(frame, invalid, 'invalid_required_key_year_or_quota')
    # Exact duplicate content only: do not collapse different admission programs.
    good = good.drop_duplicates(subset=[c for c in good if c != 'source_file'])
    good['admission_year'] = good.admission_year.astype('int64')
    good['quota'] = good.quota.astype('Int64')
    return good.reset_index(drop=True), bad, len(frame)


def jobs(paths):
    rows = []
    for path in paths:
        data = json.loads(path.read_text(encoding='utf-8-sig'))
        if not isinstance(data, list) or not all(isinstance(r, dict) for r in data):
            raise ValueError(f'{path.name}: JSON phải là mảng các object')
        for row in data:
            r = {k: clean(v) for k, v in row.items()}
            r['source_file'] = path.name
            # Preserve the full original location; multi-location ads remain one ad.
            r['location'] = location(row.get('location'))
            rows.append(r)
    d = pd.DataFrame(rows)
    require(d, ['job_id', 'job_title', 'source', 'location', 'crawled_at'], 'jobs')
    d['observed_at'] = pd.to_datetime(d.crawled_at, errors='coerce', utc=True)
    invalid = d[['job_id', 'job_title', 'source']].isna().any(axis=1) | d.observed_at.isna()
    good, bad = split_invalid(d, invalid, 'missing_id_title_source_or_invalid_crawled_at')
    # Same source + id => keep the latest observed record, deterministically.
    good = good.sort_values(['observed_at', 'source_file'], kind='stable')
    good = good.drop_duplicates(['source', 'job_id'], keep='last')
    good['observed_month'] = good.observed_at.dt.tz_convert('Asia/Ho_Chi_Minh').dt.strftime('%Y-%m')
    good['location'] = good.location.fillna('Chưa rõ')
    if 'category_name' not in good:
        good['category_name'] = 'Chưa rõ'
    good['category_name'] = good.category_name.fillna('Chưa rõ')
    return good.reset_index(drop=True), bad, len(d)


def labour(paths, unit):
    rows = []
    for path in paths:
        x = pd.read_excel(path, sheet_name='V02.37', header=None)
        if 'Lực lượng lao động' not in str(x.iloc[0, 0]):
            raise ValueError(f'{path.name}: không phải bảng lực lượng lao động V02.37')
        candidates = []
        for index, row in x.iterrows():
            nums = pd.to_numeric(row.iloc[1:], errors='coerce')
            if nums.notna().all() and nums.between(1900, 2100).all() and nums.mod(1).eq(0).all():
                candidates.append(index)
        if len(candidates) != 1:
            raise ValueError(f'{path.name}: không xác định được một hàng tiêu đề năm')
        header = candidates[0]
        years = [int(v) for v in x.iloc[header, 1:]]
        if len(set(years)) != len(years):
            raise ValueError(f'{path.name}: năm bị trùng')
        for _, row in x.iloc[header + 1:].iterrows():
            if row.isna().all():
                continue
            for col, year in enumerate(years, 1):
                rows.append({'location': location(row.iloc[0]), 'year': year,
                             'indicator': 'labour_force_15_plus', 'value_raw': clean(row.iloc[col]),
                             'unit': unit, 'source_file': path.name, 'source_sheet': 'V02.37'})
    d = pd.DataFrame(rows)
    d['value'] = pd.to_numeric(d.value_raw, errors='coerce')
    invalid = d.location.isna() | d.value.isna() | d.value.lt(0)
    good, bad = split_invalid(d, invalid, 'missing_location_or_invalid_nonnegative_value')
    good = good.drop_duplicates(subset=[c for c in good if c != 'source_file'])
    key = ['location', 'year', 'indicator']
    if good.duplicated(key).any():
        raise ValueError('Nhiều giá trị khác nhau cho cùng địa phương/năm/chỉ tiêu lao động')
    return good.reset_index(drop=True), bad, len(d)


def make_gold(a, j, l):
    keys = ['admission_year', 'university_code', 'university_name', 'major_code', 'major_name']
    ag = a.groupby(keys, dropna=False).agg(
        program_rows=('major_code', 'size'), quota_known_sum=('quota', lambda s: s.sum(min_count=1)),
        quota_missing_rows=('quota', lambda s: int(s.isna().sum()))).reset_index()
    jg = j.groupby(['observed_month', 'location', 'category_name'], dropna=False).size().reset_index(name='job_posting_count')
    lg = l.sort_values(['location', 'indicator', 'year']).copy()
    group = lg.groupby(['location', 'indicator', 'unit'], dropna=False)
    previous = group.value.shift()
    previous_year = group.year.shift()
    lg['previous_year_value'] = previous.where(lg.year.sub(previous_year).eq(1))
    lg['yoy_pct'] = ((lg.value / lg.previous_year_value - 1) * 100).where(lg.previous_year_value.gt(0))
    lg['geographic_level'] = lg.location.map(lambda s: 'national' if s == 'Cả nước' else 'province')
    return {'admission_summary': ag, 'job_demand_summary': jg, 'labour_yearly': lg}


def check(condition, message):
    if not condition:
        raise ValueError(message)


def validate(silver, gold):
    for name, frame in silver.items():
        check(len(frame) > 0, f'Silver {name} rỗng')
    a, j, l = silver['admissions'], silver['jobs'], silver['labour']
    check(not j.duplicated(['source', 'job_id']).any(), 'Job key bị trùng')
    check(not l.duplicated(['location', 'year', 'indicator']).any(), 'Labour key bị trùng')
    check(int(gold['job_demand_summary'].job_posting_count.sum()) == len(j), 'Sai tổng số tin tuyển dụng')
    check(int(gold['admission_summary'].program_rows.sum()) == len(a), 'Sai tổng dòng chương trình')
    check(int(gold['admission_summary'].quota_missing_rows.sum()) == int(a.quota.isna().sum()), 'Sai số dòng thiếu chỉ tiêu')
    check(gold['admission_summary'].quota_known_sum.sum() == a.quota.sum(), 'Sai tổng chỉ tiêu đã biết')
    check(len(gold['labour_yearly']) == len(l), 'Sai số dòng lao động')


def digest(path):
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def publish(root, manifest):
    import boto3
    from botocore.config import Config
    s3 = boto3.client('s3', endpoint_url=os.environ.get('MINIO_ENDPOINT', 'http://minio:9000'),
        aws_access_key_id=os.environ['MINIO_ROOT_USER'], aws_secret_access_key=os.environ['MINIO_ROOT_PASSWORD'],
        region_name='us-east-1', config=Config(signature_version='s3v4', s3={'addressing_style': 'path'},
                                            connect_timeout=10, read_timeout=60, retries={'max_attempts': 3}))
    for bucket in ['bronze', 'silver', 'gold']:
        s3.head_bucket(Bucket=bucket)  # Existing stack's minio-init owns bucket creation.
    for item in manifest['files']:
        p = root / item['path']
        bucket, rel = item['path'].split('/', 1)
        key = f"simple_pipeline/{root.name}/{rel}"
        s3.upload_file(str(p), bucket, key)
        # Read-back hash verifies uploaded bytes, not only request success.
        response = s3.get_object(Bucket=bucket, Key=key)
        h = hashlib.sha256()
        with response['Body'] as body:
            for chunk in iter(lambda: body.read(1024 * 1024), b''):
                h.update(chunk)
        check(h.hexdigest() == item['sha256'], f'Sai checksum MinIO: {bucket}/{key}')
    manifest['minio_verified'] = True
    manifest['status'] = 'SUCCESS'
    payload = json.dumps(manifest, ensure_ascii=False, indent=2).encode()
    # Completion marker only exists after all uploads and read-back checks pass.
    s3.put_object(Bucket='gold', Key=f'simple_pipeline/{root.name}/manifest.json', Body=payload, ContentType='application/json')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--data-dir', type=Path, default=Path('/app/data'))
    parser.add_argument('--output-dir', type=Path, default=Path('/app/output'))
    parser.add_argument('--local-only', action='store_true')
    parser.add_argument('--labour-unit', default='unknown', help='Chỉ đổi khi đã xác minh đơn vị từ nguồn')
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format='%(levelname)s %(message)s')
    run_id = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '_' + uuid.uuid4().hex[:8]
    root = args.output_dir / run_id
    root.mkdir(parents=True, exist_ok=False)
    manifest = {'run_id': run_id, 'status': 'RUNNING', 'minio_verified': False, 'counts': {}, 'warnings': [], 'files': []}
    try:
        patterns = {'admissions': 'education/admission_2024.csv', 'jobs': 'jobs/timviec365_all_jobs_20260925_155847.json',
                    'labour': 'economy/6_luc_luong_lao_dong.xlsx'}
        snapshots = {}
        for name, pattern in patterns.items():
            inputs = sorted(args.data_dir.glob(pattern))
            check(bool(inputs), f'Không tìm thấy file: {args.data_dir / pattern}')
            snapshots[name] = []
            for src in inputs:
                target = root / 'bronze' / name / src.name
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(src, target)
                check(digest(src) == digest(target), f'Nguồn thay đổi khi sao chép: {src}')
                snapshots[name].append(target)
        # All transformations read the Bronze snapshot, not mutable input files.
        processed = {'admissions': admissions(snapshots['admissions']),
                     'jobs': jobs(snapshots['jobs']),
                     'labour': labour(snapshots['labour'], args.labour_unit)}
        silver = {k: v[0] for k, v in processed.items()}
        rejected = 0
        for name, (good, bad, count) in processed.items():
            rejected += len(bad)
            manifest['counts'][name] = {'input_rows': count, 'silver_rows': len(good),
                                       'rejected_rows': len(bad), 'duplicate_rows_removed': count - len(good) - len(bad)}
            if len(bad):
                p = root / 'silver' / 'rejected' / f'{name}.json'
                p.parent.mkdir(parents=True, exist_ok=True)
                bad.to_json(p, orient='records', force_ascii=False, indent=2, date_format='iso')
        check(rejected == 0, f'Có {rejected} dòng không hợp lệ; xem silver/rejected, sửa dữ liệu rồi chạy lại')
        gold = make_gold(silver['admissions'], silver['jobs'], silver['labour'])
        validate(silver, gold)
        for layer, tables in [('silver', silver), ('gold', gold)]:
            for name, frame in tables.items():
                dest = root / layer / f'{name}.parquet'
                dest.parent.mkdir(parents=True, exist_ok=True)
                frame.to_parquet(dest, engine='pyarrow', index=False)
                pd.testing.assert_frame_equal(frame.reset_index(drop=True), pd.read_parquet(dest), check_dtype=False)
                LOG.info('%s/%s: %s rows', layer, name, len(frame))
                if layer == 'gold':
                    frame.to_csv(dest.with_suffix('.csv'), index=False, encoding='utf-8-sig')
                    manifest['counts'][name] = {'gold_rows': len(frame)}
        missing = int(silver['admissions'].quota.isna().sum())
        if missing:
            manifest['warnings'].append(f'{missing} dòng tuyển sinh thiếu chỉ tiêu: giữ NULL, tổng Gold chỉ gồm giá trị đã biết')
        if args.labour_unit == 'unknown':
            manifest['warnings'].append('Excel không ghi đơn vị trong bảng: giữ unit=unknown; chưa quy đổi sang người')
        manifest['warnings'].append('Tháng tuyển dụng là tháng crawl (observed_month), không phải tháng đăng tin')
        manifest['warnings'].append('Địa phương giữ theo nguồn lịch sử; chưa ánh xạ thay đổi địa giới')
        for p in sorted(root.rglob('*')):
            if p.is_file():
                manifest['files'].append({'path': p.relative_to(root).as_posix(), 'bytes': p.stat().st_size, 'sha256': digest(p)})
        if args.local_only:
            manifest['status'] = 'SUCCESS_LOCAL_ONLY'
        else:
            publish(root, manifest)
    except Exception as exc:
        manifest['status'] = 'FAIL'
        manifest['error'] = str(exc)
        raise
    finally:
        (root / 'manifest.json').write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding='utf-8')
        LOG.info('Manifest: %s (%s)', root / 'manifest.json', manifest['status'])
    print(json.dumps(manifest, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()

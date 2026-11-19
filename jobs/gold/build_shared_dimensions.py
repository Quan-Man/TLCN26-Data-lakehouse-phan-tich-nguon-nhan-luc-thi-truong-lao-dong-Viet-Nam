from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
import unicodedata
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable, Mapping
from urllib.parse import urlparse

LOG = logging.getLogger(__name__)
OPEN_END = date(9999, 12, 31)
UNKNOWN_START = date(1900, 1, 1)
UNKNOWN_ID = '__UNKNOWN__'
UNKNOWN_KEY = '0'


@dataclass(frozen=True)
class DimensionSpec:
    name: str
    id_column: str
    key_column: str
    name_column: str
    attributes: tuple[str, ...]
    filename: str


SPECS = {
    'location': DimensionSpec('location', 'location_id', 'location_key', 'province_name',
        ('location_code', 'province_name', 'economic_region', 'location_name_normalized', 'country_code'),
        'location_dictionary.csv'),
    # 'occupation': DimensionSpec('occupation', 'occupation_id', 'occupation_key',
    #     'occupation_name', ('occupation_code', 'occupation_name',
    #     'occupation_group', 'field_group_name'), 'occupation_mapping.csv'),
    # 'industry': DimensionSpec('industry', 'industry_id', 'industry_key',
    #     'industry_name', ('industry_code', 'industry_name', 'industry_group',
    #     'industry_section', 'industry_level', 'parent_industry_code'),
    #     'industry_mapping.csv'),
    'skill': DimensionSpec('skill', 'skill_id', 'skill_key', 'canonical_name',
        ( 'canonical_name', 'normalized_name', 'skill_type', 'skill_category', 'cluster_id', 'validation_status'),
        'skill_dictionary.csv'),
}
# Registry includes job skill for existing lookup consumers; only location is built here.
SHARED_SPECS = {'location': SPECS['location']}
SCD_COLUMNS = ('valid_from', 'valid_to', 'is_current', 'effective_date_basis')


def clean_text(value: Any) -> str | None:
    """NFC/whitespace only: do not remove accents or punctuation (C++/C#)."""
    if value is None:
        return None
    text = re.sub(r'\s+', ' ', unicodedata.normalize('NFC', str(value))).strip()
    return text or None


def parse_date(value: Any, field: str) -> date | None:
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value).strip())
    except ValueError as exc:
        raise ValueError(f'{field}: expected ISO YYYY-MM-DD, got {value!r}') from exc


def parse_boolean(value: Any) -> bool | None:
    if value is None or value == '':
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in ('true', '1'):
        return True
    if text in ('false', '0'):
        return False
    raise ValueError(f'is_current: invalid boolean {value!r}')


def version_key(spec: DimensionSpec, business_id: str, valid_from: date) -> str:
    payload = json.dumps([spec.name, business_id, valid_from.isoformat()],
                         ensure_ascii=False, separators=(',', ':'))
    return hashlib.sha256(payload.encode('utf-8')).hexdigest()


REGIONS = ('Đồng bằng sông Hồng', 'Trung du và miền núi phía Bắc',
           'Bắc Trung Bộ và duyên hải miền Trung', 'Tây Nguyên',
           'Đông Nam Bộ', 'Đồng bằng sông Cửu Long')


def location_normalize(value):
    text = clean_text(value) or ''
    return ''.join(c for c in unicodedata.normalize('NFD', text.lower().replace('đ', 'd'))
                   if not unicodedata.combining(c))


def location_row(raw):
    identity = clean_text(raw.get('location_id'))
    name = clean_text(raw.get('province_name') or raw.get('location_name'))
    level = (clean_text(raw.get('location_level') or raw.get('location_type')) or 'PROVINCE').upper()
    region = clean_text(raw.get('economic_region'))
    if identity == 'COUNTRY_VN':
        return dict(location_key=hashlib.sha256(b'S:10:COUNTRY_VN').hexdigest(), location_id=identity, location_code='VN', province_name='Cả nước', economic_region=None, location_name_normalized='ca nuoc', country_code='VN')
    if not identity or identity == UNKNOWN_ID or not name or level not in {'PROVINCE', 'MUNICIPALITY', 'CITY', 'CAPITAL'}:
        raise ValueError(f'Invalid province: {raw}')
    if region not in REGIONS:
        raise ValueError(f'Missing/invalid economic_region: {identity}')
    return dict(location_key=hashlib.sha256(f'S:{len(identity)}:{identity}'.encode()).hexdigest(),
                location_id=identity, location_code=clean_text(raw.get('location_code') or raw.get('province_code')),
                province_name=name, economic_region=region,
                location_name_normalized=location_normalize(name), country_code='VN')


def validate_locations(rows, spec):
    ids, keys = set(), set()
    for row in rows:
        if set(row) != {spec.key_column, spec.id_column, *spec.attributes}:
            raise ValueError('Invalid location schema')
        identity, key = row['location_id'], row['location_key']
        if not identity or not key or identity in ids or key in keys:
            raise ValueError('Duplicate/empty location ID or key')
        if identity == UNKNOWN_ID:
            if row != unknown_member(spec): raise ValueError('Invalid unknown location')
        else:
            expected = location_row(row)
            if any(row[c] != expected[c] for c in spec.attributes) or key == UNKNOWN_KEY:
                raise ValueError(f'Invalid province attributes/key: {identity}')
        ids.add(identity); keys.add(key)
    if UNKNOWN_ID not in ids: raise ValueError('Missing unknown location')


def reconcile_locations(source, previous, spec):
    if previous: validate_locations(previous, spec)
    old = {r['location_id']: r for r in previous or []}
    result = {**old, UNKNOWN_ID: unknown_member(spec)}
    incoming = {}
    for raw in source:
        row = location_row(raw)
        identity = row['location_id']
        if identity in old: row['location_key'] = old[identity]['location_key']
        if identity in incoming and incoming[identity] != row:
            raise ValueError(f'Conflicting province: {identity}')
        incoming[identity] = row
        result[identity] = row
    if len(result) == 1: raise ValueError('Empty province source')
    rows = list(result.values())
    validate_locations(rows, spec)
    return rows


COUNTRY_REGION_NAMES = {location_normalize(x) for x in (*REGIONS, 'Việt Nam', 'Vietnam', 'VN', 'Cả nước', 'Toàn quốc', 'Trung du và miền núi Bắc Bộ')}


def aggregate_location(value):
    text = clean_text(value) or ''
    return text.upper().startswith(('REG_VN_', 'COUNTRY_')) or location_normalize(text) in COUNTRY_REGION_NAMES


def silver_locations(spark, source, aliases_path, maximum):
    from pyspark.sql import functions as F
    candidates = [location_row(r) for r in source if not aggregate_location(r.get('location_id'))
                  and not aggregate_location(r.get('province_name') or r.get('location_name'))
                  and str(r.get('location_level') or r.get('location_type') or '').upper() not in ('REGION','COUNTRY')]
    candidates.append(location_row({'location_id': 'COUNTRY_VN'}))
    by_id = {r['location_id']: r for r in candidates}
    if len(by_id) != len(candidates): raise ValueError('Duplicate location dictionary IDs')
    lookup = {}
    def add(value, identity):
        if value: lookup.setdefault(location_normalize(value), set()).add(identity)
    for r in candidates:
        add(r['province_name'], r['location_id']); add(r['location_id'], r['location_id'])
    for r in bounded_rows(spark.read.option('header',True).csv(aliases_path), maximum):
        if str(r.get('requires_context','')).lower() in ('true','1','yes'): continue
        if aggregate_location(r['location_id']): continue
        if r['location_id'] not in by_id: raise ValueError(f'Orphan location alias: {r}')
        add(r['alias'], r['location_id'])
    if 'LOC_VN63_79' in by_id:
        add('Thành phố Hồ Chí Minh','LOC_VN63_79'); add('TP. Hồ Chí Minh','LOC_VN63_79')
    paths = [('delta','s3a://silver/education/universities','location_name'),
             ('parquet','s3a://silver/jobs/current/job_locations.parquet','location_id')]
    paths += [('delta',f's3a://silver/economic/{name}_statistics','location_id') for name in ('economic','labour','enterprise')]
    selected, missing = set(), []
    for fmt,path,col in paths:
        if not hadoop_exists(spark,path):
            LOG.warning('Silver input not available: %s',path); continue
        frame = spark.read.format(fmt).load(path)
        if col not in frame.columns: raise ValueError(f'Missing {col}: {path}')
        for r in bounded_rows(frame.select(F.col(col).cast('string').alias('value')).distinct(),maximum):
            value = clean_text(r['value'])
            if value in (None, UNKNOWN_ID, UNKNOWN_KEY): continue
            if aggregate_location(value) and value != 'COUNTRY_VN':
                if '/economic/' in path: raise ValueError(f'Unsupported Economy location: {value}')
                continue
            matches = {value} if value in by_id else lookup.get(location_normalize(value),set())
            if len(matches) != 1: missing.append((path,value,sorted(matches)))
            else: selected.update(matches)
    if missing: raise ValueError(f'Unmapped/ambiguous Silver provinces: {missing[:50]}')
    if not selected: raise ValueError('No Silver provinces')
    return [by_id[k] for k in sorted(by_id)]


def unknown_member(spec: DimensionSpec) -> dict[str, Any]:
    row = {column: None for column in spec.attributes}
    if spec.name == 'location':
        return dict(row, location_id=UNKNOWN_ID, location_key=UNKNOWN_KEY, province_name='Không xác định', location_name_normalized='khong xac dinh', country_code='VN')
    row.update({spec.id_column: UNKNOWN_ID, spec.key_column: UNKNOWN_KEY,
                spec.name_column: 'Không xác định', 'valid_from': UNKNOWN_START,
                'valid_to': OPEN_END, 'is_current': True,
                'effective_date_basis': 'UNKNOWN'})
    if spec.name == 'skill':
        row.update(normalized_name='không xác định', validation_status='UNKNOWN')
    return row


def canonical_row(raw: Mapping[str, Any], spec: DimensionSpec) -> dict[str, Any]:
    # Existing Silver IDs are preserved as strings, including leading zeros.
    row = {column: clean_text(raw.get(column))
           for column in (spec.id_column, *spec.attributes)}
    if spec.name == 'skill':
        fallback = clean_text(raw.get('skill_name'))
        row['canonical_name'] = row['canonical_name'] or fallback
        value = row['normalized_name'] or row['canonical_name']
        row['normalized_name'] = value.lower() if value else None
    if not row[spec.id_column] or not row[spec.name_column]:
        raise ValueError(f'dim_{spec.name}: nonempty {spec.id_column} and '
                         f'{spec.name_column} are required')
    if row[spec.id_column] == UNKNOWN_ID:
        raise ValueError(f'{UNKNOWN_ID} is reserved for the unknown member')
    row['valid_from'] = parse_date(raw.get('valid_from'), 'valid_from')
    row['valid_to'] = parse_date(raw.get('valid_to'), 'valid_to')
    row['_source_current'] = parse_boolean(raw.get('is_current'))
    return row


def same_attributes(left: Mapping[str, Any], right: Mapping[str, Any],
                    spec: DimensionSpec) -> bool:
    return all(left.get(column) == right.get(column) for column in spec.attributes)


def validate_dimension(rows: list[dict[str, Any]], spec: DimensionSpec) -> None:
    """Validate all history, including closed versions and the unknown row."""
    if spec.name == 'location':
        return validate_locations(rows, spec)
    grouped: dict[str, list[dict[str, Any]]] = {}
    keys: set[str] = set()
    for row in rows:
        identifier, key = row.get(spec.id_column), row.get(spec.key_column)
        if not isinstance(identifier, str) or not identifier or not isinstance(key, str):
            raise ValueError(f'dim_{spec.name}: invalid ID/key type')
        if clean_text(identifier) != identifier or not clean_text(row.get(spec.name_column)):
            raise ValueError(f'dim_{spec.name}: invalid business ID or canonical name')
        if row.get('effective_date_basis') not in ('SOURCE', 'OBSERVED', 'UNKNOWN'):
            raise ValueError(f'dim_{spec.name}: invalid effective_date_basis')
        if key in keys:
            raise ValueError(f'dim_{spec.name}: duplicate/colliding key {key}')
        keys.add(key)
        start, end = row.get('valid_from'), row.get('valid_to')
        if not isinstance(start, date) or not isinstance(end, date) or start >= end:
            raise ValueError(f'dim_{spec.name}: invalid interval for {identifier}')
        if row.get('is_current') is not (end == OPEN_END):
            raise ValueError(f'dim_{spec.name}: inconsistent is_current for {identifier}')
        if identifier == UNKNOWN_ID:
            if row != unknown_member(spec):
                raise ValueError(f'dim_{spec.name}: unknown member has changed')
        elif key != version_key(spec, identifier, start):
            raise ValueError(f'dim_{spec.name}: incompatible legacy key convention; '
                             'migrate keys and dependent facts explicitly')
        grouped.setdefault(identifier, []).append(row)
    if UNKNOWN_ID not in grouped:
        raise ValueError(f'dim_{spec.name}: missing unknown member')
    for identifier, versions in grouped.items():
        ordered = sorted(versions, key=lambda r: r['valid_from'])
        if sum(r['is_current'] for r in ordered) > 1:
            raise ValueError(f'dim_{spec.name}: multiple current versions for {identifier}')
        for previous, current in zip(ordered, ordered[1:]):
            if previous['valid_to'] > current['valid_from']:
                raise ValueError(f'dim_{spec.name}: overlapping versions for {identifier}')


def reconcile_dimension(source: Iterable[Mapping[str, Any]],
                        previous: list[dict[str, Any]], spec: DimensionSpec,
                        as_of: date) -> list[dict[str, Any]]:
    """Pure, deterministic SCD2 reconciliation, also callable without Spark."""
    if spec.name == 'location':
        return reconcile_locations(source, previous, spec)
    if previous:
        validate_dimension(previous, spec)
    else:
        previous = [unknown_member(spec)]
    versions = {(r[spec.id_column], r['valid_from']): dict(r) for r in previous}
    candidates: dict[tuple[str, date | None], dict[str, Any]] = {}
    for raw in source:
        row = canonical_row(raw, spec)
        identifier = row[spec.id_column]
        token = (identifier, row['valid_from'])
        if token in candidates and candidates[token] != row:
            raise ValueError(f'dim_{spec.name}: conflicting catalog rows for {token}')
        candidates[token] = row
    if not candidates:
        raise ValueError(f'dim_{spec.name}: source catalog is empty')
    # Dates must be sortable; a snapshot and explicit history for one ID cannot mix.
    by_id: dict[str, list[dict[str, Any]]] = {}
    for row in candidates.values():
        by_id.setdefault(row[spec.id_column], []).append(row)
    for identifier, incoming in sorted(by_id.items()):
        if len(incoming) > 1 and any(r['valid_from'] is None for r in incoming):
            raise ValueError(f'{identifier}: multiple versions require explicit valid_from')
        incoming.sort(key=lambda r: r['valid_from'] or as_of)
        for row in incoming:
            existing = sorted((r for (business_id, _), r in versions.items()
                               if business_id == identifier), key=lambda r: r['valid_from'])
            latest = existing[-1] if existing else None
            start = row['valid_from']
            basis = 'SOURCE' if start is not None else 'OBSERVED'
            if start is None:
                if latest and as_of < latest['valid_from']:
                    raise ValueError(f'{identifier}: --as-of predates existing history')
                if latest and latest['is_current'] and same_attributes(latest, row, spec):
                    if row['valid_to'] not in (None, latest['valid_to']):
                        raise ValueError(f'{identifier}: closing an existing version '
                                         'requires an explicit versioned catalog')
                    if row['_source_current'] not in (None, True):
                        raise ValueError(f'{identifier}: is_current conflicts with history')
                    continue
                start = as_of
            if start > as_of or start >= OPEN_END:
                raise ValueError(f'{identifier}: future valid_from; advance --as-of explicitly')
            token = (identifier, start)
            if token in versions:
                old = versions[token]
                if not same_attributes(old, row, spec):
                    raise ValueError(f'{identifier}: attributes changed at the same '
                                     'valid_from; supply a later effective date')
                if row['valid_to'] not in (None, old['valid_to']):
                    raise ValueError(f'{identifier}: rewriting existing valid_to is not allowed')
                if row['_source_current'] not in (None, old['is_current']):
                    raise ValueError(f'{identifier}: is_current conflicts with history')
                continue
            if latest:
                if start <= latest['valid_from']:
                    raise ValueError(f'{identifier}: backdated insertion needs an explicit migration')
                if latest['valid_to'] != OPEN_END and latest['valid_to'] > start:
                    raise ValueError(f'{identifier}: new version overlaps closed history')
                if latest['valid_to'] == OPEN_END:
                    latest['valid_to'] = start
                    latest['is_current'] = False
            end = row['valid_to'] or OPEN_END
            new = {column: row[column] for column in (spec.id_column, *spec.attributes)}
            new.update({spec.key_column: version_key(spec, identifier, start),
                        'valid_from': start, 'valid_to': end,
                        'is_current': end == OPEN_END, 'effective_date_basis': basis})
            if row['_source_current'] not in (None, new['is_current']):
                # For full-history input, omitted valid_to can be derived below.
                if not (row['valid_to'] is None and row['_source_current'] is False
                        and row is not incoming[-1]):
                    raise ValueError(f'{identifier}: inconsistent source is_current')
            versions[token] = new
    result = sorted(versions.values(), key=lambda r: (r[spec.id_column], r['valid_from']))
    validate_dimension(result, spec)
    return result


DATE_FIELDS = ('date_key', 'full_date', 'day_number', 'day_name', 'week_number',
               'iso_year', 'month_number', 'month_name', 'quarter_number',
               'year_number', 'year_month', 'year_quarter', 'academic_year', 'is_weekend')


def observed_date_rows(days: Iterable[date], academic_start_month: int = 9) -> list[dict[str, Any]]:
    """One row per distinct observed day, plus the reserved unknown member."""
    if not 1 <= academic_start_month <= 12:
        raise ValueError('academic_start_month must be between 1 and 12')
    days = set(days)
    if any(not isinstance(day, date) or isinstance(day, datetime) or day >= OPEN_END for day in days):
        raise ValueError('Source dates must be date values before 9999-12-31')
    if not days:
        raise ValueError('No non-null dates in the configured sources; refusing to clear dim_date')
    if UNKNOWN_START in days:
        raise ValueError('1900-01-01 is reserved for unknown date')
    if len(days) > 100001:
        raise ValueError('More than 100001 distinct source dates; use a distributed reference pipeline')
    names = ('Thứ Hai', 'Thứ Ba', 'Thứ Tư', 'Thứ Năm', 'Thứ Sáu', 'Thứ Bảy', 'Chủ Nhật')
    rows = [{column: None for column in DATE_FIELDS}]
    rows[0].update({
        'date_key': 0,
        'full_date': UNKNOWN_START,
        'day_number': 1,
        'day_name': 'Unknown',
        'week_number': 0,
        'iso_year': 1900,
        'month_number': 1,
        'month_name': 'Unknown',
        'quarter_number': 1,
        'year_number': 1900,
        'year_month': '1900-01',
        'year_quarter': '1900-Q1',
        'academic_year': '1899-1900',
        'is_weekend': False,
    })
    for cursor in sorted(days):
        iso = cursor.isocalendar()
        academic_year = cursor.year if cursor.month >= academic_start_month else cursor.year - 1
        quarter = (cursor.month - 1) // 3 + 1
        rows.append(dict(zip(DATE_FIELDS, (
            cursor.year * 10000 + cursor.month * 100 + cursor.day, cursor,
            cursor.day, names[cursor.weekday()], iso.week, iso.year, cursor.month,
            f'Tháng {cursor.month}', quarter, cursor.year, cursor.strftime('%Y-%m'),
            f'{cursor.year}-Q{quarter}', f'{academic_year}-{academic_year + 1}',
            cursor.weekday() >= 5))))
    return rows


def observed_silver_dates(spark):
    from pyspark.sql import functions as F
    jobs = os.getenv('JOBS_SILVER_ROOT', 's3a://silver/jobs').rstrip('/')
    edu = os.getenv('EDUCATION_SILVER_PREFIX', 's3a://silver/education').rstrip('/')
    econ = os.getenv('ECONOMIC_SILVER_ROOT', 's3a://silver/economic').rstrip('/')
    inputs = [('parquet', f'{jobs}/current/job_postings.parquet', ('posted_date', 'expiration_date'), False),
              ('delta', f'{edu}/admission_announcements', ('announcement_date',), False)]
    inputs += [('delta', f'{econ}/{name}_statistics', ('year',), True) for name in ('economic','labour','enterprise')]
    days = set()
    for fmt, path, columns, annual in inputs:
        if not hadoop_exists(spark, path):
            LOG.warning('Date source not available: %s', path)
            continue
        frame = spark.read.format(fmt).load(path)
        for col in columns:
            if col not in frame.columns:
                raise ValueError(f'Missing {col}: {path}')
            parsed = F.make_date(F.col(col).cast('int'), F.lit(1), F.lit(1)) if annual else F.expr(f'try_cast(`{col}` AS DATE)')
            values = frame.select(F.col(col).alias('raw'), parsed.alias('day'))
            if values.filter(F.col('raw').isNotNull() & F.col('day').isNull()).limit(1).count():
                raise ValueError(f'Invalid date in {path}: {col}')
            days.update(r['day'] for r in bounded_rows(values.select('day').where(F.col('day').isNotNull()).distinct(), 100001))
    return days


def make_spark(app_name: str = 'gold-shared-dimensions', use_s3: bool = True):
    from pyspark.sql import SparkSession
    master = os.getenv('SPARK_MASTER', 'local[2]')
    builder = (SparkSession.builder.appName(app_name)
        .master(master)
        .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
        .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
        .config('spark.sql.session.timeZone', 'UTC')
        .config('spark.sql.shuffle.partitions', os.getenv('SPARK_SHUFFLE_PARTITIONS', '4')))
    if master.startswith('local'):
        address = os.getenv('SPARK_LOCAL_IP', '127.0.0.1')
        builder = builder.config('spark.driver.bindAddress', address).config('spark.driver.host', address)
    # JARs must already be in the project image or supplied by spark-submit.
    # No runtime Maven downloads or hard-coded Hadoop/AWS JAR versions here.
    if use_s3:
        endpoint = os.getenv('MINIO_ENDPOINT', 'http://minio:9000')
        if not endpoint.startswith(('http://', 'https://')):
            raise ValueError('MINIO_ENDPOINT must include http:// or https://')
        access = os.getenv('MINIO_ROOT_USER') or os.getenv('AWS_ACCESS_KEY_ID')
        secret = os.getenv('MINIO_ROOT_PASSWORD') or os.getenv('AWS_SECRET_ACCESS_KEY')
        if not access or not secret:
            raise ValueError('Missing MinIO/AWS access key or secret key')
        builder = (builder.config('spark.hadoop.fs.s3a.endpoint', endpoint)
            .config('spark.hadoop.fs.s3a.access.key', access)
            .config('spark.hadoop.fs.s3a.secret.key', secret)
            .config('spark.hadoop.fs.s3a.path.style.access', 'true')
            .config('spark.hadoop.fs.s3a.connection.ssl.enabled', str(endpoint.startswith('https://')).lower())
            .config('spark.hadoop.fs.s3a.aws.credentials.provider',
                    'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider'))
    return builder.getOrCreate()


def hadoop_exists(spark, path: str) -> bool:
    jpath = spark._jvm.org.apache.hadoop.fs.Path(path)
    return bool(jpath.getFileSystem(spark._jsc.hadoopConfiguration()).exists(jpath))


def bounded_rows(frame, maximum: int) -> list[dict[str, Any]]:
    rows = frame.limit(maximum + 1).collect()
    if len(rows) > maximum:
        raise ValueError(f'Reference catalog exceeds {maximum} rows; use a distributed reference pipeline')
    return [row.asDict(recursive=True) for row in rows]


def dimension_schema(spec: DimensionSpec):
    from pyspark.sql.types import StructType, StructField, StringType, DateType, BooleanType
    return StructType([
        StructField(column, StringType(), True)
        for column in (spec.key_column, spec.id_column, *spec.attributes)
    ] + ([] if spec.name == 'location' else [StructField('valid_from', DateType(), False), StructField('valid_to', DateType(), False),
         StructField('is_current', BooleanType(), False),
         StructField('effective_date_basis', StringType(), False)]))


def date_schema():
    from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, BooleanType
    integers = {'date_key', 'day_number', 'week_number', 'iso_year', 'month_number',
                'quarter_number', 'year_number'}
    return StructType([StructField(column,
        DateType() if column == 'full_date' else BooleanType() if column == 'is_weekend'
        else IntegerType() if column in integers else StringType(), True)
        for column in DATE_FIELDS])


def existing_delta(spark, path: str, expected_schema):
    from delta.tables import DeltaTable
    if not hadoop_exists(spark, path):
        return None
    if not DeltaTable.isDeltaTable(spark, path):
        raise ValueError(f'Target exists but is not Delta: {path}')
    frame = spark.read.format('delta').load(path)
    expected = {field.name: field.dataType for field in expected_schema}
    actual = {field.name: field.dataType for field in frame.schema}
    if actual != expected:
        raise ValueError(f'Incompatible schema at {path}; migrate explicitly, do not overwrite')
    return frame


def read_catalog(spark, path: str, spec: DimensionSpec, source_format: str = 'auto'):
    if not hadoop_exists(spark, path):
        raise FileNotFoundError(f'Missing {spec.name} catalog: {path}')
    suffix = Path(urlparse(path).path).suffix.lower()
    fmt = source_format if source_format != 'auto' else {
        '.csv': 'csv', '.json': 'json', '.jsonl': 'json', '.parquet': 'parquet'
    }.get(suffix, 'delta')
    reader = spark.read.format(fmt)
    if fmt == 'csv':
        reader = (reader.option('header', 'true').option('encoding', 'UTF-8')
            .option('inferSchema', 'false').option('mode', 'FAILFAST')
            .option('multiLine', 'true').option('escape', '"'))
    frame = reader.load(path)
    renamed = [column.lstrip('\ufeff').strip() for column in frame.columns]
    if len(set(renamed)) != len(renamed):
        raise ValueError(f'Duplicate input headers in {path}')
    frame = frame.toDF(*renamed)
    required = {spec.id_column, spec.name_column}
    if spec.name == 'skill' and 'canonical_name' not in frame.columns and 'skill_name' in frame.columns:
        required.remove('canonical_name')
        required.add('skill_name')
    if spec.name == 'location' and 'province_name' not in frame.columns and 'location_name' in frame.columns:
        frame = frame.withColumnRenamed('location_name', 'province_name')
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f'{path}: missing required columns {sorted(missing)}')
    from pyspark.sql.types import StringType
    if not isinstance(frame.schema[spec.id_column].dataType, StringType):
        raise ValueError(f'{path}: {spec.id_column} must be string to preserve business IDs')
    selected = [column for column in (*required, *spec.attributes,
                'skill_name', 'province_code', 'location_type', 'location_level', 'valid_from', 'valid_to', 'is_current') if column in frame.columns]
    return frame.select(*dict.fromkeys(selected)).distinct()


def merge_rows(spark, path: str, rows: list[dict[str, Any]], schema,
               key_column: str, existing: list[dict[str, Any]] | None) -> bool:
    """One atomic merge, skip exact no-op reruns. Never delete absent keys."""
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F
    if existing is not None:
        before = {row[key_column]: row for row in existing}
        after = {row[key_column]: row for row in rows}
        if before == after:
            return False
    frame = spark.createDataFrame(rows, schema=schema)
    if existing is None:
        frame.write.format('delta').mode('errorifexists').save(path)
    else:
        compare = ' OR '.join(f'NOT (t.`{c}` <=> s.`{c}`)' for c in frame.columns if c != key_column)
        (DeltaTable.forPath(spark, path).alias('t')
            .merge(frame.alias('s'), F.col(f't.{key_column}') == F.col(f's.{key_column}'))
            .whenMatchedUpdateAll(condition=compare).whenNotMatchedInsertAll().execute())
    return True


def attach_dimension_key(facts, dimension, name: str, source_id_column: str | None = None,
                         event_date_column: str | None = None, output_key: str | None = None):
    """LEFT lookup: unmatched/NULL ID or date gets '0'; original rows are kept.

    Pass only dimensions produced/validated by this job. Supplying an event date
    resolves the SCD2 version; without one, the open current version is used.
    Raw IDs remain on the facts for DQ/mapping coverage reporting.
    """
    from pyspark.sql import functions as F
    spec = SPECS[name]
    source_id_column = source_id_column or spec.id_column
    output_key = output_key or spec.key_column
    if output_key in facts.columns:
        raise ValueError(f'Output key already exists: {output_key}')
    left, right = facts.alias('_fact'), dimension.alias('_dimension')
    # Qualified unresolved columns avoid Spark's ambiguous-self-join metadata
    # when the same dimension is used in more than one role in a fact lineage.
    business_id = F.col(f'_dimension.`{spec.id_column}`')
    condition = F.col(f'_fact.`{source_id_column}`').cast('string') == business_id
    if name == 'location':
        pass
    elif event_date_column:
        event = F.col(f'_fact.`{event_date_column}`').cast('date')
        condition = condition & (event >= F.col('_dimension.valid_from')) & (event < F.col('_dimension.valid_to'))
    else:
        condition = condition & F.col('_dimension.is_current')
    return left.join(right, condition, 'left').select(
        '_fact.*', F.coalesce(F.col(f'_dimension.`{spec.key_column}`'), F.lit(UNKNOWN_KEY)).alias(output_key))


def attach_date_key(facts, dim_date, source_column: str, output_key: str):
    """Use twice for posted_date_key/expiration_date_key against ONE dim_date.

    Missing/invalid dates map to integer 0. A valid date outside the calendar
    raises instead of silently creating an orphan FK: extend dim_date first.
    """
    from pyspark.sql import functions as F
    if output_key in facts.columns:
        raise ValueError(f'Output key already exists: {output_key}')
    left = facts.alias('_fact')
    right = dim_date.select(F.col('full_date').alias('_lookup_date'),
                           F.col('date_key').alias('_lookup_key')).alias('_calendar')
    parsed = F.col(f'_fact.`{source_column}`').cast('date')
    joined = left.join(right, parsed == F.col('_calendar._lookup_date'), 'left')
    if joined.filter(parsed.isNotNull() & F.col('_calendar._lookup_key').isNull()).limit(1).count():
        raise ValueError(f'{source_column}: date outside dim_date; extend the calendar')
    return joined.select('_fact.*', F.coalesce(F.col('_calendar._lookup_key'), F.lit(0)).alias(output_key))


def migrate_location_rows(rows, spec):
    """Collapse legacy SCD2 to one current province, preserving its current key."""
    selected = {}
    for raw in rows:
        identity = raw.get('location_id')
        level = (clean_text(raw.get('location_level')) or '').upper()
        if identity == UNKNOWN_ID or str(raw.get('location_key')) == UNKNOWN_KEY:
            continue
        if identity != 'COUNTRY_VN' and (level in ('REGION', 'COUNTRY') or aggregate_location(identity) or aggregate_location(raw.get('province_name') or raw.get('location_name'))):
            continue
        if 'is_current' in raw and parse_boolean(raw['is_current']) is not True:
            continue
        row = location_row(raw)
        row['location_key'] = clean_text(raw.get('location_key'))
        if not row['location_key'] or row['location_key'] == UNKNOWN_KEY:
            raise ValueError(f'Invalid legacy location key: {identity}')
        if identity in selected and selected[identity] != row:
            raise ValueError(f'Multiple current legacy rows: {identity}')
        selected[identity] = row
    result = [unknown_member(spec), *selected.values()]
    validate_locations(result, spec)
    return result


def previous_location_rows(spark,path,spec,maximum):
    if not hadoop_exists(spark,path): return None, False
    frame = spark.read.format('delta').load(path)
    expected = set(dimension_schema(spec).fieldNames())
    old = set(frame.columns)
    if old == expected:
        return bounded_rows(existing_delta(spark,path,dimension_schema(spec)),maximum), False
    required = {'location_id','location_key','location_code','province_name','economic_region','location_name','location_level'}
    allowed = expected | {'location_name','location_level','valid_from','valid_to','is_current','effective_date_basis'}
    if not required <= old or not old <= allowed:
        raise ValueError(f'Unexpected legacy location schema: {old}')
    if old & {'valid_from','valid_to'} and 'is_current' not in old:
        raise ValueError('Legacy SCD2 location has no is_current flag')
    rows = bounded_rows(frame,maximum)
    keep = migrate_location_rows(rows, spec)
    retired = {str(r['location_key']) for r in rows} - {r['location_key'] for r in keep}
    LOG.warning('Legacy dim_location migration: %d retired keys; review dependent facts before replacement. Sample: %s', len(retired), sorted(retired)[:20])
    return keep, True


def build_shared_dimensions(spark=None, *, sources: Mapping[str, str] | None = None,
                            output_root: str | None = None, as_of: date | str | None = None,
                            date_start: date | str = '2015-01-01',
                            date_end: date | str | None = None, academic_start_month: int = 9,
                            source_format: str = 'auto', max_reference_rows: int = 100000,
                            dry_run: bool = False, replace_shared: bool = False) -> dict[str, dict[str, Any]]:
    """Airflow-friendly entry point. An injected SparkSession is never stopped."""
    as_of = parse_date(as_of, 'as_of') or date.today()
    # Legacy arguments retained for callers; actual dates are determined by Silver.
    if max_reference_rows < 1:
        raise ValueError('max_reference_rows must be positive')
    output_root = (output_root or os.getenv('SHARED_GOLD_ROOT', 's3a://gold/shared')).rstrip('/')
    mapping_root = os.getenv('SHARED_MAPPING_ROOT', '/opt/airflow/configs/mappings').rstrip('/')
    sources = dict(sources or {name: f'{mapping_root}/{spec.filename}' for name, spec in SHARED_SPECS.items()})
    if set(sources) != set(SHARED_SPECS):
        raise ValueError(f'Exactly the configured catalog sources are required: {list(SHARED_SPECS)}')
    paths = [output_root, *sources.values()]
    if any(path.startswith('s3://') for path in paths):
        raise ValueError('Use s3a:// for Spark paths; s3:// is for Trino registration')
    owns_spark = spark is None
    spark = spark or make_spark(use_s3=any(path.startswith('s3a://') for path in paths))
    try:
        # Preflight every catalog and existing dimension BEFORE the first write.
        prepared: dict[str, tuple[list[dict], Any, str, list[dict] | None]] = {}
        report: dict[str, dict[str, Any]] = {}
        migrations = {}
        for name, spec in SHARED_SPECS.items():
            source = bounded_rows(read_catalog(spark, sources[name], spec, source_format), max_reference_rows)
            if name == 'location':
                source = silver_locations(spark, source, f'{mapping_root}/location_aliases.csv', max_reference_rows)
            path, schema = f'{output_root}/dim_{name}', dimension_schema(spec)
            old, migrations[name] = previous_location_rows(spark, path, spec, max_reference_rows)
            result = reconcile_dimension(source, old or [], spec, as_of)
            if len(result) > max_reference_rows:
                raise ValueError(f'dim_{name}: history exceeds max_reference_rows')
            prepared[name] = result, schema, path, old
            report[f'dim_{name}'] = {'rows': len(result), 'path': path,
                'current_members': len(result) - 1 if name == 'location' else sum(r['is_current'] for r in result) - 1}
        dates = observed_date_rows(observed_silver_dates(spark), academic_start_month)
        path, schema = f'{output_root}/dim_date', date_schema()
        old_frame = existing_delta(spark, path, schema)
        old_dates = bounded_rows(old_frame, 100002) if old_frame is not None else None
        if len({r['date_key'] for r in old_dates or []}) != len(old_dates or []):
            raise ValueError('Duplicate existing date keys')
        actual = [r['full_date'] for r in dates if r['date_key'] != 0]
        removed_dates = {r['date_key'] for r in old_dates or []} - {r['date_key'] for r in dates}
        if removed_dates and not (dry_run or replace_shared):
            raise ValueError('Date removal requires --replace-shared after checking dependent facts')
        prepared['date'] = dates, schema, path, old_dates
        report['dim_date'] = {'rows': len(dates), 'path': path,
            'start': min(actual).isoformat() if actual else None,
            'end': max(actual).isoformat() if actual else None, 'removed_keys': len(removed_dates)}
        for name, (rows, schema, path, previous) in prepared.items():
            if name != 'location':
                continue
            removed = {r['location_key'] for r in previous or []} - {r['location_key'] for r in rows}
            report['dim_location'].update(removed_province_keys=len(removed), schema_migration=migrations[name])
            if not dry_run and not replace_shared and (removed or migrations[name]):
                raise ValueError('Location replacement requires --replace-shared after reviewing dependent Gold facts')
        for name, (rows, schema, path, previous) in prepared.items():
            key = 'date_key' if name == 'date' else SPECS[name].key_column
            if dry_run:
                changed = False
            elif name == 'location':
                changed = migrations[name] or {r[key]: r for r in rows} != {r[key]: r for r in previous or []}
                if changed:
                    spark.createDataFrame(rows, schema=schema).write.format('delta').mode(
                        'overwrite' if previous is not None else 'errorifexists').option('overwriteSchema', 'true').save(path)
            else:
                changed = {r[key]: r for r in rows} != {r[key]: r for r in previous or []}
                if changed:
                    spark.createDataFrame(rows, schema=schema).write.format('delta').mode(
                        'overwrite' if previous is not None else 'errorifexists').save(path)
            report[f'dim_{name}'].update(written=changed, dry_run=dry_run)
            LOG.info('%s: %s', f'dim_{name}', json.dumps(report[f'dim_{name}'], ensure_ascii=False))
        return report
    finally:
        if owns_spark:
            spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    mapping_root = os.getenv('SHARED_MAPPING_ROOT', '/opt/airflow/configs/mappings').rstrip('/')
    for name, spec in SHARED_SPECS.items():
        parser.add_argument(f'--{name}-source', default=f'{mapping_root}/{spec.filename}')
    parser.add_argument('--output-root', default=os.getenv('SHARED_GOLD_ROOT', 's3a://gold/shared'))
    parser.add_argument('--as-of', default=date.today().isoformat())
    parser.add_argument('--date-start', default=os.getenv('SHARED_DATE_START', '2015-01-01'))
    parser.add_argument('--date-end', default=os.getenv('SHARED_DATE_END'))
    parser.add_argument('--academic-start-month', type=int, default=9)
    parser.add_argument('--source-format', choices=['auto', 'csv', 'json', 'parquet', 'delta'], default='auto')
    parser.add_argument('--max-reference-rows', type=int, default=100000)
    parser.add_argument('--dry-run', action='store_true', help='Validate all inputs/targets; do not write')
    parser.add_argument('--replace-shared', action='store_true', help='Allow location schema migration and removal of obsolete keys')
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    report = build_shared_dimensions(sources={name: getattr(args, f'{name}_source') for name in SHARED_SPECS},
        output_root=args.output_root, as_of=args.as_of, date_start=args.date_start,
        date_end=args.date_end, academic_start_month=args.academic_start_month,
        source_format=args.source_format, max_reference_rows=args.max_reference_rows,
        dry_run=args.dry_run, replace_shared=args.replace_shared)
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()

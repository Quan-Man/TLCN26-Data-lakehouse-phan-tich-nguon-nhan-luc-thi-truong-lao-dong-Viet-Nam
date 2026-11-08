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
    'location': DimensionSpec('location', 'location_id', 'location_key',
        'location_name', ('location_code', 'location_name', 'location_level',
        'province_name', 'economic_region'), 'location_mapping.csv'),
    'occupation': DimensionSpec('occupation', 'occupation_id', 'occupation_key',
        'occupation_name', ('occupation_code', 'occupation_name',
        'occupation_group', 'field_group_name'), 'occupation_mapping.csv'),
    'industry': DimensionSpec('industry', 'industry_id', 'industry_key',
        'industry_name', ('industry_code', 'industry_name', 'industry_group',
        'industry_section', 'industry_level', 'parent_industry_code'),
        'industry_mapping.csv'),
    'skill': DimensionSpec('skill', 'skill_id', 'skill_key', 'canonical_name',
        ('canonical_name', 'normalized_name', 'skill_type', 'skill_category',
        'cluster_id', 'validation_status'), 'skill_catalog.csv'),
}
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


def unknown_member(spec: DimensionSpec) -> dict[str, Any]:
    row = {column: None for column in spec.attributes}
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


def calendar_rows(start: date, end: date, academic_start_month: int = 9) -> list[dict[str, Any]]:
    if not 1 <= academic_start_month <= 12 or start > end:
        raise ValueError('Invalid date range or academic_start_month')
    if start.year < 1 or end >= OPEN_END or (end - start).days > 100000:
        raise ValueError('Calendar range must be below 9999-12-31 and at most 100001 days')
    names = ('Thứ Hai', 'Thứ Ba', 'Thứ Tư', 'Thứ Năm', 'Thứ Sáu', 'Thứ Bảy', 'Chủ Nhật')
    rows = [{column: None for column in DATE_FIELDS}]
    rows[0]['date_key'] = 0
    cursor = start
    while cursor <= end:
        iso = cursor.isocalendar()
        academic_year = cursor.year if cursor.month >= academic_start_month else cursor.year - 1
        quarter = (cursor.month - 1) // 3 + 1
        rows.append(dict(zip(DATE_FIELDS, (
            cursor.year * 10000 + cursor.month * 100 + cursor.day, cursor,
            cursor.day, names[cursor.weekday()], iso.week, iso.year, cursor.month,
            f'Tháng {cursor.month}', quarter, cursor.year, cursor.strftime('%Y-%m'),
            f'{cursor.year}-Q{quarter}', f'{academic_year}-{academic_year + 1}',
            cursor.weekday() >= 5))))
        cursor += timedelta(days=1)
    return rows


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
    ] + [StructField('valid_from', DateType(), False), StructField('valid_to', DateType(), False),
         StructField('is_current', BooleanType(), False),
         StructField('effective_date_basis', StringType(), False)])


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
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f'{path}: missing required columns {sorted(missing)}')
    from pyspark.sql.types import StringType
    if not isinstance(frame.schema[spec.id_column].dataType, StringType):
        raise ValueError(f'{path}: {spec.id_column} must be string to preserve business IDs')
    selected = [column for column in (*required, *spec.attributes,
                'skill_name', 'valid_from', 'valid_to', 'is_current') if column in frame.columns]
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
    if event_date_column:
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


def build_shared_dimensions(spark=None, *, sources: Mapping[str, str] | None = None,
                            output_root: str | None = None, as_of: date | str | None = None,
                            date_start: date | str = '2010-01-01',
                            date_end: date | str | None = None, academic_start_month: int = 9,
                            source_format: str = 'auto', max_reference_rows: int = 100000,
                            dry_run: bool = False) -> dict[str, dict[str, Any]]:
    """Airflow-friendly entry point. An injected SparkSession is never stopped."""
    as_of = parse_date(as_of, 'as_of') or date.today()
    date_start = parse_date(date_start, 'date_start')
    date_end = parse_date(date_end, 'date_end') or date(as_of.year + 2, 12, 31)
    if date_start is None or max_reference_rows < 1:
        raise ValueError('date_start is required and max_reference_rows must be positive')
    output_root = (output_root or os.getenv('SHARED_GOLD_ROOT', 's3a://gold/shared')).rstrip('/')
    mapping_root = os.getenv('SHARED_MAPPING_ROOT', '/opt/airflow/configs/mappings').rstrip('/')
    sources = dict(sources or {name: f'{mapping_root}/{spec.filename}' for name, spec in SPECS.items()})
    if set(sources) != set(SPECS):
        raise ValueError(f'Exactly four catalog sources are required: {list(SPECS)}')
    paths = [output_root, *sources.values()]
    if any(path.startswith('s3://') for path in paths):
        raise ValueError('Use s3a:// for Spark paths; s3:// is for Trino registration')
    owns_spark = spark is None
    spark = spark or make_spark(use_s3=any(path.startswith('s3a://') for path in paths))
    try:
        # Preflight every catalog and existing dimension BEFORE the first write.
        prepared: dict[str, tuple[list[dict], Any, str, list[dict] | None]] = {}
        report: dict[str, dict[str, Any]] = {}
        for name, spec in SPECS.items():
            source = bounded_rows(read_catalog(spark, sources[name], spec, source_format), max_reference_rows)
            path, schema = f'{output_root}/dim_{name}', dimension_schema(spec)
            old_frame = existing_delta(spark, path, schema)
            old = bounded_rows(old_frame, max_reference_rows) if old_frame is not None else None
            result = reconcile_dimension(source, old or [], spec, as_of)
            if len(result) > max_reference_rows:
                raise ValueError(f'dim_{name}: history exceeds max_reference_rows')
            prepared[name] = result, schema, path, old
            report[f'dim_{name}'] = {'rows': len(result), 'path': path,
                'current_members': sum(r['is_current'] for r in result) - 1}
        dates = calendar_rows(date_start, date_end, academic_start_month)
        path, schema = f'{output_root}/dim_date', date_schema()
        old_frame = existing_delta(spark, path, schema)
        old_dates = bounded_rows(old_frame, 100002) if old_frame is not None else None
        combined = {row['date_key']: row for row in (old_dates or [])}
        if len(combined) != len(old_dates or []):
            raise ValueError('dim_date: duplicate keys in existing table')
        for row in dates:
            if row['date_key'] in combined and combined[row['date_key']] != row:
                raise ValueError('dim_date: existing date attributes differ; migrate explicitly')
            combined[row['date_key']] = row
        dates = sorted(combined.values(), key=lambda row: row['date_key'])
        # Expand, never shrink, and fill any gap in the combined calendar range.
        actual = [row['full_date'] for row in dates if row['date_key'] != 0]
        if actual:
            if any(not isinstance(value, date) for value in actual):
                raise ValueError('dim_date: invalid existing date')
            full = calendar_rows(min(actual), max(actual), academic_start_month)
            expected = {row['date_key']: row for row in full}
            for row in combined.values():
                if expected.get(row['date_key']) != row:
                    raise ValueError('dim_date: invalid existing calendar row')
            dates = full
        prepared['date'] = dates, schema, path, old_dates
        report['dim_date'] = {'rows': len(dates), 'path': path,
            'start': min(actual).isoformat() if actual else None,
            'end': max(actual).isoformat() if actual else None}
        for name, (rows, schema, path, previous) in prepared.items():
            key = 'date_key' if name == 'date' else SPECS[name].key_column
            changed = False if dry_run else merge_rows(spark, path, rows, schema, key, previous)
            report[f'dim_{name}'].update(written=changed, dry_run=dry_run)
            LOG.info('%s: %s', f'dim_{name}', json.dumps(report[f'dim_{name}'], ensure_ascii=False))
        return report
    finally:
        if owns_spark:
            spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    mapping_root = os.getenv('SHARED_MAPPING_ROOT', '/opt/airflow/configs/mappings').rstrip('/')
    for name, spec in SPECS.items():
        parser.add_argument(f'--{name}-source', default=f'{mapping_root}/{spec.filename}')
    parser.add_argument('--output-root', default=os.getenv('SHARED_GOLD_ROOT', 's3a://gold/shared'))
    parser.add_argument('--as-of', default=date.today().isoformat())
    parser.add_argument('--date-start', default=os.getenv('SHARED_DATE_START', '2010-01-01'))
    parser.add_argument('--date-end', default=os.getenv('SHARED_DATE_END'))
    parser.add_argument('--academic-start-month', type=int, default=9)
    parser.add_argument('--source-format', choices=['auto', 'csv', 'json', 'parquet', 'delta'], default='auto')
    parser.add_argument('--max-reference-rows', type=int, default=100000)
    parser.add_argument('--dry-run', action='store_true', help='Validate all inputs/targets; do not write')
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    report = build_shared_dimensions(sources={name: getattr(args, f'{name}_source') for name in SPECS},
        output_root=args.output_root, as_of=args.as_of, date_start=args.date_start,
        date_end=args.date_end, academic_start_month=args.academic_start_month,
        source_format=args.source_format, max_reference_rows=args.max_reference_rows,
        dry_run=args.dry_run)
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()

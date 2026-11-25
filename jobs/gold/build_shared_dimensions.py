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
    'location': DimensionSpec('location', 'location_id', 'location_key', 'location_name',
        ('province_code', 'location_name', 'location_name_normalized', 'location_type', 'country_code'),
        'location_dictionary.csv'),
    # 'occupation': DimensionSpec('occupation', 'occupation_id', 'occupation_key',
    #     'occupation_name', ('occupation_code', 'occupation_name',
    #     'occupation_group', 'field_group_name'), 'occupation_mapping.csv'),
    # 'industry': DimensionSpec('industry', 'industry_id', 'industry_key',
    #     'industry_name', ('industry_code', 'industry_name', 'industry_group',
    #     'industry_section', 'industry_level', 'parent_industry_code'),
    #     'industry_mapping.csv'),
    'skill': DimensionSpec('skill', 'skill_id', 'skill_key', 'canonical_name',
        ( 'canonical_name', 'normalized_name', 'skill_type', 'skill_category'),
        'skill_dictionary.csv'),
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


def observed_date_rows(days: Iterable[date], academic_start_month: int = 9) -> list[dict[str, Any]]:
    """One row per distinct observed day, plus the reserved unknown member."""
    if not 1 <= academic_start_month <= 12:
        raise ValueError('academic_start_month must be between 1 and 12')
    days = set(days)
    if any(not isinstance(day, date) or isinstance(day, datetime) or day >= OPEN_END for day in days):
        raise ValueError('Source dates must be date values before 9999-12-31')
    if not days:
        raise ValueError('No non-null dates in the configured sources; refusing to clear dim_date')
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


def calendar_rows(start: date, end: date, academic_start_month: int = 9) -> list[dict[str, Any]]:
    """Legacy helper for other modules; the shared Gold builder never calls it."""
    if start > end or end >= OPEN_END or (end - start).days > 100000:
        raise ValueError('Invalid legacy calendar range')
    return observed_date_rows((start + timedelta(days=offset)
                              for offset in range((end - start).days + 1)), academic_start_month)


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
               key_column: str, existing: list[dict[str, Any]] | None, *,
               sync_calendar: bool = False) -> bool:
    """Atomic merge. Only a deliberately synchronized calendar deletes keys."""
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
        merge = (DeltaTable.forPath(spark, path).alias('t')
            .merge(frame.alias('s'), F.col(f't.{key_column}') == F.col(f's.{key_column}'))
            .whenMatchedUpdateAll(condition=compare).whenNotMatchedInsertAll())
        if sync_calendar:
            if key_column != 'date_key' or 0 not in after:
                raise ValueError('Calendar synchronization requires date_key and unknown key 0')
            merge = merge.whenNotMatchedBySourceDelete(condition='t.date_key <> 0')
        merge.execute()
    return True


def reconcile_observed_dates(previous: list[dict[str, Any]], days: Iterable[date],
                             academic_start_month: int):
    """Synchronize observed dates while preserving their YYYYMMDD keys."""
    seen = set()
    for row in previous:
        key, day = row['date_key'], row['full_date']
        if key in seen:
            raise ValueError('dim_date: duplicate keys in existing table')
        seen.add(key)
        if key == 0:
            # Older jobs used NULL for the unknown date. Migrate that row to
            # the existing unknown convention without changing its key.
            continue
        if not isinstance(day, date) or isinstance(day, datetime):
            raise ValueError('dim_date: invalid existing full_date')
        if key != day.year * 10000 + day.month * 100 + day.day:
            raise ValueError('dim_date: incompatible existing date_key; migrate explicitly')
    rows = observed_date_rows(days, academic_start_month)
    removed = sorted(seen - {row['date_key'] for row in rows} - {0})
    return rows, removed


def parse_date_sources(values: Iterable[str]) -> dict[str, tuple[str, ...]]:
    """CLI form PATH::column1,column2; repeat for education/economy sources."""
    result = {}
    for value in values:
        path, separator, raw_columns = value.rpartition('::')
        columns = tuple(dict.fromkeys(column.strip() for column in raw_columns.split(',') if column.strip()))
        if not separator or not path.strip() or not columns:
            raise ValueError('Use --date-source "PATH::column1,column2"')
        path = path.strip()
        result[path] = tuple(dict.fromkeys((*result.get(path, ()), *columns)))
    if not result:
        raise ValueError('At least one date source with explicit column names is required')
    return result


def read_observed_dates(spark, sources: Mapping[str, Iterable[str]], source_format: str = 'auto'):
    """Union distinct real dates. NULL/blank values do not create date members."""
    from pyspark.sql import functions as F
    combined = None
    for path, columns in sources.items():
        if not hadoop_exists(spark, path):
            raise FileNotFoundError(f'Missing date source: {path}')
        suffix = Path(urlparse(path).path).suffix.lower()
        fmt = source_format if source_format != 'auto' else {
            '.csv': 'csv', '.json': 'json', '.jsonl': 'json', '.parquet': 'parquet'
        }.get(suffix)
        if fmt is None:
            fmt = 'delta' if hadoop_exists(spark, f'{path.rstrip("/")}/_delta_log') else 'parquet'
        reader = spark.read.format(fmt)
        if fmt == 'csv':
            reader = (reader.option('header', 'true').option('encoding', 'UTF-8')
                      .option('multiLine', 'true').option('escape', '"').option('mode', 'FAILFAST'))
        frame = reader.load(path)
        renamed = [column.lstrip('\ufeff').strip() for column in frame.columns]
        if len(set(renamed)) != len(renamed):
            raise ValueError(f'Duplicate headers in date source: {path}')
        frame = frame.toDF(*renamed)
        missing = set(columns) - set(frame.columns)
        if missing:
            raise ValueError(f'{path}: missing configured date columns {sorted(missing)}')
        raw = frame.select(F.explode(F.array(*[
            F.trim(F.col('`' + column.replace('`', '``') + '`').cast('string'))
            for column in columns])).alias('_raw_date'))
        parsed = raw.withColumn('full_date', F.to_date(F.col('_raw_date')))
        invalid = parsed.filter(F.col('_raw_date').isNotNull() & (F.col('_raw_date') != '')
                                & F.col('full_date').isNull()).limit(5).collect()
        if invalid:
            raise ValueError(f'{path}: invalid date values {[row["_raw_date"] for row in invalid]}; '
                             'normalize dates in Silver before building Gold')
        dates = parsed.filter(F.col('full_date').isNotNull()).select('full_date').distinct()
        combined = dates if combined is None else combined.unionByName(dates)
    if combined is None:
        raise ValueError('No configured date sources')
    records = bounded_rows(combined.distinct(), 100001)
    return [row['full_date'] for row in records]


def check_removed_date_references(spark, removed_keys: list[int], output_root: str,
                                  extra_roots: Iterable[str] = ()) -> list[str]:
    """Reject shrinking dates referenced by live date_key/*_date_key columns.

    Standard sibling domains are scanned one level deep. Custom layouts must
    supply their domain root or exact Delta table path as an extra root. Published
    snapshots are independent datasets and are not rewritten by this job.
    """
    if not removed_keys:
        return []
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F
    parent = output_root.rsplit('/', 1)[0]
    roots = {f'{parent}/{name}' for name in ('jobs', 'education', 'economy', 'labor')}
    roots.update(root.rstrip('/') for root in extra_roots)
    removed = spark.createDataFrame([(key,) for key in removed_keys], '_removed_date_key long')
    checked = []
    for root in sorted(roots):
        if not hadoop_exists(spark, root):
            continue
        if DeltaTable.isDeltaTable(spark, root):
            paths = [root]
        else:
            jpath = spark._jvm.org.apache.hadoop.fs.Path(root)
            fs = jpath.getFileSystem(spark._jsc.hadoopConfiguration())
            paths = [item.getPath().toString() for item in fs.listStatus(jpath)
                     if item.isDirectory() and not item.getPath().getName().startswith(('_', '.'))]
        for path in sorted(paths):
            if not DeltaTable.isDeltaTable(spark, path):
                continue
            frame = spark.read.format('delta').load(path)
            columns = [column for column in frame.columns
                       if column == 'date_key' or column.endswith('_date_key')]
            for column in columns:
                references = frame.select(F.col(f'`{column}`').cast('long').alias('_removed_date_key'))
                sample = references.join(removed, '_removed_date_key', 'left_semi').limit(1).collect()
                if sample:
                    raise ValueError(f'Cannot shrink dim_date: {path}.{column} still references '
                                     f'date_key={sample[0][0]}. Include all contributing date sources '
                                     'or migrate dependent facts before retrying. No dimensions were written.')
            if columns:
                checked.append(path)
    return sorted(set(checked))


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
    right = dim_date.filter(F.col('date_key') != 0).select(F.col('full_date').alias('_lookup_date'),
                           F.col('date_key').alias('_lookup_key')).alias('_calendar')
    parsed = F.col(f'_fact.`{source_column}`').cast('date')
    joined = left.join(right, parsed == F.col('_calendar._lookup_date'), 'left')
    if joined.filter(parsed.isNotNull() & F.col('_calendar._lookup_key').isNull()).limit(1).count():
        raise ValueError(f'{source_column}: date outside dim_date; extend the calendar')
    return joined.select('_fact.*', F.coalesce(F.col('_calendar._lookup_key'), F.lit(0)).alias(output_key))


def build_shared_dimensions(spark=None, *, sources: Mapping[str, str] | None = None,
                            output_root: str | None = None, as_of: date | str | None = None,
                            date_start: date | str | None = None,
                            date_end: date | str | None = None, academic_start_month: int = 9,
                            source_format: str = 'auto', max_reference_rows: int = 100000,
                            dry_run: bool = False, sync_calendar: bool = True,
                            date_reference_roots: Iterable[str] = (),
                            date_sources: Mapping[str, Iterable[str]] | None = None,
                            date_source_format: str = 'auto') -> dict[str, dict[str, Any]]:
    """Airflow-friendly entry point. An injected SparkSession is never stopped."""
    as_of = parse_date(as_of, 'as_of') or date.today()
    if date_start is not None or date_end is not None:
        LOG.warning('date_start/date_end are ignored: dim_date uses only dates present in date_sources')
    if not sync_calendar:
        raise ValueError('Observed-date mode requires synchronization; sync_calendar=False is unsupported')
    if not date_sources:
        raise ValueError('Provide date_sources={"Silver path": ["posted_date", "expiration_date"]}; '
                         'automatic calendar generation is disabled')
    normalized_sources = {}
    for path, columns in date_sources.items():
        if not isinstance(path, str) or not path.strip() or isinstance(columns, str):
            raise ValueError('Each date source needs a nonempty path and a sequence of column names')
        columns = tuple(columns)
        if not columns or any(not isinstance(column, str) or not column.strip() for column in columns):
            raise ValueError(f'{path}: nonempty date column names are required')
        normalized_sources[path] = tuple(dict.fromkeys(column.strip() for column in columns))
    date_sources = normalized_sources
    if max_reference_rows < 1:
        raise ValueError('max_reference_rows must be positive')
    output_root = (output_root or os.getenv('SHARED_GOLD_ROOT', 's3a://gold/shared')).rstrip('/')
    mapping_root = os.getenv('SHARED_MAPPING_ROOT', '/opt/airflow/configs/mappings').rstrip('/')
    sources = dict(sources or {name: f'{mapping_root}/{spec.filename}' for name, spec in SPECS.items()})
    if set(sources) != set(SPECS):
        raise ValueError(f'Exactly these catalog sources are required: {list(SPECS)}')
    date_reference_roots = tuple(date_reference_roots)
    paths = [output_root, *sources.values(), *date_reference_roots, *date_sources]
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
        observed_days = read_observed_dates(spark, date_sources, date_source_format)
        path, schema = f'{output_root}/dim_date', date_schema()
        old_frame = existing_delta(spark, path, schema)
        old_dates = bounded_rows(old_frame, 100002) if old_frame is not None else None
        dates, removed = reconcile_observed_dates(old_dates or [], observed_days, academic_start_month)
        checked = check_removed_date_references(spark, removed, output_root, date_reference_roots)
        actual = [row['full_date'] for row in dates if row['date_key'] != 0]
        prepared['date'] = dates, schema, path, old_dates
        report['dim_date'] = {'rows': len(dates), 'path': path,
            'start': min(actual).isoformat() if actual else None,
            'end': max(actual).isoformat() if actual else None,
            'date_mode': 'observed', 'source_date_count': len(dates) - 1,
            'date_sources': {path: list(columns) for path, columns in date_sources.items()},
            'sync_calendar': sync_calendar, 'removed_date_keys': len(removed),
            'checked_reference_tables': checked}
        for name, (rows, schema, path, previous) in prepared.items():
            key = 'date_key' if name == 'date' else SPECS[name].key_column
            changed = False if dry_run else merge_rows(spark, path, rows, schema, key, previous,
                                                       sync_calendar=sync_calendar and name == 'date')
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
    parser.add_argument('--date-source', action='append', required=True,
                        help='PATH::column1,column2; repeat for every contributing Silver source')
    parser.add_argument('--date-source-format', choices=['auto', 'csv', 'json', 'parquet', 'delta'], default='auto')
    parser.add_argument('--date-start', help='Deprecated and ignored; no date range is generated')
    parser.add_argument('--date-end', help='Deprecated and ignored; no date range is generated')
    parser.add_argument('--sync-calendar', action='store_true', default=True,
                        help='Compatibility flag; observed-date synchronization is always enabled')
    parser.add_argument('--date-reference-root', action='append', default=[],
                        help='Additional Gold domain root or Delta table path to check before shrinking')
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
        dry_run=args.dry_run, sync_calendar=args.sync_calendar,
        date_reference_roots=args.date_reference_root,
        date_sources=parse_date_sources(args.date_source), date_source_format=args.date_source_format)
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()

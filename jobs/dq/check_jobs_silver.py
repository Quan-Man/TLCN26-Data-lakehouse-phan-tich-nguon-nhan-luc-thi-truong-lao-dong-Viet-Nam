#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import sys
import tempfile
from datetime import date, datetime, timezone
from decimal import Decimal, localcontext
from functools import reduce
from pathlib import Path, PurePosixPath
from urllib.parse import urlsplit
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[2]
TABLES = ("job_postings", "companies", "job_locations", "job_skills")
SHA256 = re.compile(r"^[0-9a-f]{64}$")
HASH_EXCLUDED = {"processed_at", "crawled_at", "record_status", "raw_id",
                 "ingestion_batch_id", "source_file", "source_record_index",
                 "ingested_at", "business_hash"}

def json_default(value):
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat().replace('+00:00', 'Z')
    if isinstance(value, date):
        return value.isoformat()
    raise TypeError(type(value).__name__)

def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, allow_nan=False,
                      separators=(',', ':'), default=json_default)

def reject_constant(value):
    raise ValueError(f'Non-finite JSON value: {value}')

def unique_keys(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f'Duplicate JSON key: {key}')
        result[key] = value
    return result

def decode_json(data):
    return json.loads(data, object_pairs_hook=unique_keys, parse_constant=reject_constant)

def relative(value):
    if not isinstance(value, str) or not value or '\\' in value:
        raise ValueError(f'Invalid relative path: {value!r}')
    parts = value.split('/')
    if any(part in ('', '.', '..') for part in parts) or ':' in parts[0]:
        raise ValueError(f'Unsafe manifest path: {value!r}')
    return PurePosixPath(value).as_posix()

class Issues:
    def __init__(self):
        self.items = {}

    def add(self, code, table='', column='', count=1, sample=None, severity='error'):
        if not count:
            return
        key = (severity, code, table, column)
        item = self.items.setdefault(key, dict(severity=severity, code=code, table=table,
                                               column=column, count=0, samples=[]))
        item['count'] += count
        if sample is not None and len(item['samples']) < 5:
            item['samples'].append(str(sample)[:180])

    def export(self):
        return [self.items[key] for key in sorted(self.items)]

class Store:
    def __init__(self, root):
        self.root = str(root).rstrip('/')
        if self.root.endswith('/current'):
            self.root = self.root[:-len('/current')]
        url = urlsplit(self.root)
        self.client = None
        if url.scheme in ('s3', 's3a'):
            from minio import Minio
            if not url.netloc or url.query or url.fragment:
                raise ValueError('Expected s3://bucket/prefix')
            self.bucket, self.prefix = url.netloc, url.path.strip('/')
            if self.prefix:
                relative(self.prefix)
            endpoint = os.getenv('MINIO_ENDPOINT', 'http://minio:9000')
            parsed = urlsplit(endpoint if '://' in endpoint else '//'+endpoint)
            if parsed.path not in ('', '/') or not parsed.netloc or parsed.username or parsed.password:
                raise ValueError('MINIO_ENDPOINT must be host[:port], http://host[:port] or https://host[:port]')
            if parsed.scheme and parsed.scheme not in ('http', 'https'):
                raise ValueError('Unsupported MinIO endpoint scheme')
            secure = parsed.scheme == 'https' if parsed.scheme else os.getenv('MINIO_SECURE', 'false').lower() in ('1','true','yes')
            access = os.getenv('MINIO_ACCESS_KEY') or os.getenv('MINIO_ROOT_USER') or os.getenv('AWS_ACCESS_KEY_ID')
            secret = os.getenv('MINIO_SECRET_KEY') or os.getenv('MINIO_ROOT_PASSWORD') or os.getenv('AWS_SECRET_ACCESS_KEY')
            if not access or not secret:
                raise ValueError('Missing MinIO credentials in environment')
            self.client = Minio(parsed.netloc, access_key=access, secret_key=secret, secure=secure)
        elif url.scheme and url.scheme != 'file':
            # A Windows drive letter is a local path, not a remote scheme.
            if not re.match(r'^[A-Za-z]:[\\/]', self.root):
                raise ValueError('Use a local path, s3:// or s3a://')
        if self.client is None:
            if url.scheme == 'file':
                raise ValueError('Pass an absolute local filesystem path instead of file://')
            self.local = Path(self.root).expanduser().resolve(strict=True)

    def _key(self, name):
        return '/'.join(part for part in (self.prefix, relative(name)) if part)

    def _path(self, name):
        path = (self.local / relative(name)).resolve()
        if not path.is_relative_to(self.local):
            raise ValueError('Manifest path escapes the local root')
        return path

    def open(self, name):
        if self.client is None:
            return self._path(name).open('rb')
        try:
            return self.client.get_object(self.bucket, self._key(name))
        except Exception as exc:
            if getattr(exc, 'code', None) in ('NoSuchKey', 'NoSuchObject'):
                raise FileNotFoundError(name) from exc
            raise

    @staticmethod
    def close(stream):
        stream.close()
        release = getattr(stream, 'release_conn', None)
        if release:
            release()

    def read(self, name, optional=False):
        try:
            stream = self.open(name)
        except FileNotFoundError:
            if optional:
                return None
            raise
        try:
            data = stream.read(32 * 1024 * 1024 + 1)
            if len(data) > 32 * 1024 * 1024:
                raise ValueError(f'Metadata exceeds 32 MiB: {name}')
            return data
        finally:
            self.close(stream)

    def copy_verified(self, name, destination, spec):
        if not isinstance(spec, dict) or not SHA256.fullmatch(str(spec.get('sha256', ''))):
            raise ValueError(f'Missing/invalid SHA256 in manifest: {name}')
        expected = spec.get('bytes')
        if type(expected) is not int or expected < 0:
            raise ValueError(f'Missing/invalid byte count: {name}')
        stream = self.open(name)
        digest, size = hashlib.sha256(), 0
        try:
            with Path(destination).open('wb') as output:
                while block := stream.read(1024 * 1024):
                    output.write(block)
                    digest.update(block)
                    size += len(block)
        finally:
            self.close(stream)
        if size != expected or digest.hexdigest() != spec['sha256']:
            raise ValueError(f'Checksum/size mismatch: {name}')

def select_batch(store, expected_snapshot_id=None):
    monitors = {}
    pointer_bytes = store.read('latest.json', optional=True)
    if pointer_bytes is None:
        manifest_bytes = store.read('manifest.json')
        manifest = decode_json(manifest_bytes)
        base, data_base = '', ''
        monitors['manifest.json'] = manifest_bytes
    else:
        pointer = decode_json(pointer_bytes)
        if pointer.get('status') != 'completed':
            raise ValueError('Silver latest pointer is not completed')
        base = relative(pointer['snapshot'])
        manifest_name = relative(pointer['manifest'])
        if manifest_name != f'{base}/manifest.json':
            raise ValueError('latest manifest and snapshot refer to different batches')
        manifest_bytes = store.read(manifest_name)
        manifest = decode_json(manifest_bytes)
        if base != 'snapshots/' + str(manifest.get('snapshot_id')):
            raise ValueError('Snapshot ID differs from latest pointer')
        if pointer.get('table_counts') != manifest.get('table_counts'):
            raise ValueError('latest table counts differ from snapshot manifest')
        monitors.update({'latest.json': pointer_bytes, manifest_name: manifest_bytes})
        data_base = base
        if store.client is not None:
            current_bytes = store.read('current/manifest.json')
            current = decode_json(current_bytes)
            if current.get('status') != 'completed':
                raise ValueError('Silver current/ is still updating; retry after the writer completes')
            for name in ('dataset', 'snapshot_id', 'format', 'table_counts', 'processed_at'):
                if current.get(name) != manifest.get(name):
                    raise ValueError(f'current/ and snapshot disagree: {name}')
            if current.get('snapshot') != base:
                raise ValueError('current/ points to another snapshot')
            current_files = current.get('files', {})
            extensions = {'jsonl': ['jsonl'], 'parquet': ['parquet'], 'both': ['parquet', 'jsonl']}.get(manifest.get('format'), [])
            expected_files = {f'{table}.{extension}' for table in TABLES for extension in extensions}
            if not expected_files or set(current_files) != expected_files:
                raise ValueError('current/ manifest does not list all published table files')
            for name, spec in current_files.items():
                if manifest.get('files', {}).get(name) != spec:
                    raise ValueError(f'current/ file metadata differs: {name}')
            monitors['current/manifest.json'] = current_bytes
            data_base = 'current'
    if manifest.get('dataset') != 'silver.jobs' or manifest.get('status') != 'completed':
        raise ValueError('Expected completed silver.jobs manifest')
    snapshot_id = manifest.get('snapshot_id')
    if not isinstance(snapshot_id, str) or not snapshot_id:
        raise ValueError('Missing snapshot_id')
    if expected_snapshot_id and expected_snapshot_id != snapshot_id:
        raise ValueError('Snapshot is not the batch expected by this DAG run')
    return manifest, base, data_base, monitors

def parsed_date(value):
    if isinstance(value, datetime):
        raise ValueError('timestamp is not a date')
    return value if isinstance(value, date) else date.fromisoformat(value)

def parsed_time(value):
    result = value if isinstance(value, datetime) else datetime.fromisoformat(value.replace('Z', '+00:00'))
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError('timestamp requires timezone')
    return result.astimezone(timezone.utc)

def write_report(path, report):
    path = Path(path).resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name('.'+path.name+'.'+uuid4().hex+'.tmp')
    try:
        temporary.write_text(json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False)+'\n', encoding='utf-8')
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def normalize_spark_json(value):
    """Fill optional Spark metadata keys without changing nullability rules."""
    if isinstance(value, dict):
        value = {k: normalize_spark_json(v) for k, v in value.items()}
        if value.get('type') == 'struct':
            for field in value['fields']:
                field.setdefault('metadata', {})
                field.setdefault('nullable', True)
        if value.get('type') == 'array':
            value.setdefault('containsNull', True)
        return value
    if isinstance(value, list):
        return [normalize_spark_json(v) for v in value]
    return value


def spark_schema(value):
    from pyspark.sql.types import StructType, StructField, _parse_datatype_json_value
    if value.get('type') == 'struct':
        result = StructType.fromJson(normalize_spark_json(value))
    else:
        # Compact producer schema.json: {column: "string!", ...}.
        fields = []
        for name, spec in value.items():
            if not isinstance(spec, str):
                raise ValueError(f'Invalid compact schema field: {name}')
            typ = spec.rstrip('!')
            dtype = ({'type': 'array', 'elementType': 'string', 'containsNull': False}
                     if typ == 'array' else 'integer' if typ == 'int' else typ)
            fields.append(StructField(name, _parse_datatype_json_value(dtype),
                                      not spec.endswith('!')))
        result = StructType(fields)
    if not result.fields or len(result.names) != len(set(result.names)):
        raise ValueError('Empty schema or duplicate field names')
    if any(not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', n) for n in result.names):
        raise ValueError('Unsupported column name in schema')
    return result


def load_contract(document):
    result = {}
    for table in TABLES:
        entry = document.get('tables', {}).get('silver.' + table)
        if entry is None:
            entry = document.get('tables', {}).get(table)
        if not isinstance(entry, dict):
            raise ValueError(f'Missing tables["silver.{table}"] in schema config')
        definition = entry.get('spark_schema', entry)
        schema = spark_schema(definition)
        keys = entry.get('primary_key', document.get('primary_keys', {}).get(table))
        if not keys or len(keys) != len(set(keys)) or not set(keys) <= set(schema.names):
            raise ValueError(f'Missing/invalid primary key: {table}')
        unique = entry.get('unique_keys', [])
        fks = entry.get('foreign_keys', [])
        for key in unique:
            if not key or not set(key) <= set(schema.names):
                raise ValueError(f'Invalid unique key in {table}: {key}')
        for fk in fks:
            if (not fk.get('columns') or not set(fk['columns']) <= set(schema.names)
                    or len(fk['columns']) != len(fk.get('referenced_columns', []))):
                raise ValueError(f'Invalid foreign key in {table}: {fk}')
        result[table] = dict(schema=schema, primary_key=keys, unique_keys=unique,
                             foreign_keys=fks)
    return result

def same_type(left, right):
    return left.simpleString() == right.simpleString()


def compare_schema(actual, expected, table, issues, *, allow_extra=False):
    missing = sorted(set(expected.names) - set(actual.names))
    extra = [] if allow_extra else sorted(set(actual.names) - set(expected.names))
    if missing or extra:
        issues.add('SCHEMA_COLUMNS', table, sample=f'missing={missing}; extra={extra}')
    for field in expected:
        if field.name in actual.names and not same_type(actual[field.name].dataType, field.dataType):
            issues.add('SCHEMA_TYPE', table, field.name,
                       sample=f'expected={field.dataType.simpleString()}, actual={actual[field.name].dataType.simpleString()}')
    return not missing and not extra and all(
        same_type(actual[f.name].dataType, f.dataType) for f in expected if f.name in actual.names)

def make_spark(master, partitions):
    from pyspark.sql import SparkSession
    if not re.fullmatch(r'local(?:\[(?:\*|[1-9]\d*)(?:,[1-9]\d*)?\])?', master):
        raise ValueError('Use --master local[1]; staged files are local, not shared with remote executors')
    if partitions < 1:
        raise ValueError('--partitions must be positive')
    spark = (SparkSession.builder.master(master).appName('check-jobs-silver')
             .config('spark.sql.session.timeZone', 'UTC')
             .config('spark.sql.shuffle.partitions', str(partitions))
             .config('spark.default.parallelism', str(partitions))
             .config('spark.sql.adaptive.enabled', 'false')
             .config('spark.ui.enabled', 'false')
             .config('spark.driver.host', os.getenv('SPARK_LOCAL_IP', '127.0.0.1'))
             .config('spark.driver.bindAddress', os.getenv('SPARK_LOCAL_IP', '127.0.0.1'))
             .getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    return spark

def parse_json_value(value, dtype):
    from pyspark.sql import types as T
    if value is None:
        return None
    if isinstance(dtype, T.StringType):
        if not isinstance(value, str):
            raise ValueError('expected string')
    elif isinstance(dtype, T.BooleanType):
        if type(value) is not bool:
            raise ValueError('expected boolean')
    elif isinstance(dtype, (T.IntegerType, T.LongType)):
        bits = 32 if isinstance(dtype, T.IntegerType) else 64
        if type(value) is not int or not -(2**(bits-1)) <= value < 2**(bits-1):
            raise ValueError('invalid integer')
    elif isinstance(dtype, (T.DoubleType, T.FloatType)):
        if type(value) not in (int, float) or not math.isfinite(value):
            raise ValueError('expected finite number')
        return float(value)
    elif isinstance(dtype, T.DecimalType):
        if type(value) not in (str, int, float, Decimal):
            raise ValueError('expected decimal')
        number = Decimal(str(value))
        with localcontext() as context:
            context.prec = max(40, dtype.precision + dtype.scale)
            if (not number.is_finite() or abs(number) >= Decimal(10)**(dtype.precision-dtype.scale)
                    or number != number.quantize(Decimal(1).scaleb(-dtype.scale))):
                raise ValueError('decimal overflow or excess scale')
            return number.quantize(Decimal(1).scaleb(-dtype.scale))
    elif isinstance(dtype, T.DateType):
        return parsed_date(value)
    elif isinstance(dtype, T.TimestampType):
        return parsed_time(value)
    elif isinstance(dtype, T.ArrayType):
        if not isinstance(value, list):
            raise ValueError('expected array')
        if not dtype.containsNull and any(v is None for v in value):
            raise ValueError('NULL array element')
        return [parse_json_value(v, dtype.elementType) for v in value]
    else:
        raise ValueError(f'Unsupported JSONL type: {dtype.simpleString()}')
    return value

def parse_json_partition(lines, schema_json):
    from pyspark.sql.types import StructType
    schema = StructType.fromJson(schema_json)
    for record in lines:
        values, errors = {}, []
        try:
            raw = decode_json(record.value.lstrip('\ufeff'))
            if not isinstance(raw, dict):
                raise ValueError('expected JSON object')
            if set(raw) != set(schema.names):
                errors.append('JSON_COLUMNS')
            for field in schema:
                try:
                    values[field.name] = parse_json_value(raw.get(field.name), field.dataType)
                except (ValueError, TypeError, ArithmeticError, OverflowError):
                    errors.append('JSON_VALUE:' + field.name)
        except (ValueError, TypeError):
            errors.append('INVALID_JSON')
        yield tuple(values.get(field.name) for field in schema) + (errors,)

def read_table(spark, path, schema):
    from pyspark.sql import types as T
    if path.suffix == '.parquet':
        return spark.read.parquet(str(path))
    # Input rows remain nullable so DQ reports NULLs rather than crashing the JVM.
    relaxed = T.StructType([T.StructField(f.name, f.dataType, True) for f in schema])
    relaxed.add('_dq_parse_errors', T.ArrayType(T.StringType()), False)
    text = spark.read.text(str(path))
    parsed = text.rdd.mapPartitions(lambda it: parse_json_partition(it, schema.jsonValue()))
    return spark.createDataFrame(parsed, relaxed)

def business_digest(row):
    values = row.asDict(recursive=True)
    try:
        return hashlib.sha256(canonical({k: v for k, v in values.items()
                                         if k not in HASH_EXCLUDED}).encode('utf-8')).hexdigest()
    except (ValueError, TypeError):
        return None

def row_rules(frame, schemas, processed_at, table, issues):
    from pyspark.sql import functions as F, types as T
    rules, rule_keys = [], set()
    def add(code, condition, column='', severity='error'):
        key = (code, column, severity)
        if key in rule_keys:
            return
        rule_keys.add(key)
        rules.append((code, column, severity, condition))
    fields = {}
    for schema in schemas:
        for field in schema:
            previous = fields.get(field.name)
            if previous is None or not field.nullable:
                fields[field.name] = field
            if isinstance(field.dataType, T.ArrayType) and not field.dataType.containsNull:
                add('ARRAY_NULL_ELEMENT', F.exists(F.col(field.name), lambda x: x.isNull()), field.name)
    for field in fields.values():
        c = F.col(field.name)
        if not field.nullable:
            add('NULL', c.isNull(), field.name)
            if isinstance(field.dataType, T.StringType):
                add('BLANK', F.length(F.trim(c)) == 0, field.name)
        if isinstance(field.dataType, T.DecimalType):
            add('NEGATIVE_VALUE', c < 0, field.name)
        if isinstance(field.dataType, (T.DoubleType, T.FloatType)):
            add('NONFINITE', F.isnan(c) | (F.abs(c) == float('inf')), field.name)
    if '_dq_parse_errors' in frame.columns:
        add('JSON_PARSE', F.size('_dq_parse_errors') > 0)
    if table == 'job_postings':
        c = F.col
        for col in ('raw_id', 'business_hash'):
            if col in frame.columns:
                add('HASH_FORMAT', ~c(col).rlike('^[0-9a-f]{64}$'), col)
        if 'source_record_index' in frame.columns:
            add('NEGATIVE_SOURCE_INDEX', c('source_record_index') < 0, 'source_record_index')
        add('MIXED_PROCESSING_BATCH', ~c('processed_at').eqNullSafe(F.lit(processed_at)), 'processed_at')
        low, high, mid, typ = [c(n) for n in ('salary_min_vnd', 'salary_max_vnd', 'salary_midpoint_vnd', 'salary_type')]
        has_amount = low.isNotNull() | high.isNotNull() | mid.isNotNull()
        allowed = ['MISSING', 'NEGOTIABLE', 'UNPARSED', 'FIXED', 'RANGE', 'LOWER_BOUND', 'UPPER_BOUND']
        add('SALARY_TYPE', ~typ.isin(allowed))
        add('SALARY_MUST_BE_NULL', typ.isin('MISSING', 'NEGOTIABLE', 'UNPARSED') & has_amount)
        add('SALARY_BOUNDS_MISSING', typ.isin('FIXED', 'RANGE') & (low.isNull() | high.isNull() | mid.isNull()))
        add('SALARY_BOUND_SHAPE', ((typ == 'LOWER_BOUND') & (low.isNull() | high.isNotNull() | mid.isNotNull())) |
            ((typ == 'UPPER_BOUND') & (high.isNull() | low.isNotNull() | mid.isNotNull())))
        add('SALARY_CURRENCY', has_amount & ~c('salary_currency').eqNullSafe(F.lit('VND')))
        add('REVERSED_SALARY_RANGE', low > high)
        add('SALARY_MIDPOINT', mid.isNotNull() & (low.isNull() | high.isNull() | (mid != F.round((low + high) / 2, 2))))
        add('FIXED_SALARY_RANGE', (typ == 'FIXED') & (low != high))
        lo, hi, et = [c(n) for n in ('experience_min_years', 'experience_max_years', 'experience_type')]
        add('EXPERIENCE_TYPE', ~et.isin([a for a in allowed if a != 'NEGOTIABLE'] + ['NO_REQUIREMENT']))
        add('EXPERIENCE_MUST_BE_NULL', et.isin('MISSING', 'UNPARSED', 'NO_REQUIREMENT') & (lo.isNotNull() | hi.isNotNull()))
        add('REVERSED_EXPERIENCE_RANGE', lo > hi)
        add('EXPERIENCE_BOUNDS_MISSING', et.isin('FIXED', 'RANGE') & (lo.isNull() | hi.isNull()))
        add('FIXED_EXPERIENCE_RANGE', (et == 'FIXED') & (lo != hi))
        add('EXPERIENCE_BOUND_SHAPE', ((et == 'LOWER_BOUND') & (lo.isNull() | hi.isNotNull())) |
            ((et == 'UPPER_BOUND') & (hi.isNull() | lo.isNotNull())))
        add('EXPIRATION_BEFORE_POSTED', c('expiration_date') < c('posted_date'))
        observed_day = F.to_date(F.from_utc_timestamp(c('crawled_at'), 'Asia/Ho_Chi_Minh'))
        status = (F.when(c('expiration_date').isNull() | c('crawled_at').isNull(), 'UNKNOWN')
                  .when(c('expiration_date') < observed_day, 'EXPIRED').otherwise('WITHIN_DEADLINE'))
        add('RECORD_STATUS', ~c('record_status').eqNullSafe(status))
        if 'business_hash' in frame.columns:
            digest = F.udf(business_digest, T.StringType())
            names = [n for n in frame.columns if not n.startswith('_dq_')]
            add('BUSINESS_HASH_MISMATCH', ~c('business_hash').eqNullSafe(digest(F.struct(*names))))
    elif table == 'job_skills':
        c = F.col('confidence_score')
        add('CONFIDENCE_RANGE', (c < 0) | (c > 1))
        add('SKILL_REQUIREMENT_UNKNOWN', F.col('is_required').isNull(), 'is_required', 'warning')
    # One aggregate action for all row-level checks, not collect() on raw data.
    counts = frame.agg(*[F.sum(F.when(cond, 1).otherwise(0)).alias('r'+str(i))
                         for i, (_, _, _, cond) in enumerate(rules)]).first()
    for i, (code, col, severity, _) in enumerate(rules):
        issues.add(code, table, col, count=int(counts[i] or 0), severity=severity)

def duplicate_check(frame, keys, table, issues, code='DUPLICATE_KEY', nullable=False):
    from pyspark.sql import functions as F
    selected = frame
    if nullable:
        selected = selected.where(reduce(lambda a, b: a & b, [F.col(k).isNotNull() for k in keys]))
    groups = selected.groupBy(*keys).count().where(F.col('count') > 1)
    excess = groups.agg(F.sum(F.col('count') - 1)).first()[0] or 0
    issues.add(code, table, ','.join(keys), count=int(excess))

def foreign_check(frame, columns, parent, parent_columns, table, issues, code='FOREIGN_KEY'):
    from pyspark.sql import functions as F
    names = ['k'+str(i) for i in range(len(columns))]
    child = frame.select(*[F.col(c).alias(n) for c, n in zip(columns, names)])
    child = child.where(reduce(lambda a, b: a & b, [F.col(n).isNotNull() for n in names]))
    target = parent.select(*[F.col(c).alias(n) for c, n in zip(parent_columns, names)]).distinct()
    issues.add(code, table, ','.join(columns), count=child.join(target, names, 'left_anti').count())

def bounded_counts(frame, column):
    rows = frame.groupBy(column).count().limit(1001).collect()
    if len(rows) > 1000:
        raise ValueError(f'Too many metric groups: {column}')
    return {r[0]: r['count'] for r in rows if r[0] is not None}

def validate_frames(frames, contracts, frozen, references, processed_at, counts, metrics, issues, report, thresholds):
    from pyspark.sql import functions as F
    for table, frame in frames.items():
        n = frame.count()
        report['rows'][table] = n
        if n != counts[table]:
            issues.add('ROW_COUNT', table, sample=f'actual={n}, manifest={counts[table]}')
        row_rules(frame, [frozen[table]['schema'], contracts[table]['schema']], processed_at, table, issues)
        primary = contracts[table]['primary_key']
        for key in primary:
            issues.add('NULL_PRIMARY_KEY', table, key, count=frame.where(F.col(key).isNull()).count())
        duplicate_check(frame, primary, table, issues)
        unique = list(contracts[table]['unique_keys'])
        core_unique = {'job_postings': ['source_name', 'source_job_id'],
                       'companies': ['source_name', 'normalized_name']}.get(table)
        if core_unique and core_unique not in unique:
            unique.append(core_unique)
        for keys in unique:
            duplicate_check(frame, keys, table, issues, code='DUPLICATE_UNIQUE_KEY', nullable=True)
    jobs, companies = frames['job_postings'], frames['companies']
    locations, skills = frames['job_locations'], frames['job_skills']
    if not report['rows']['job_postings']:
        issues.add('EMPTY_JOBS', 'job_postings')
    # Always enforce core relations, including when compact snapshot schemas lack FK metadata.
    relations = [
        ('job_postings', ['company_id'], 'silver.companies', ['company_id']),
        ('job_postings', ['primary_location_id'], 'reference.locations', ['location_id']),
        ('job_locations', ['job_id'], 'silver.job_postings', ['job_id']),
        ('job_locations', ['location_id'], 'reference.locations', ['location_id']),
        ('job_skills', ['job_id'], 'silver.job_postings', ['job_id']),
        ('job_skills', ['skill_id'], 'reference.skills', ['skill_id'])]
    for table, entry in contracts.items():
        for fk in entry['foreign_keys']:
            relation = (table, fk['columns'], fk['references'], fk['referenced_columns'])
            if relation not in relations:
                relations.append(relation)
            if not fk.get('nullable', True):
                bad = frames[table].where(reduce(lambda a, b: a | b, [F.col(c).isNull() for c in fk['columns']])).count()
                issues.add('NULL_FK', table, ','.join(fk['columns']), count=bad)
    parents = {'silver.'+k: v for k, v in frames.items()} | references
    for table, cols, ref, refcols in relations:
        parent = parents.get(ref)
        if parent is None:
            populated = frames[table].where(reduce(lambda a, b: a & b, [F.col(c).isNotNull() for c in cols])).count()
            if populated:
                issues.add('MISSING_REFERENCE', table, ','.join(cols), sample=f'{ref}: supply --reference {ref}=/path.csv')
        elif not set(refcols) <= set(parent.columns):
            issues.add('REFERENCE_COLUMNS', table, sample=ref)
        else:
            foreign_check(frames[table], cols, parent, refcols, table, issues)
    foreign_check(jobs, ['job_id', 'primary_location_id'], locations, ['job_id', 'location_id'],
                  'job_postings', issues, 'PRIMARY_LOCATION_PAIR')
    if 'is_primary' in locations.columns:
        foreign_check(jobs, ['job_id', 'primary_location_id'], locations.where(F.col('is_primary') == True),
                      ['job_id', 'location_id'], 'job_postings', issues, 'PRIMARY_LOCATION_FLAG')
    linked = jobs.alias('j').join(companies.alias('c'), F.col('j.company_id') == F.col('c.company_id'))
    issues.add('COMPANY_SOURCE_MISMATCH', 'job_postings', 'company_id',
               count=linked.where(~F.col('j.source_name').eqNullSafe(F.col('c.source_name'))).count())
    total = jobs.select('job_id').distinct().count()
    covered = {
        'company': jobs.join(companies.select('company_id').distinct(), 'company_id').select('job_id').distinct().count(),
        'location': jobs.select('job_id', F.col('primary_location_id').alias('location_id'))
            .join(locations.select('job_id', 'location_id').distinct(), ['job_id', 'location_id'])
            .join(references['reference.locations'].select('location_id').distinct(), 'location_id')
            .select('job_id').distinct().count(),
        'skill': jobs.select('job_id').join(skills.join(references['reference.skills'].select('skill_id').distinct(), 'skill_id')
            .select('job_id').distinct(), 'job_id').distinct().count()}
    for name, numerator in covered.items():
        ratio = numerator / total if total else 0.0
        report['coverage'][name] = dict(numerator=numerator, denominator=total, ratio=ratio)
        issues.add('COVERAGE_GAP', 'job_postings', name, count=total-numerator, severity='warning')
        if thresholds[name] is not None and ratio < thresholds[name]:
            issues.add('COVERAGE_THRESHOLD', 'job_postings', name, sample=ratio)
    required = skills.where(F.col('is_required') == True)
    report['skills'] = dict(pairs=report['rows']['job_skills'], required_pairs=required.count(),
        jobs_with_required_skills=required.select('job_id').distinct().count(),
        unknown_required_pairs=skills.where(F.col('is_required').isNull()).count())
    report.update(source_counts=bounded_counts(jobs, 'source_name'), salary_types=bounded_counts(jobs, 'salary_type'),
                  quality_flags=bounded_counts(jobs.select(F.explode('data_quality_flags').alias('flag')), 'flag'))
    for name in ('table_counts', 'source_counts', 'salary_types', 'quality_flags'):
        actual = report['rows'] if name == 'table_counts' else report[name]
        if metrics.get(name) != actual:
            issues.add('METRICS_MISMATCH', column=name)
    for name in ('duplicate_observations', 'quarantined_records'):
        value = metrics.get(name)
        if type(value) is not int or value < 0:
            issues.add('METRICS_INVALID', column=name)
        elif value:
            issues.add(name.upper(), count=value, severity='warning')

def check(silver_root='s3://silver/jobs', *, schema_path=None, expected_snapshot_id=None,
          master='local[1]', partitions=1, min_company_coverage=None, min_location_coverage=None,
          min_skill_coverage=None, reference_paths=None, spark=None):
    from pyspark import StorageLevel
    from pyspark.sql import functions as F
    issues, cached, owned = Issues(), [], spark is None
    report = dict(check='silver.jobs', engine='pyspark', run_id=uuid4().hex,
                  silver_root=str(silver_root), started_at=datetime.now(timezone.utc).isoformat(),
                  rows={}, coverage={}, issues=[])
    store, monitors = None, {}
    try:
        thresholds = dict(company=min_company_coverage, location=min_location_coverage, skill=min_skill_coverage)
        if any(t is not None and not 0 <= t <= 1 for t in thresholds.values()):
            raise ValueError('Coverage thresholds must be between 0 and 1')
        store = Store(silver_root)
        manifest, metadata_base, data_base, monitors = select_batch(store, expected_snapshot_id)
        report.update(snapshot_id=manifest['snapshot_id'], data_path=data_base or '.', format=manifest.get('format'))
        formats = {'jsonl': ['jsonl'], 'parquet': ['parquet'], 'both': ['parquet', 'jsonl']}.get(manifest.get('format'))
        if formats is None:
            raise ValueError('Unsupported Silver format')
        processed_at = parsed_time(manifest['processed_at'])
        files, counts = manifest.get('files'), manifest.get('table_counts')
        if (not isinstance(files, dict) or not isinstance(counts, dict) or set(counts) != set(TABLES)
                or any(type(c) is not int or c < 0 for c in counts.values())):
            raise ValueError('Invalid manifest table_counts/files')
        with tempfile.TemporaryDirectory(prefix='dq_silver_jobs_') as temporary:
            work = Path(temporary)
            def fetch(name, base=metadata_base):
                destination = work / relative(name)
                destination.parent.mkdir(parents=True, exist_ok=True)
                if name not in files:
                    raise ValueError(f'Missing manifest entry: {name}')
                store.copy_verified('/'.join(p for p in (base, name) if p), destination, files[name])
                return destination
            frozen = load_contract(decode_json(fetch('schema.json').read_bytes()))
            metrics = decode_json(fetch('metrics.json').read_bytes())
            default_config = ROOT / 'configs/schema/job_schema.json'
            config_path = Path(schema_path) if schema_path else default_config if default_config.is_file() else None
            config_bytes = config_path.read_bytes() if config_path else None
            contracts = load_contract(decode_json(config_bytes.decode('utf-8-sig'))) if config_bytes else frozen
            report['schema_source'] = str(config_path.resolve()) if config_path else 'snapshot/schema.json'
            report['schema_sha256'] = hashlib.sha256(config_bytes).hexdigest() if config_bytes else files['schema.json']['sha256']
            report['extra_columns_policy'] = 'allow producer columns beyond external schema; require every declared column'
            report['checks_policy'] = 'JSON spark_schema/keys/FKs are enforced; prose checks are not executed as SQL. Business checks are explicit Python rules.'
            for table in TABLES:
                compare_schema(frozen[table]['schema'], contracts[table]['schema'], table, issues, allow_extra=True)
                if frozen[table]['primary_key'] != contracts[table]['primary_key']:
                    issues.add('PRIMARY_KEY_CONTRACT', table)
            if any(i['severity'] == 'error' for i in issues.export()):
                raise ValueError('Config and producer schema disagree; fix process_job_silver output or its schema contract')
            paths = {t: [fetch(t+'.'+ext, data_base) for ext in formats] for t in TABLES}
            ref_files = {'reference.locations': (fetch('references/location_dictionary.csv'), 'location_id'),
                         'reference.skills': (fetch('references/skill_dictionary.csv'), 'skill_id')}
            for name, path in (reference_paths or {}).items():
                if name in ref_files:
                    raise ValueError(f'Cannot replace frozen reference: {name}')
                if name not in ('reference.occupations', 'reference.industries'):
                    raise ValueError(f'Unsupported reference: {name}')
                source = Path(path).resolve(strict=True)
                dest = work / (name + '.csv')
                # Stage immutable bytes and recheck the external file at the end.
                digest = hashlib.sha256()
                with source.open('rb') as inp, dest.open('wb') as out:
                    while block := inp.read(1024*1024):
                        digest.update(block)
                        out.write(block)
                ref_files[name] = (dest, 'occupation_id' if name.endswith('occupations') else 'industry_id')
                report.setdefault('reference_files', {})[str(source)] = digest.hexdigest()
            if spark is None:
                spark = make_spark(master, partitions)
            if not spark.sparkContext.master.startswith('local'):
                raise ValueError('Staged inputs require local Spark')
            spark.conf.set('spark.sql.session.timeZone', 'UTC')
            spark.sparkContext.addPyFile(str(Path(__file__).resolve()))
            frames, references = {}, {}
            for name, (path, key) in ref_files.items():
                frame = spark.read.option('header', True).option('inferSchema', False).option('mode', 'FAILFAST').csv(str(path))
                if key not in frame.columns:
                    raise ValueError(f'Missing {key} in {name}')
                frame = frame.select(key).persist(StorageLevel.DISK_ONLY)
                cached.append(frame)
                n = frame.count()
                if not n:
                    issues.add('EMPTY_REFERENCE', name)
                issues.add('NULL_REFERENCE_KEY', name, key,
                           count=frame.where(F.col(key).isNull() | (F.trim(F.col(key)) == '')).count())
                duplicate_check(frame, [key], name, issues)
                references[name] = frame
            valid = True
            for table, inputs in paths.items():
                loaded = []
                for path in inputs:
                    frame = read_table(spark, path, frozen[table]['schema']).persist(StorageLevel.DISK_ONLY)
                    cached.append(frame)
                    columns = frame.drop('_dq_parse_errors').schema
                    valid = compare_schema(columns, frozen[table]['schema'], table, issues) and valid
                    if '_dq_parse_errors' in frame.columns:
                        errors = frame.where(F.size('_dq_parse_errors') > 0).count()
                        issues.add('JSON_PARSE', table, count=errors)
                    loaded.append(frame)
                frames[table] = loaded[0]
                if valid and len(loaded) == 2:
                    names = frozen[table]['schema'].names
                    left, right = [f.select(*names) for f in loaded]
                    difference = left.exceptAll(right).limit(1).count() + right.exceptAll(left).limit(1).count()
                    issues.add('PARQUET_JSONL_CONTENT_MISMATCH', table, count=1 if difference else 0)
            if valid:
                validate_frames(frames, contracts, frozen, references, processed_at, counts, metrics, issues, report, thresholds)
            for source, digest in report.get('reference_files', {}).items():
                current = hashlib.sha256()
                with Path(source).open('rb') as stream:
                    while block := stream.read(1024*1024):
                        current.update(block)
                if current.hexdigest() != digest:
                    issues.add('REFERENCE_CHANGED_DURING_CHECK', sample=source)
            if config_path and config_path.read_bytes() != config_bytes:
                issues.add('SCHEMA_CHANGED_DURING_CHECK')
    except Exception as exc:
        issues.add('CHECK_FAILED', sample=f'{type(exc).__name__}: {exc}')
    finally:
        for frame in reversed(cached):
            try:
                frame.unpersist(blocking=True)
            except Exception:
                pass
        if owned and spark is not None:
            try:
                spark.stop()
            except Exception as exc:
                issues.add('SPARK_STOP_FAILED', sample=type(exc).__name__)
        if store is not None:
            for name, original in monitors.items():
                try:
                    if store.read(name) != original:
                        issues.add('BATCH_CHANGED_DURING_CHECK', sample=name)
                except Exception as exc:
                    issues.add('BATCH_RECHECK_FAILED', sample=type(exc).__name__)
    report['issues'] = issues.export()
    for severity in ('error', 'warning'):
        report[severity+'_count'] = sum(i['count'] for i in report['issues'] if i['severity'] == severity)
    report['status'] = 'failed' if report['error_count'] else 'passed'
    report['finished_at'] = datetime.now(timezone.utc).isoformat()
    return report

def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--silver-root', '--input-dir', default=os.getenv('JOBS_SILVER_ROOT', 's3://silver/jobs'))
    parser.add_argument('--schema', type=Path, help='JSON with tables["silver.<table>"].spark_schema')
    parser.add_argument('--master', default=os.getenv('SPARK_MASTER', 'local[1]'))
    parser.add_argument('--partitions', type=int, default=1)
    parser.add_argument('--expected-snapshot-id')
    for name in ('company', 'location', 'skill'):
        parser.add_argument('--min-'+name+'-coverage', type=float)
    parser.add_argument('--reference', action='append', default=[], metavar='NAME=CSV_PATH',
                        help='Optional reference.occupations or reference.industries CSV')
    parser.add_argument('--strict-warnings', action='store_true')
    parser.add_argument('--report', type=Path)
    args = parser.parse_args(argv)
    refs = {}
    for item in args.reference:
        if '=' not in item:
            parser.error('--reference expects NAME=CSV_PATH')
        name, path = item.split('=', 1)
        if name in refs or not path:
            parser.error('Duplicate/empty --reference')
        refs[name] = path
    report = check(args.silver_root, schema_path=args.schema, master=args.master, partitions=args.partitions,
                   expected_snapshot_id=args.expected_snapshot_id, reference_paths=refs,
                   min_company_coverage=args.min_company_coverage,
                   min_location_coverage=args.min_location_coverage, min_skill_coverage=args.min_skill_coverage)
    if args.strict_warnings and report['warning_count']:
        report.update(status='failed', strict_warnings=True)
    path = args.report or ROOT/'data/dq_reports/jobs_silver'/f'{report["run_id"]}.json'
    report['report_path'] = str(path.resolve())
    try:
        write_report(path, report)
    except Exception as exc:
        print(f'Cannot save DQ report: {exc}', file=sys.stderr)
        return 1
    print(json.dumps({k: report.get(k) for k in ('status', 'snapshot_id', 'rows', 'error_count', 'warning_count', 'report_path')}, ensure_ascii=False))
    for issue in report['issues']:
        print(f"{issue['severity'].upper()} {issue['code']} {issue['table']}.{issue['column']}: {issue['count']} {issue['samples']}", file=sys.stderr)
    return 0 if report['status'] == 'passed' else 1


if __name__ == '__main__':
    raise SystemExit(main())
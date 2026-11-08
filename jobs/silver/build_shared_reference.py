"""Validate shared CSV mappings -> six Silver Delta reference tables -> Gold.

Called by dags/00_reference_data.py; no Spark or Airflow side effects on import.
Reference catalogs are bounded, not fact data. Missing source effective dates
stay NULL in Silver; Gold assigns OBSERVED dates using the explicit as_of.
"""
from __future__ import annotations

import csv
from datetime import date
import hashlib
import io
import json
from pathlib import Path

from jobs.gold import build_shared_dimensions as shared


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
        default=str, separators=(',', ':')).encode('utf-8')).hexdigest()


def csv_rows(path, maximum):
    """Bound memory, preserve IDs, reject malformed widths and duplicate headers."""
    path = Path(path)
    if path.stat().st_size > 32 * 1024 * 1024:
        raise ValueError(f'Mapping exceeds 32 MiB: {path.name}')
    data = path.read_bytes()
    reader = csv.reader(io.StringIO(data.decode('utf-8-sig')), strict=True)
    try:
        headers = [name.strip() for name in next(reader)]
    except StopIteration as exc:
        raise ValueError(f'Empty mapping: {path.name}') from exc
    if not headers or any(not h for h in headers) or len(headers) != len(set(headers)):
        raise ValueError(f'Blank/duplicate headers: {path.name}')
    rows = []
    for values in reader:
        if not values or not any(v.strip() for v in values):
            continue
        if len(values) != len(headers):
            raise ValueError(f'Invalid CSV width: {path.name}, line {reader.line_num}')
        rows.append(dict(zip(headers, values)))
        if len(rows) > maximum:
            raise ValueError(f'Mapping exceeds {maximum} rows: {path.name}')
    return rows, headers, hashlib.sha256(data).hexdigest()


def canonical_catalog(records, spec, as_of):
    canonical = {}
    for raw in records:
        row = shared.canonical_row(raw, spec)
        row['is_current'] = row.pop('_source_current')
        token = (row[spec.id_column], row['valid_from'])
        if token in canonical and canonical[token] != row:
            raise ValueError(f'{spec.name}: conflicting catalog versions: {token}')
        canonical[token] = row
    rows = sorted(canonical.values(), key=lambda r: (r[spec.id_column], r['valid_from'] or date.min))
    if not rows:
        raise ValueError(f'{spec.name}: empty catalog; do not publish accidental empty reference')
    # Validate explicit intervals, duplicate versions and overlaps using the same
    # rules as Gold. Discard generated OBSERVED dates; keep source dates intact.
    shared.reconcile_dimension(rows, [], spec, as_of)
    return rows


def canonical_aliases(records, skill_ids):
    """No unimplemented context rules: one normalized alias -> one real skill."""
    result = {}
    for raw in records:
        alias = shared.clean_text(raw.get('alias'))
        identifier = shared.clean_text(raw.get('skill_id'))
        if not alias or not identifier:
            raise ValueError('skill_aliases: blank alias/skill_id')
        if identifier not in skill_ids:
            raise ValueError(f'skill_aliases: orphan skill_id {identifier}')
        normalized = alias.casefold()  # Preserve accents and C/C++/C# punctuation.
        if normalized in result and result[normalized]['skill_id'] != identifier:
            raise ValueError(f'skill_aliases: ambiguous alias {alias}; context resolver is required')
        # A contextual alias must not silently become an unconditional match.
        if any(shared.clean_text(raw.get(c)) for c in ('context', 'scope', 'source_name', 'match_rule', 'priority')):
            raise ValueError('skill_aliases: contextual/priority rules need an explicit resolver; not supported here')
        result[normalized] = {'normalized_alias': normalized, 'skill_id': identifier}
    return sorted(result.values(), key=lambda row: row['normalized_alias'])


def prepare_mappings(*, mapping_root, as_of, date_start='2010-01-01', date_end=None,
                     academic_start_month=9, max_reference_rows=100000):
    as_of = shared.parse_date(as_of, 'as_of')
    start = shared.parse_date(date_start, 'date_start')
    end = shared.parse_date(date_end, 'date_end') if date_end else date(as_of.year + 2, 12, 31) if as_of else None
    if as_of is None or start is None or end is None or max_reference_rows < 1:
        raise ValueError('Explicit as_of/date range and positive max_reference_rows are required')
    shared.calendar_rows(start, end, academic_start_month)  # Range/config preflight.
    root = Path(mapping_root).resolve()
    catalogs, inputs = {}, {}
    for name, spec in shared.SPECS.items():
        path = root/spec.filename
        records, headers, sha = csv_rows(path, max_reference_rows)
        required = {spec.id_column, spec.name_column}
        if name == 'skill' and 'canonical_name' not in headers and 'skill_name' in headers:
            required.remove('canonical_name'); required.add('skill_name')
        if not required.issubset(headers):
            raise ValueError(f'{path.name}: missing columns {sorted(required-set(headers))}')
        catalogs[name] = canonical_catalog(records, spec, as_of)
        inputs[name] = {'path': str(path), 'sha256': sha, 'rows': len(records)}
    aliases_path = root/'skill_aliases.csv'
    records, headers, sha = csv_rows(aliases_path, max_reference_rows)
    if not {'alias', 'skill_id'}.issubset(headers):
        raise ValueError('skill_aliases.csv requires alias, skill_id headers')
    aliases = canonical_aliases(records, {r['skill_id'] for r in catalogs['skill']})
    inputs['skill_alias'] = {'path': str(aliases_path), 'sha256': sha, 'rows': len(records)}
    settings = {'mapping_root': str(root), 'as_of': as_of.isoformat(), 'date_start': start.isoformat(),
        'date_end': end.isoformat(), 'academic_start_month': academic_start_month,
        'max_reference_rows': max_reference_rows}
    fingerprint = digest({'settings': settings, 'inputs': inputs})
    return catalogs, aliases, {'settings': settings, 'inputs': inputs, 'fingerprint': fingerprint}


def validate_mappings(**settings):
    """Only compact manifest crosses XCom, never catalog rows/DataFrames."""
    return prepare_mappings(**settings)[2]


def reference_schema(name):
    from pyspark.sql import types as T
    if name == 'date':
        return shared.date_schema()
    if name == 'skill_alias':
        return T.StructType([T.StructField('normalized_alias', T.StringType()), T.StructField('skill_id', T.StringType())])
    spec = shared.SPECS[name]
    return T.StructType([T.StructField(c, T.StringType()) for c in ('reference_key', spec.id_column, *spec.attributes)] + [
        T.StructField('valid_from', T.DateType()), T.StructField('valid_to', T.DateType()),
        T.StructField('is_current', T.BooleanType())])


def existing_snapshot(spark, path, schema, maximum):
    from delta.tables import DeltaTable
    old = shared.existing_delta(spark, path, schema)
    if old is None:
        return None, None
    version = int(DeltaTable.forPath(spark, path).history(1).first()['version'])
    frame = spark.read.format('delta').option('versionAsOf', version).load(path)
    return shared.bounded_rows(frame, maximum), version


def assert_version(spark, path, expected):
    from delta.tables import DeltaTable
    if expected is None:
        if shared.hadoop_exists(spark, path):
            raise ValueError(f'Table appeared during preflight: {path}')
    elif int(DeltaTable.forPath(spark, path).history(1).first()['version']) != expected:
        raise ValueError(f'Reference version changed: {path}; retry whole reference DAG')


def synchronize(spark, path, rows, previous, schema, key):
    from delta.tables import DeltaTable
    before = {r[key]: r for r in previous or []}
    after = {r[key]: r for r in rows}
    if len(before) != len(previous or []) or len(after) != len(rows):
        raise ValueError(f'Duplicate reference key: {path}')
    if previous is not None and before == after:
        return False
    frame = spark.createDataFrame(rows, schema)
    if previous is None:
        frame.write.format('delta').mode('errorifexists').save(path)
    else:
        condition = ' OR '.join(f'NOT (s.`{c}` <=> t.`{c}`)' for c in frame.columns if c != key)
        (DeltaTable.forPath(spark, path).alias('t').merge(frame.alias('s'), f't.`{key}` = s.`{key}`')
            .whenMatchedUpdateAll(condition=condition).whenNotMatchedInsertAll().whenNotMatchedBySourceDelete().execute())
    return True


def calendar_union(old, settings):
    """Expand Silver calendar; reject changed attributes instead of rewriting."""
    requested = shared.calendar_rows(shared.parse_date(settings['date_start'], 'date_start'),
        shared.parse_date(settings['date_end'], 'date_end'), settings['academic_start_month'])
    combined = {r['date_key']: r for r in old or []}
    if len(combined) != len(old or []):
        raise ValueError('ref_date: duplicate date keys')
    for row in requested:
        if row['date_key'] in combined and combined[row['date_key']] != row:
            raise ValueError('ref_date attributes changed; explicit migration is required')
        combined[row['date_key']] = row
    actual = [r['full_date'] for r in combined.values() if r['date_key'] != 0]
    full = shared.calendar_rows(min(actual), max(actual), settings['academic_start_month'])
    expected = {r['date_key']: r for r in full}
    if any(expected.get(key) != row for key, row in combined.items()):
        raise ValueError('ref_date: invalid existing calendar')
    return full


def build_shared_reference(*, validation, silver_root='s3a://silver/reference',
                           gold_root='s3a://gold/shared', spark=None):
    from delta.tables import DeltaTable
    catalogs, aliases, manifest = prepare_mappings(**validation['settings'])
    if manifest['fingerprint'] != validation['fingerprint']:
        raise ValueError('Mapping changed after validation; rerun the DAG from validate_mappings')
    silver_root, gold_root = silver_root.rstrip('/'), gold_root.rstrip('/')
    if silver_root == gold_root or any(p.startswith('s3://') for p in (silver_root,gold_root)):
        raise ValueError('Distinct Silver/Gold roots and Spark s3a:// paths are required')
    owns_spark = spark is None
    spark = spark or shared.make_spark('silver-shared-reference', any(p.startswith('s3a://') for p in (silver_root,gold_root)))
    settings = manifest['settings']
    maximum = settings['max_reference_rows']
    try:
        # Validate existing Gold SCD/calendar BEFORE any Silver mutation.
        gold_preview = shared.build_shared_dimensions(spark=spark, sources={n: manifest['inputs'][n]['path'] for n in shared.SPECS},
            output_root=gold_root, as_of=settings['as_of'], date_start=settings['date_start'], date_end=settings['date_end'],
            academic_start_month=settings['academic_start_month'], max_reference_rows=maximum, dry_run=True)
        if validate_mappings(**settings)['fingerprint'] != manifest['fingerprint']:
            raise ValueError('Mapping changed during preflight; rerun validation')
        calendar_settings = dict(settings, date_start=gold_preview['dim_date']['start'],
            date_end=gold_preview['dim_date']['end'])
        prepared = []
        for name, records in {**catalogs, 'skill_alias': aliases, 'date': []}.items():
            path, schema = f'{silver_root}/ref_{name}', reference_schema(name)
            previous, version = existing_snapshot(spark, path, schema, 100002 if name == 'date' else maximum)
            if name == 'date':
                rows, key = calendar_union(previous, calendar_settings), 'date_key'
            elif name == 'skill_alias':
                rows, key = records, 'normalized_alias'
            else:
                spec = shared.SPECS[name]
                rows = [dict(r, reference_key=digest([name, r[spec.id_column], r['valid_from']])) for r in records]
                key = 'reference_key'
            prepared.append((name,path,rows,previous,schema,key,version))
        for _, path, _, _, _, _, version in prepared:
            assert_version(spark, path, version)
        tables = {}
        for name, path, rows, previous, schema, key, _ in prepared:
            written = synchronize(spark, path, rows, previous, schema, key)
            version = int(DeltaTable.forPath(spark, path).history(1).first()['version'])
            tables[name] = {'path': path, 'version': version, 'rows': len(rows), 'written': written}
        return {'manifest': manifest, 'silver_root': silver_root, 'gold_root': gold_root, 'tables': tables}
    finally:
        if owns_spark:
            spark.stop()


def build_gold_from_reference(*, reference, spark=None):
    from delta.tables import DeltaTable
    settings = reference['manifest']['settings']
    owns_spark = spark is None
    spark = spark or shared.make_spark('gold-shared-reference', any(
        p.startswith('s3a://') for p in (reference['silver_root'],reference['gold_root'])))
    try:
        for table in reference['tables'].values():
            assert_version(spark, table['path'], table['version'])
        result = shared.build_shared_dimensions(spark=spark,
            sources={n: reference['tables'][n]['path'] for n in shared.SPECS}, source_format='delta',
            output_root=reference['gold_root'], as_of=settings['as_of'],
            date_start=settings['date_start'], date_end=settings['date_end'],
            academic_start_month=settings['academic_start_month'], max_reference_rows=settings['max_reference_rows'])
        # Do not emit readiness for an input advanced by another writer.
        for table in reference['tables'].values():
            assert_version(spark, table['path'], table['version'])
        for table in result.values():
            table['version'] = int(DeltaTable.forPath(spark, table['path']).history(1).first()['version'])
        return {'as_of': settings['as_of'], 'fingerprint': reference['manifest']['fingerprint'],
            'silver_tables': reference['tables'], 'gold_tables': result}
    finally:
        if owns_spark:
            spark.stop()

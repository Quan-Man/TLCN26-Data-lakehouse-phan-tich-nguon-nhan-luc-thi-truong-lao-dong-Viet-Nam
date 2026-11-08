"""Versioned DQ rules and publish decisions shared by file and Delta checks."""
from __future__ import annotations

import csv
import hashlib
import json
from collections import Counter
from decimal import Decimal, InvalidOperation
from pathlib import Path

from .settings import Settings, load_yaml
from .table_catalog import load_table_catalog


def load_rules(path=None):
    settings = Settings.from_env()
    path = Path(path).resolve() if path else settings.config_path('dq', 'rules.yml')
    config = load_yaml(path)
    if not isinstance(config, dict) or not isinstance(config.get('version'), str):
        raise ValueError('DQ rules require a string version')
    catalog = load_table_catalog()
    if set(config.get('tables', {})) != set(catalog):
        raise ValueError('DQ tables must exactly match configs/tables.yml')
    if set(config.get('publish', {})) != {'block_on_warn'}:
        raise ValueError('Invalid DQ publish policy')
    if any(type(v) is not bool for v in config['publish'].values()):
        raise ValueError('DQ publish flags must be boolean')
    for name, rule in config['tables'].items():
        if rule['primary_key'] != catalog[name]['primary_key'] or rule['layer'] != catalog[name]['layer']:
            raise ValueError(f'DQ/catalog key or layer mismatch: {name}')
        grain = rule['grain']['columns']
        if not grain or len(set(grain)) != len(grain):
            raise ValueError(f'Invalid grain: {name}')
        if any(kind not in {'string', 'date', 'timestamp', 'boolean', 'integer', 'numeric', 'array'}
               for kind in rule['types'].values()):
            raise ValueError(f'Unknown column type: {name}')
        limits = rule['thresholds']
        warn, fail = limits['warn_rate_gt'], limits['fail_rate_gt']
        if not 0 <= warn <= fail <= 1 or limits['empty'] not in {'pass', 'warn', 'fail'}:
            raise ValueError(f'Invalid thresholds: {name}')
        for fk in rule['foreign_keys']:
            if fk['references'] not in config['tables'] and fk['references'] not in config['references']:
                raise ValueError(f'Unknown FK reference: {name}')
            if not fk['columns'] or len(fk['columns']) != len(fk['reference_columns']):
                raise ValueError(f'Invalid FK columns: {name}')
        for coverage in rule['mapping_coverage']:
            if (coverage['denominator'] != 'all_rows'
                    or not 0 <= coverage['fail_below'] <= coverage['warn_below'] <= 1
                    or coverage['references'] not in set(config['tables']) | set(config['references'])):
                raise ValueError(f'Invalid mapping coverage: {name}')
        for bounds in rule['ranges'].values():
            if 'min' in bounds and 'max' in bounds and bounds['min'] > bounds['max']:
                raise ValueError(f'Reversed numeric range: {name}')
    config = dict(config)
    config['checksum'] = hashlib.sha256(path.read_bytes()).hexdigest()
    return config


def load_references(config, root=None):
    root = Path(root) if root else Settings.from_env().project_root
    result = {}
    for name, ref in config['references'].items():
        with (root / ref['path']).open(encoding='utf-8-sig', newline='') as stream:
            rows = list(csv.DictReader(stream))
        if not rows or any(any(blank(row.get(k)) for k in ref['key']) for row in rows):
            raise ValueError(f'Empty/invalid DQ reference: {name}')
        keys = [tuple(row[k] for k in ref['key']) for row in rows]
        if len(keys) != len(set(keys)):
            raise ValueError(f'Duplicate reference keys: {name}')
        result[name] = rows
    return result


def blank(value):
    return value is None or isinstance(value, str) and not value.strip()


def number(value):
    if isinstance(value, bool):
        raise ValueError('Boolean is not a numeric measure')
    result = Decimal(str(value))
    if not result.is_finite():
        raise ValueError('Non-finite number')
    return result


def evaluate_tables(tables, config=None, *, references=None, selected=None, issues=()):
    """Evaluate materialized rows; suitable for the project's local batch snapshots.

    Duplicate counts count excess rows. Violation rates are per check, with all
    table rows as denominator. Missing inputs/references always block publish.
    """
    from dq._common import _matches_type
    config = config or load_rules()
    selected = list(selected) if selected is not None else list(config['tables'])
    unknown = set(selected) - set(config['tables'])
    if unknown or not selected:
        raise ValueError('DQ selection is empty or contains unknown tables')
    combined = {**(references or {}), **tables}
    findings = [dict(item) for item in issues]
    metrics = {}

    def emit(name, code, count, total, *, severity=None, detail=None):
        limits = config['tables'][name]['thresholds']
        rate = count / total if total else 0
        if severity is None:
            severity = ('error' if rate > limits['fail_rate_gt'] else
                        'warning' if rate > limits['warn_rate_gt'] else None)
        if severity:
            findings.append(dict(table=name, code=code, count=count, rate=rate,
                                 severity=severity, message=detail or code))

    for name in selected:
        rule = config['tables'][name]
        if name not in tables:
            emit(name, 'MISSING_TABLE', 1, 0, severity='error')
            continue
        rows = tables[name]
        total = len(rows)
        metrics[name] = {'rows': total, 'mapping_coverage': {}}
        if not total:
            level = rule['thresholds']['empty']
            if level != 'pass':
                emit(name, 'EMPTY_TABLE', 0, 0, severity='error' if level == 'fail' else 'warning')
        for field in set(rule['not_null']) | set(rule['primary_key']):
            emit(name, 'NULL:' + field, sum(blank(r.get(field)) for r in rows), total)
        for label, fields in [('PK', rule['primary_key']), ('GRAIN', rule['grain']['columns'])]:
            keys = [json.dumps([r.get(f) for f in fields], sort_keys=True, default=str) for r in rows]
            emit(name, 'DUPLICATE_' + label, sum(v - 1 for v in Counter(keys).values()), total)
        for field, kind in rule['types'].items():
            invalid = 0
            for row in rows:
                value = row.get(field)
                if value is None:
                    continue
                try:
                    valid = _matches_type(value, kind)
                    if kind == 'numeric':
                        number(value)
                except (ValueError, TypeError, InvalidOperation):
                    valid = False
                invalid += not valid
            emit(name, 'TYPE:' + field, invalid, total)
        for field, bounds in rule['ranges'].items():
            invalid = 0
            for row in rows:
                value = row.get(field)
                if blank(value):
                    invalid += not bounds.get('nullable', True)
                    continue
                try:
                    value = number(value)
                    invalid += ('min' in bounds and value < Decimal(str(bounds['min']))) or (
                        'max' in bounds and value > Decimal(str(bounds['max'])))
                except (ValueError, TypeError, InvalidOperation):
                    invalid += 1
            emit(name, 'RANGE:' + field, invalid, total)
        for low, high in rule.get('ordered_pairs', []):
            invalid = 0
            for row in rows:
                if blank(row.get(low)) or blank(row.get(high)):
                    continue
                try:
                    invalid += number(row[low]) > number(row[high])
                except (ValueError, InvalidOperation):
                    invalid += 1
            emit(name, f'ORDER:{low}:{high}', invalid, total)
        for dependency in rule.get('null_dependencies', []):
            invalid = sum(blank(r.get(dependency['if_null'])) and not blank(r.get(dependency['then_null'])) for r in rows)
            emit(name, 'NULL_DEPENDENCY', invalid, total)
        for fk in rule['foreign_keys']:
            parent = combined.get(fk['references'])
            if parent is None:
                emit(name, 'MISSING_REFERENCE:' + fk['references'], 1, total, severity='error')
                continue
            values = {tuple(r.get(f) for f in fk['reference_columns']) for r in parent}
            invalid = 0
            for row in rows:
                key = tuple(row.get(f) for f in fk['columns'])
                if any(blank(v) for v in key):
                    invalid += not (fk['nullable'] and all(blank(v) for v in key))
                else:
                    invalid += key not in values
            emit(name, 'FK:' + ','.join(fk['columns']), invalid, total)
        for coverage in rule['mapping_coverage']:
            parent = combined.get(coverage['references'])
            if parent is None:
                emit(name, 'MISSING_MAPPING:' + coverage['references'], 1, total, severity='error')
                continue
            values = {r.get(coverage['reference_column']) for r in parent
                      if not blank(r.get(coverage['reference_column']))}
            matched = sum(not blank(r.get(coverage['column'])) and r.get(coverage['column']) in values for r in rows)
            ratio = matched / total if total else None
            metrics[name]['mapping_coverage'][coverage['column']] = ratio
            if ratio is not None and ratio < coverage['warn_below']:
                emit(name, 'MAPPING_COVERAGE:' + coverage['column'], total - matched, total,
                     severity='error' if ratio < coverage['fail_below'] else 'warning')
    counts = Counter(item['severity'] for item in findings)
    allowed = not counts['error'] and not (config['publish']['block_on_warn'] and counts['warning'])
    return dict(rules_version=config['version'], rules_checksum=config['checksum'],
                status='passed' if allowed else 'failed', publish_allowed=allowed,
                selected_tables=selected, scope='catalog' if set(selected) == set(config['tables']) else 'selection',
                metrics=metrics, issue_counts=dict(counts), issues=findings)

#!/usr/bin/env python3
from __future__ import annotations

import argparse
from base64 import b64encode
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from http.client import HTTPConnection, HTTPSConnection
import json
import logging
import math
import os
from pathlib import Path
import re
import ssl
import sys
import time
from urllib.parse import urlsplit
from uuid import uuid4

LOG = logging.getLogger('register_trino')
DEFAULT_ROOTS = {'shared': 's3a://gold/shared', 'jobs': 's3a://gold/jobs',
                 'education': 's3a://gold/education', 'economy': 's3a://gold/economic'}
DEFAULT_SCHEMAS = {'shared': 'shared', 'jobs': 'jobs',
                   'education': 'education', 'economy': 'economic'}
ROOT_ENVS = {'shared': 'SHARED_GOLD_ROOT', 'jobs': 'JOBS_GOLD_ROOT',
             'education': 'EDUCATION_GOLD_PREFIX', 'economy': 'ECONOMY_GOLD_ROOT'}
DOMAIN_TABLES = {
    'shared': ('dim_date', 'dim_location', 'dim_skill'),
    'jobs': ('dim_company', 'fact_job_posting', 'fact_job_skill', 'bridge_job_location'),
    'education': ('dim_university', 'dim_major', 'dim_program', 'fact_admission'),
    'economy': ('dim_industry', 'dim_occupation', 'dim_economic_indicator', 'fact_economic_statistics'),
}

def identifier(value):
    if not isinstance(value, str) or not re.fullmatch(r'[a-z][a-z0-9_]{0,100}', value):
        raise ValueError(f'Invalid SQL identifier: {value!r}')
    return '"' + value + '"'

def qualified(*parts):
    return '.'.join(identifier(part) for part in parts)

def sql_literal(value):
    if not isinstance(value, str) or any(ord(c) < 32 for c in value):
        raise ValueError('SQL literal must be a string without control characters')
    return "'" + value.replace("'", "''") + "'"

def trino_location(value):
    if not isinstance(value, str) or any(ord(c) < 32 for c in value):
        raise ValueError('Invalid Delta location')
    url = urlsplit(value)
    if (url.scheme not in ('s3', 's3a') or not re.fullmatch(r'[a-z0-9][a-z0-9.-]*[a-z0-9]', url.netloc)
            or url.query or url.fragment or not url.path.strip('/')):
        raise ValueError(f'Expected s3a://bucket/table-path or s3://bucket/table-path: {value!r}')
    path = url.path.rstrip('/')
    if any(p in ('', '.', '..') for p in path.split('/')[1:]):
        raise ValueError(f'Invalid object prefix: {value!r}')
    if path.endswith('/_delta_log'):
        raise ValueError('Use the table directory, not its _delta_log subdirectory')
    return 's3://' + url.netloc + path

@dataclass(frozen=True)
class TableSpec:
    domain: str
    catalog: str
    schema: str
    name: str
    location: str

    @property
    def sql_name(self):
        return qualified(self.catalog, self.schema, self.name)

    @property
    def label(self):
        return '.'.join((self.catalog, self.schema, self.name))

def build_plan(config=None, *, domains=None, catalog=None, roots=None, schemas=None):
    config = config or {}
    selected = set(domains or ['all'])
    if not selected <= {'all', *DEFAULT_ROOTS}:
        raise ValueError('Unknown domain; choose shared, jobs, education, economy or all')
    if 'all' in selected:
        selected = set(DEFAULT_ROOTS)
    catalog = catalog or os.getenv('TRINO_CATALOG') or config.get('trino', {}).get('catalog', 'delta')
    identifier(catalog)
    resolved_roots, resolved_schemas = {}, {}
    for group in DEFAULT_ROOTS:
        root = ((roots or {}).get(group) or os.getenv(ROOT_ENVS[group])
                or config.get('roots', {}).get(group) or DEFAULT_ROOTS[group])
        resolved_roots[group] = trino_location(root)
        schema = ((schemas or {}).get(group) or os.getenv('TRINO_'+group.upper()+'_SCHEMA')
                  or config.get('schemas', {}).get(group) or DEFAULT_SCHEMAS[group])
        identifier(schema)
        resolved_schemas[group] = schema
    if len(set(resolved_schemas.values())) != len(resolved_schemas):
        raise ValueError('Use separate schemas for shared, jobs, education and economy')
    for section in ('roots', 'schemas'):
        unknown = set(config.get(section, {})) - set(DEFAULT_ROOTS)
        if unknown:
            raise ValueError(f'Unknown {section} keys: {sorted(unknown)}')
    known = {group+'.'+name for group, names in DOMAIN_TABLES.items() for name in names}
    overrides = config.get('tables', {})
    unknown = set(overrides) - known
    if unknown:
        raise ValueError('Config tables must use group.table keys from the current model; '
                         f'not the old publication.json list. Unknown: {sorted(unknown)}')
    def table_path(group, name):
        item = overrides.get(group+'.'+name, {})
        path = item if isinstance(item, str) else item.get('path')
        return trino_location(path or resolved_roots[group]+'/'+name)
    # These builder env vars can otherwise make Education consume a different calendar/location.
    if 'education' in selected:
        for variable, name in [('GOLD_DATE_PATH', 'dim_date'), ('GOLD_LOCATION_PATH', 'dim_location')]:
            if os.getenv(variable) and trino_location(os.environ[variable]) != table_path('shared', name):
                raise ValueError(f'{variable} disagrees with shared Gold; align builder and registrar paths')
    required_shared = set(DOMAIN_TABLES['shared']) if selected & {'shared', 'jobs'} else set()
    if selected & {'education', 'economy'}:
        required_shared.update(('dim_date', 'dim_location'))
    plan = []
    def add(domain, name, owner=None):
        plan.append(TableSpec(domain, catalog, resolved_schemas[domain], name, table_path(owner or domain, name)))
    for name in DOMAIN_TABLES['shared']:
        if name in required_shared:
            add('shared', name)
    for group in ('jobs', 'education', 'economy'):
        if group not in selected:
            continue
        if group == 'jobs':
            for name in DOMAIN_TABLES['shared']:
                add(group, name, owner='shared')
        for name in DOMAIN_TABLES[group]:
            add(group, name)
    if len({t.label for t in plan}) != len(plan):
        raise ValueError('Duplicate target table in registration plan')
    return plan, resolved_roots


class TrinoSQL:
    def __init__(self):
        self.scheme = os.getenv('TRINO_HTTP_SCHEME', 'http')
        self.host = os.getenv('TRINO_HOST', 'trino')
        self.port = int(os.getenv('TRINO_PORT', '8080'))
        user = os.getenv('TRINO_USER', 'publisher')
        password = os.getenv('TRINO_PASSWORD')
        if self.scheme not in ('http', 'https') or not 0 < self.port < 65536:
            raise ValueError('Invalid Trino HTTP scheme/port')
        if not user or any(ord(c) < 32 for c in user) or ':' in user:
            raise ValueError('Invalid TRINO_USER')
        if password and self.scheme != 'https':
            raise ValueError('TRINO_PASSWORD requires TRINO_HTTP_SCHEME=https')
        self.query_timeout = float(os.getenv('TRINO_QUERY_TIMEOUT', '600'))
        self.request_timeout = float(os.getenv('TRINO_REQUEST_TIMEOUT', '30'))
        if any(not math.isfinite(t) or t <= 0 for t in (self.query_timeout, self.request_timeout)):
            raise ValueError('Trino timeouts must be positive and finite')
        self.headers = {'X-Trino-User': user, 'X-Trino-Source': 'lakehouse-register-trino',
                        'Content-Type': 'text/plain; charset=utf-8'}
        if password:
            token = b64encode(f'{user}:{password}'.encode()).decode('ascii')
            self.headers['Authorization'] = 'Basic '+token
        self.context = ssl.create_default_context(cafile=os.getenv('TRINO_CA_BUNDLE')) if self.scheme == 'https' else None
        self.connection = None

    def close(self):
        if self.connection:
            self.connection.close()
            self.connection = None

    @staticmethod
    def continuation(uri):
        url = urlsplit(uri)
        # Poll the configured coordinator, including when its advertised hostname differs.
        if (url.scheme not in ('http', 'https') or not url.netloc or url.fragment
                or not url.path.startswith('/v1/statement/')
                or any(p in ('.', '..') for p in url.path.split('/'))):
            raise ValueError('Unexpected Trino continuation URI')
        return url.path + ('?'+url.query if url.query else '')

    def request(self, method, target, body, timeout):
        # Fresh connection bounds each socket timeout and allows recovery after a failed response.
        self.close()
        if self.scheme == 'https':
            self.connection = HTTPSConnection(self.host, self.port, timeout=timeout, context=self.context)
        else:
            self.connection = HTTPConnection(self.host, self.port, timeout=timeout)
        self.connection.request(method, target, body=body, headers=self.headers)
        with self.connection.getresponse() as response:
            content = response.read(8*1024*1024+1)
            if len(content) > 8*1024*1024:
                raise RuntimeError('Unexpectedly large Trino response')
            return response.status, response.getheader('Retry-After'), content

    def query(self, sql):
        method, target, body = 'POST', '/v1/statement', sql.encode('utf-8')
        rows, pending = [], None
        deadline = time.monotonic() + self.query_timeout
        try:
            while True:
                remaining = deadline-time.monotonic()
                if remaining <= 0:
                    raise TimeoutError('Trino query exceeded TRINO_QUERY_TIMEOUT')
                status, retry_after, content = self.request(method, target, body, min(remaining, self.request_timeout))
                if status in (429, 502, 503, 504) and method == 'GET':
                    # A POST with an uncertain outcome is never blindly replayed.
                    try:
                        delay = max(0.05, min(5.0, float(retry_after or '0.1')))
                    except ValueError:
                        delay = 0.1
                    time.sleep(min(delay, max(0, deadline-time.monotonic())))
                    continue
                if status != 200:
                    raise RuntimeError(f'Trino HTTP {status}; check coordinator, authentication and access rights')
                page = json.loads(content)
                next_uri = page.get('nextUri')
                pending = self.continuation(next_uri) if next_uri else None
                if error := page.get('error'):
                    raise RuntimeError(f"{error.get('errorName', 'TRINO_ERROR')}: {error.get('message', 'query failed')}")
                rows.extend(page.get('data') or [])
                if len(rows) > 10000:
                    raise RuntimeError('Registration queries returned too many rows')
                if pending is None:
                    return rows
                method, target, body = 'GET', pending, None
        except BaseException:
            if pending:
                try:
                    self.request('DELETE', pending, None, min(2.0, self.request_timeout))
                except Exception:
                    pass
            raise
        finally:
            self.close()

def flush(table, query):
    query(f'CALL {identifier(table.catalog)}.system.flush_metadata_cache('
          f'schema_name => {sql_literal(table.schema)}, table_name => {sql_literal(table.name)})')

def check_location(table, query):
    result = query('SHOW CREATE TABLE '+table.sql_name)
    if len(result) != 1 or not result[0] or not isinstance(result[0][0], str):
        raise ValueError(f'{table.label}: missing SHOW CREATE TABLE result')
    locations = re.findall(r"\blocation\s*=\s*'((?:''|[^'])*)'", result[0][0], re.IGNORECASE)
    if len(locations) != 1:
        raise ValueError(f'{table.label}: cannot determine its Delta location')
    actual = trino_location(locations[0].replace("''", "'"))
    if actual != table.location:
        raise ValueError(f'{table.label}: existing location {actual} differs from expected {table.location}. '
                         'Correct the path/configuration explicitly; no table was dropped.')

def existing_table(table, query):
    rows = query(f'SELECT table_type FROM {identifier(table.catalog)}.information_schema.tables '
                 f'WHERE table_schema = {sql_literal(table.schema)} AND table_name = {sql_literal(table.name)}')
    if not rows:
        return False
    if rows != [['BASE TABLE']] and rows != [('BASE TABLE',)]:
        raise ValueError(f'{table.label}: existing object is not one BASE TABLE')
    flush(table, query)
    check_location(table, query)
    return True

def register_all(plan, schema_roots, query, *, count_rows=False, report=None):
    report = report if report is not None else {}
    report['tables'] = []
    # Detect wrong paths or views across all existing targets before adding any new ones.
    existing = {table.label: existing_table(table, query) for table in plan}
    created_schemas = set()
    for table in plan:
        namespace = (table.catalog, table.schema)
        if namespace not in created_schemas:
            query(f'CREATE SCHEMA IF NOT EXISTS {qualified(*namespace)} '
                  f'WITH (location = {sql_literal(schema_roots[table.domain]+"/")})')
            created_schemas.add(namespace)
        result = dict(table=table.label, location=table.location,
                      action='existing' if existing[table.label] else 'registered')
        LOG.info('%s -> %s', table.label, table.location)
        if not existing[table.label]:
            try:
                query(f'CALL {identifier(table.catalog)}.system.register_table('
                      f'schema_name => {sql_literal(table.schema)}, table_name => {sql_literal(table.name)}, '
                      f'table_location => {sql_literal(table.location)})')
            except Exception as original:
                # A second registrar may have created the same table after preflight.
                # Swallow only the original error if a fully checked matching table now exists.
                try:
                    recovered = existing_table(table, query)
                except Exception:
                    recovered = False
                if not recovered:
                    raise original
                result['action'] = 'existing_after_race'
        flush(table, query)
        check_location(table, query)
        # Empty tables are valid: this query may return zero rows.
        query('SELECT 1 FROM '+table.sql_name+' LIMIT 1')
        history_name = qualified(table.catalog, table.schema) + '."' + table.name + '$history"'
        history = query(f'SELECT version FROM {history_name} ORDER BY version DESC LIMIT 1')
        if len(history) != 1 or type(history[0][0]) is not int:
            raise ValueError(f'{table.label}: cannot read Delta version')
        result.update(readable=True, observed_delta_version=history[0][0])
        if count_rows:
            rows = query('SELECT count(*) FROM '+table.sql_name)
            if len(rows) != 1 or type(rows[0][0]) is not int:
                raise ValueError(f'{table.label}: invalid COUNT result')
            result['rows'] = rows[0][0]
        report['tables'].append(result)
    report['status'] = 'passed'
    return report


def write_report(path, report):
    path = Path(path).resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name('.'+path.name+'.'+uuid4().hex+'.tmp')
    try:
        temp.write_text(json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False)+'\n', encoding='utf-8')
        os.replace(temp, path)
    finally:
        temp.unlink(missing_ok=True)

def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--domains', nargs='+', default=['all'], choices=['all', *DEFAULT_ROOTS])
    parser.add_argument('--config', type=Path, help='Explicit registration config; old publication.json is not auto-loaded')
    parser.add_argument('--catalog')
    for group in DEFAULT_ROOTS:
        parser.add_argument('--'+group+'-root')
        parser.add_argument('--'+group+'-schema')
    parser.add_argument('--dry-run', action='store_true', help='Print mappings; do not connect or change metadata')
    parser.add_argument('--count', action='store_true', help='Also COUNT every registered table')
    parser.add_argument('--report', type=Path, help='Optional atomic JSON report path')
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    report = dict(run_id=uuid4().hex, started_at=datetime.now(timezone.utc).isoformat(),
                  domains=args.domains, status='failed', tables=[])
    client = None
    try:
        config = json.loads(args.config.read_text(encoding='utf-8-sig')) if args.config else {}
        if not isinstance(config, dict):
            raise ValueError('--config must contain a JSON object')
        roots = {g: getattr(args, g+'_root') for g in DEFAULT_ROOTS}
        schemas = {g: getattr(args, g+'_schema') for g in DEFAULT_ROOTS}
        plan, schema_roots = build_plan(config, domains=args.domains, catalog=args.catalog, roots=roots, schemas=schemas)
        report.update(planned_tables=len(plan), unique_delta_paths=len({t.location for t in plan}))
        if args.dry_run:
            report.update(status='planned', tables=[asdict(t) for t in plan])
        else:
            client = TrinoSQL()
            register_all(plan, schema_roots, client.query, count_rows=args.count, report=report)
    except Exception as exc:
        report.update(status='failed', error=f'{type(exc).__name__}: {exc}')
        LOG.error('%s', report['error'])
    finally:
        if client:
            client.close()
    report['finished_at'] = datetime.now(timezone.utc).isoformat()
    if args.report:
        try:
            write_report(args.report, report)
        except Exception as exc:
            report.update(status='failed', report_error=f'{type(exc).__name__}: {exc}')
    print(json.dumps(report, ensure_ascii=False, allow_nan=False))
    return 1 if report['status'] == 'failed' else 0

if __name__ == '__main__':
    raise SystemExit(main())
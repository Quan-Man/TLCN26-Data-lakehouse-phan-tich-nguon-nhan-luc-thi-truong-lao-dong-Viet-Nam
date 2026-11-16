from __future__ import annotations

import argparse
from base64 import b64encode
from http.client import HTTPConnection, HTTPSConnection
import json
import os
from pathlib import Path
import re
from urllib.parse import urlsplit

DEFAULT_CONFIG = Path(__file__).resolve().parents[2] / 'configs' / 'publication.json'
SHARED_TABLES = ('dim_date', 'dim_location', 'dim_skill')
JOB_TABLES = ('dim_company', 'fact_job_posting', 'fact_job_skill', 'bridge_job_location')
TABLES = SHARED_TABLES + JOB_TABLES


class TrinoSQL:
    def __init__(self):
        scheme = os.getenv('TRINO_HTTP_SCHEME', 'http')
        user = os.getenv('TRINO_USER', 'publisher')
        password = os.getenv('TRINO_PASSWORD')
        if scheme not in ('http', 'https'):
            raise ValueError('TRINO_HTTP_SCHEME must be http or https')
        if password and scheme != 'https':
            raise ValueError('Set TRINO_HTTP_SCHEME=https when using TRINO_PASSWORD')
        client = HTTPSConnection if scheme == 'https' else HTTPConnection
        self.connection = client(
            os.getenv('TRINO_HOST', 'trino'), int(os.getenv('TRINO_PORT', '8080')), timeout=60
        )
        self.headers = {'X-Trino-User': user, 'Content-Type': 'text/plain; charset=utf-8'}
        if password:
            token = b64encode(f'{user}:{password}'.encode('utf-8')).decode('ascii')
            self.headers['Authorization'] = f'Basic {token}'

    def query(self, sql):
        method, target, body = 'POST', '/v1/statement', sql.encode('utf-8')
        rows = []
        while True:
            self.connection.request(method, target, body=body, headers=self.headers)
            with self.connection.getresponse() as response:
                content = response.read()
                if response.status != 200:
                    raise RuntimeError(f'Trino HTTP {response.status} {response.reason}')
            page = json.loads(content)
            if error := page.get('error'):
                raise RuntimeError(f"{error.get('errorName', 'Trino error')}: {error['message']}")
            rows.extend(page.get('data') or [])
            if not (next_uri := page.get('nextUri')):
                return rows
            # Poll the configured coordinator even when it advertises a different host name.
            next_page = urlsplit(next_uri)
            target = next_page.path
            if not target.startswith('/v1/statement/'):
                raise ValueError('Unexpected Trino continuation path')
            if next_page.query:
                target += '?' + next_page.query
            method, body = 'GET', None

    def close(self):
        self.connection.close()


def sql_literal(value):
    return "'" + value.replace("'", "''") + "'"


def register_jobs(config, query):
    catalog = config.get('trino', {}).get('catalog', 'delta')
    if not re.fullmatch(r'[a-z][a-z0-9_]{0,100}', catalog):
        raise ValueError('Invalid Trino catalog')
    namespace = f'"{catalog}"."gold_jobs"'
    paths = {}
    for name in TABLES:
        shared = name in SHARED_TABLES
        variable = 'SHARED_GOLD_ROOT' if shared else 'JOBS_GOLD_ROOT'
        folder = 'shared' if shared else 'jobs'
        root = os.getenv(variable)
        spec = config.get('tables', {}).get(name, {})
        path = f"{root.rstrip('/')}/{name}" if root else spec.get('path', f's3a://gold/{folder}/{name}')
        if not path.startswith(('s3://', 's3a://')):
            raise ValueError(f'{name}: expected a MinIO path using s3:// or s3a://')
        paths[name] = path.replace('s3a://', 's3://', 1).rstrip('/')

    query(f'CREATE SCHEMA IF NOT EXISTS {namespace}')
    for name, path in paths.items():
        matches = query(
            f'SELECT table_type FROM "{catalog}".information_schema.tables '
            f"WHERE table_schema = 'gold_jobs' AND table_name = {sql_literal(name)}"
        )
        if not matches:
            query(
                f'CALL "{catalog}".system.register_table('
                f"schema_name => 'gold_jobs', table_name => {sql_literal(name)}, "
                f'table_location => {sql_literal(path)})'
            )
        elif len(matches) != 1 or matches[0][0] != 'BASE TABLE':
            raise ValueError(f'{namespace}.{name}: existing object is not a registered table')
        ddl = query(f'SHOW CREATE TABLE {namespace}.{name}')[0][0]
        locations = re.findall(r"\blocation\s*=\s*'((?:''|[^'])*)'", ddl, re.IGNORECASE)
        if len(locations) != 1 or locations[0].replace("''", "'").rstrip('/') != path:
            raise ValueError(f'{namespace}.{name}: existing table points to a different path; expected {path}')
        query(
            f'CALL "{catalog}".system.flush_metadata_cache('
            f"schema_name => 'gold_jobs', table_name => {sql_literal(name)})"
        )

    rows = {
        name: query(f'SELECT count(*) FROM {namespace}.{name}')[0][0]
        for name in ('fact_job_posting', 'fact_job_skill')
    }
    return {'catalog': catalog, 'schema': 'gold_jobs', 'registered': list(paths), 'rows': rows}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, help='Optional publication table path configuration')
    args = parser.parse_args()
    config_path = args.config
    if config_path is None and DEFAULT_CONFIG.is_file():
        config_path = DEFAULT_CONFIG
    config = json.loads(config_path.read_text(encoding='utf-8-sig')) if config_path else {}
    client = TrinoSQL()
    try:
        report = register_jobs(config, client.query)
    finally:
        client.close()
    print(json.dumps(report, ensure_ascii=False))


if __name__ == '__main__':
    main()

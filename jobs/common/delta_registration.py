"""Idempotent registration with explicit metadata verification."""
import re
from .trino_client import quote_identifier as ident, quote_literal as literal


def register_table(client, table):
    schema, name, location = table['trino_schema'], table['name'], table['delta_location']
    qualified = f'delta.{ident(schema)}.{ident(name)}'
    query = ("SELECT table_type FROM delta.information_schema.tables WHERE table_schema = "
             + literal(schema) + " AND table_name = " + literal(name))
    client.execute(f'CREATE SCHEMA IF NOT EXISTS delta.{ident(schema)}', operation=f'create schema {schema}')
    rows = client.execute(query, operation=f'lookup {schema}.{name}').rows
    if len(rows) > 1 or (rows and rows[0][0] != 'BASE TABLE'):
        raise ValueError(f'{schema}.{name}: existing object is not a unique base table')
    created = not rows
    if created:
        # Failure is propagated: permissions, invalid Delta logs, and races are not success.
        client.execute('CALL delta.system.register_table(schema_name => ' + literal(schema) +
            ', table_name => ' + literal(name) + ', table_location => ' + literal(location) + ')',
            operation=f'register {schema}.{name}')
        rows = client.execute(query, operation=f'verify existence {schema}.{name}').rows
        if len(rows) != 1 or rows[0][0] != 'BASE TABLE':
            raise ValueError(f'{schema}.{name}: registered table is not visible')
    ddl = client.execute('SHOW CREATE TABLE ' + qualified, operation=f'inspect {schema}.{name}').rows
    if len(ddl) != 1 or not ddl[0] or not isinstance(ddl[0][0], str):
        raise ValueError(f'{schema}.{name}: invalid table metadata')
    locations = re.findall(r"\blocation\s*=\s*'((?:''|[^'])*)'", ddl[0][0], re.IGNORECASE)
    if len(locations) != 1 or locations[0].replace("''", "'").rstrip('/') != location.rstrip('/'):
        raise ValueError(f'{schema}.{name}: Delta location does not match catalog')
    columns = client.execute('SELECT column_name FROM delta.information_schema.columns WHERE table_schema = '
        + literal(schema) + ' AND table_name = ' + literal(name), operation=f'inspect columns {schema}.{name}').rows
    actual = {row[0] for row in columns}
    expected = set(table['primary_key']) | set(table.get('partition_by', []))
    if not actual or expected - actual:
        raise ValueError(f'{schema}.{name}: missing catalog columns: {sorted(expected - actual)}')
    return 'registered' if created else 'verified'

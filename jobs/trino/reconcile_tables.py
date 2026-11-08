"""Compare Trino's registered Delta tables with configs/tables.yml."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT / "jobs") not in sys.path:
    sys.path.insert(0, str(ROOT / "jobs"))

from common.table_catalog import tables_in_publish_order
from common.trino_client import TrinoClient, quote_literal


def main() -> int:
    catalog = tables_in_publish_order()
    client = TrinoClient(catalog="delta", schema="default")
    client.wait_until_ready()
    schemas = sorted({table["trino_schema"] for table in catalog})
    schema_values = ", ".join(quote_literal(schema) for schema in schemas)
    result = client.execute(
        "SELECT table_schema, table_name FROM delta.information_schema.tables "
        f"WHERE table_schema IN ({schema_values})",
        operation="reconcile table catalog",
    )
    registered = {(row[0], row[1]) for row in result.rows}
    expected = {(table["trino_schema"], table["name"]) for table in catalog}
    missing = sorted(expected - registered)
    unexpected = sorted(registered - expected)
    for schema, name in sorted(expected):
        status = "PASS" if (schema, name) in registered else "FAIL"
        print(f"[{status}] delta.{schema}.{name}")
    if unexpected:
        print("Unexpected registered tables: " + ", ".join(f"{s}.{n}" for s, n in unexpected))
    print(f"Trino catalog reconciliation: {len(expected) - len(missing)}/{len(expected)} expected tables registered")
    return 1 if missing else 0


if __name__ == "__main__":
    raise SystemExit(main())

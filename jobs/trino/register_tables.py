"""Register published Delta locations in Trino from configs/tables.yml."""
from __future__ import annotations

import sys
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT / "jobs") not in sys.path:
    sys.path.insert(0, str(ROOT / "jobs"))

from common.table_catalog import tables_in_publish_order
from common.trino_client import TrinoClient
from common.delta_registration import register_table
from common.spark_session import create_spark_session
from common.delta_io import delta_location
from dq.check_delta import check as check_delta


def main() -> int:
    spark = create_spark_session('dq-before-trino-publish')
    try:
        return publish(spark)
    finally:
        spark.stop()


def publish(spark) -> int:
    from delta.tables import DeltaTable
    report = check_delta(spark)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    if not report['publish_allowed']:
        return 1
    client = TrinoClient(catalog="delta", schema="default")
    client.wait_until_ready()
    for table in tables_in_publish_order():
        current = int(DeltaTable.forPath(spark, delta_location(table['name'])).history(1).first()['version'])
        if current != report['delta_versions'][table['name']]:
            raise RuntimeError(f"Delta changed after DQ: {table['name']}; rerun publish")
        status = register_table(client, table)
        print(f"{status}: delta.{table['trino_schema']}.{table['name']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

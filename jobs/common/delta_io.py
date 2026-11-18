"""Catalog-governed Delta reads and writes with explicit, table-specific policies."""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
import math
from numbers import Integral, Real

from .schema_utils import quoted_col, validate_primary_key
from .settings import Settings
from .table_catalog import table_spec


def delta_location(table_name: str) -> str:
    spec = table_spec(table_name)
    location = spec["delta_location"]
    if not location.startswith("s3://"):
        raise ValueError(f"Table {table_name!r} has an invalid Delta location")
    return "s3a://" + location[len("s3://"):]


def read_delta(spark, table_name: str, *, version: int | None = None,
               settings: Settings | None = None):
    """Read a registered table from the location in configs/tables.yml."""
    settings = settings or Settings.from_env(require_minio=True)
    settings.require_minio_credentials()
    reader = spark.read.format("delta")
    if version is not None:
        if type(version) is not int or version < 0:
            raise ValueError("version must be an integer >= 0")
        reader = reader.option("versionAsOf", version)
    return reader.load(delta_location(table_name, settings))


def _sql_literal(value, spark_type) -> str:
    from pyspark.sql.types import (BooleanType, DateType, DecimalType, DoubleType,
                                   FloatType, IntegerType, LongType, ShortType,
                                   StringType, TimestampType)
    if value is None:
        raise ValueError("NULL is not an allowed partition scope value")
    if isinstance(spark_type, StringType):
        if not isinstance(value, str):
            raise TypeError("Partition scope value must be a string")
        return "'" + value.replace("'", "''") + "'"
    if isinstance(spark_type, BooleanType):
        if type(value) is not bool:
            raise TypeError("Partition scope value must be boolean")
        return "TRUE" if value else "FALSE"
    if isinstance(spark_type, (ShortType, IntegerType, LongType)):
        if not isinstance(value, Integral) or isinstance(value, bool):
            raise TypeError("Partition scope value must be an integer")
        return str(int(value))
    if isinstance(spark_type, (FloatType, DoubleType, DecimalType)):
        if not isinstance(value, (Real, Decimal)) or isinstance(value, bool):
            raise TypeError("Partition scope value must be numeric")
        if isinstance(value, float) and not math.isfinite(value):
            raise ValueError("Partition scope value must be finite")
        return format(value, "f") if isinstance(value, Decimal) else repr(float(value))
    if isinstance(spark_type, DateType):
        if isinstance(value, datetime):
            value = value.date()
        if isinstance(value, date):
            text = value.isoformat()
        elif isinstance(value, str):
            text = value
        else:
            raise TypeError("Date partition scope must be a date or ISO date string")
        return "DATE '" + text.replace("'", "''") + "'"
    if isinstance(spark_type, TimestampType):
        if isinstance(value, datetime):
            text = value.isoformat(sep=" ")
        elif isinstance(value, str):
            text = value
        else:
            raise TypeError("Timestamp partition scope must be datetime or string")
        return "TIMESTAMP '" + text.replace("'", "''") + "'"
    raise TypeError(f"Unsupported Delta partition type: {spark_type.simpleString()}")


def _partition_scope(df, spec: dict, scope: dict) -> tuple[str, object]:
    from pyspark.sql import functions as F

    partition_columns = tuple(spec.get("partition_by") or ())
    if not partition_columns:
        raise ValueError(f"Table {spec['name']!r} is not partitioned")
    if not isinstance(scope, dict) or set(scope) != set(partition_columns):
        raise ValueError(
            f"Partition scope for {spec['name']!r} must specify exactly: {list(partition_columns)}"
        )
    missing = sorted(set(partition_columns) - set(df.columns))
    if missing:
        raise ValueError(f"Partition columns missing from input: {missing}")
    fields = {field.name: field.dataType for field in df.schema.fields}
    conditions = []
    predicates = []
    for column in partition_columns:
        raw_values = scope[column]
        if isinstance(raw_values, set):
            values = sorted(raw_values, key=repr)
        else:
            values = list(raw_values) if isinstance(raw_values, (list, tuple)) else [raw_values]
        if not values:
            raise ValueError(f"Partition scope for {column!r} has no values")
        if any(value is None for value in values):
            raise ValueError(f"Partition scope for {column!r} cannot include NULL")
        unique_values = list(dict.fromkeys(values))
        col = F.col(quoted_col(column))
        conditions.append(col.isin(*unique_values))
        literal_values = [_sql_literal(value, fields[column]) for value in unique_values]
        if len(literal_values) == 1:
            predicates.append(f"{quoted_col(column)} = {literal_values[0]}")
        else:
            predicates.append(f"{quoted_col(column)} IN ({', '.join(literal_values)})")
    in_scope = conditions[0]
    for condition in conditions[1:]:
        in_scope = in_scope & condition
    if df.filter(~F.coalesce(in_scope, F.lit(False))).limit(1).count():
        raise ValueError("Input rows contain values outside the declared partition replacement scope")
    return " AND ".join(predicates), in_scope


def _assert_matching_schema(source, target, table_name: str) -> None:
    source_types = {field.name: field.dataType for field in source.schema.fields}
    target_types = {field.name: field.dataType for field in target.schema.fields}
    if source_types != target_types:
        raise ValueError(f"Input and Delta schemas differ for {table_name!r}; migrate schema explicitly")


def _assert_matching_partitions(target, spec: dict) -> None:
    expected = list(spec.get("partition_by") or ())
    row = target.detail().select("partitionColumns").first()
    actual = list(row["partitionColumns"] or [])
    if actual != expected:
        raise ValueError(
            f"Delta partition layout for {spec['name']!r} is {actual}; catalog requires {expected}"
        )


def _write_new(df, path: str, partition_by: tuple[str, ...]) -> None:
    writer = df.write.format("delta").mode("errorifexists")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)


def write_delta(spark, table_name: str, df, *, partition_scope: dict | None = None,
                settings: Settings | None = None, audit=None) -> dict:
    """Write using the table's configured append/merge/replace_partitions policy.

    A replace_partitions table requires an explicit scope for every partition
    column. No API path performs an unbounded overwrite.
    """
    spec = table_spec(table_name)
    settings = settings or Settings.from_env(require_minio=True)
    settings.require_minio_credentials()
    path = delta_location(table_name)
    policy = spec["write_policy"]
    keys = tuple(spec["primary_key"])
    partition_by = tuple(spec.get("partition_by") or ())
    if policy == "replace_partitions" and partition_scope is None:
        raise ValueError(f"Table {table_name!r} requires an explicit partition_scope")
    if policy != "replace_partitions" and partition_scope is not None:
        raise ValueError(f"Table {table_name!r} uses {policy}; partition_scope is not allowed")

    validate_primary_key(df, keys)
    missing_partitions = sorted(set(partition_by) - set(df.columns))
    if missing_partitions:
        raise ValueError(f"Partition columns missing from {table_name!r} input: {missing_partitions}")

    from delta.tables import DeltaTable

    def result(action, **extra):
        version = DeltaTable.forPath(spark, path).history(1).select("version").first()["version"]
        if audit is not None:
            audit.record_output(table_name, delta_version=int(version), location=path)
        return {"table": table_name, "policy": policy, "action": action,
                "delta_version": int(version), "path": path, **extra}

    if not DeltaTable.isDeltaTable(spark, path):
        if policy == "replace_partitions":
            _partition_scope(df, spec, partition_scope)
        _write_new(df, path, partition_by)
        return result("created")

    target = DeltaTable.forPath(spark, path)
    target_df = target.toDF()
    _assert_matching_schema(df, target_df, table_name)
    _assert_matching_partitions(target, spec)

    if policy == "append":
        validate_primary_key(target_df, keys)
        overlap = (df.select(*keys).alias("source")
                   .join(target_df.select(*keys).alias("target"), list(keys), "inner")
                   .limit(1).count())
        if overlap:
            raise ValueError(f"Append to {table_name!r} would duplicate existing primary keys")
        df.write.format("delta").mode("append").save(path)
        return result("appended")

    if policy == "merge":
        validate_primary_key(target_df, keys)
        condition = " AND ".join(
            f"t.{quoted_col(key)} = s.{quoted_col(key)}" for key in keys
        )
        (target.alias("t").merge(df.alias("s"), condition)
         .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
        return result("merged")

    if policy == "replace_partitions":
        replace_where, _ = _partition_scope(df, spec, partition_scope)
        writer = (df.write.format("delta").mode("overwrite")
                  .option("replaceWhere", replace_where))
        writer.save(path)
        return result("replaced_partitions", partition_scope=partition_scope,
                      replace_where=replace_where)

    raise ValueError(f"Unsupported write policy {policy!r} for {table_name!r}")

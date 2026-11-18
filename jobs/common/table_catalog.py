"""Shared table definitions loaded from configs/tables.yml."""
from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from .settings import Settings, load_yaml

@lru_cache(maxsize=4)
def load_table_catalog(path: str | Path | None = None) -> dict[str, dict]:
    settings = Settings.from_env(validate_paths=True,
                                 required_configs=("tables.yml",) if path is None else ())
    config = load_yaml(path if path is not None else settings.config_path("tables.yml"))
    entries = config.get("tables") if isinstance(config, dict) else None
    if not isinstance(entries, list):
        raise ValueError("configs/tables.yml must contain a tables list")
    result = {}
    orders = set()
    for entry in entries:
        if not isinstance(entry, dict):
            raise ValueError("Each table catalog entry must be a mapping")
        name = entry.get("name")
        if not isinstance(name, str) or not name or name in result:
            raise ValueError(f"Invalid or duplicate table name: {name!r}")
        layer = entry.get("layer")
        if layer not in {"bronze", "silver", "gold"} or entry.get("trino_schema") != layer:
            raise ValueError(f"Invalid layer/schema for table {name!r}")
        location = entry.get("delta_location")
        if not isinstance(location, str) or not location.startswith("s3://"):
            raise ValueError(f"Invalid Delta location for table {name!r}")
        key = entry.get("primary_key")
        if (not isinstance(key, list) or not key
                or any(not isinstance(k, str) or not k for k in key)
                or len(set(key)) != len(key)):
            raise ValueError(f"Table {name!r} needs a non-empty primary_key")
        partition = entry.get("partition_by", [])
        order = entry.get("publish_order")
        if (not isinstance(partition, list)
                or any(not isinstance(column, str) or not column for column in partition)
                or len(set(partition)) != len(partition)
                or type(order) is not int or order in orders):
            raise ValueError(f"Invalid partition_by or duplicate publish_order for {name!r}")
        policy = entry.get("write_policy")
        if policy not in {"append", "merge", "replace_partitions"}:
            raise ValueError(f"Table {name!r} needs write_policy append, merge, or replace_partitions")
        if policy == "replace_partitions" and not partition:
            raise ValueError(f"Table {name!r} needs partition_by for replace_partitions")
        orders.add(order)
        result[name] = entry
    return result


def table_spec(name: str) -> dict:
    try:
        return load_table_catalog()[name]
    except KeyError as exc:
        raise KeyError(f"Table {name!r} is not in configs/tables.yml") from exc


def tables_in_publish_order() -> list[dict]:
    return sorted(load_table_catalog().values(), key=lambda entry: entry["publish_order"])

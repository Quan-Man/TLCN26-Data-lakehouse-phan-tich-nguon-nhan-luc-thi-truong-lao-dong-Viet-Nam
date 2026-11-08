"""Schema-driven file reads and idempotent raw-file archival/ingestion manifests."""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import tempfile
from typing import Iterable

from .settings import Settings, load_config, load_json


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _canonical_json(value) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
                      allow_nan=False).encode("utf-8")


def _safe_dataset(dataset: str) -> str:
    if not isinstance(dataset, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", dataset):
        raise ValueError("dataset must be a safe non-empty identifier")
    if ".." in dataset:
        raise ValueError("dataset must not contain '..'")
    return dataset


def _logical_source_id(path: Path, input_root: Path | None) -> str:
    if input_root is not None:
        try:
            return path.relative_to(input_root).as_posix()
        except ValueError:
            pass  # Custom --input paths outside the project are identified by basename only.
    return path.name


def _atomic_json(path: Path, value) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=".pending_", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as stream:
            json.dump(value, stream, ensure_ascii=False, indent=2, allow_nan=False)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp_name, path)
    except Exception:
        try:
            os.unlink(temp_name)
        except FileNotFoundError:
            pass
        raise


class Storage:
    """Local ingestion state and immutable copies of original source files.

    ``prepare_ingestion`` is read-only. Call ``commit_ingestion`` only after the
    downstream write/upload has succeeded. Identical retries receive the same
    run ID, and committed unchanged files are excluded from later plans.
    """

    def __init__(self, root: str | Path):
        self.root = Path(root).expanduser().resolve()
        self.archive_root = self.root / "archive"
        self.manifest_root = self.root / "manifests"

    def _committed_state(self, dataset: str) -> dict[str, str]:
        state: dict[str, str] = {}
        manifests = self.manifest_root / dataset
        if not manifests.is_dir():
            return state
        paths = sorted(manifests.glob("*.json"))
        entries = []
        for path in paths:
            try:
                manifest = json.loads(path.read_text(encoding="utf-8-sig"))
            except (OSError, json.JSONDecodeError) as exc:
                raise ValueError(f"Invalid committed ingestion manifest: {path.name}") from exc
            if manifest.get("dataset") != dataset or manifest.get("status") != "completed":
                raise ValueError(f"Manifest is not a completed {dataset} ingestion: {path.name}")
            entries.append((manifest.get("created_at", ""), path.name, manifest))
        for _, _, manifest in sorted(entries):
            for item in manifest.get("files", []):
                if item.get("status") in {"new", "changed"}:
                    state[item["source_file_id"]] = item["sha256"]
        return state

    def prepare_ingestion(self, files: Iterable[str | Path], *, dataset: str,
                          input_root: str | Path | None = None) -> dict:
        dataset = _safe_dataset(dataset)
        root = Path(input_root).expanduser().resolve() if input_root is not None else None
        paths = [Path(item).expanduser().resolve(strict=True) for item in files]
        if not paths:
            raise ValueError("At least one input file is required")
        if any(not path.is_file() for path in paths):
            raise ValueError("Every ingestion input must be a regular file")
        if len(paths) != len(set(paths)):
            raise ValueError("An input file was provided more than once")

        state = self._committed_state(dataset)
        seen_ids = set()
        records = []
        for path in paths:
            source_id = _logical_source_id(path, root)
            if source_id in seen_ids:
                raise ValueError(f"Duplicate logical source file ID: {source_id!r}")
            seen_ids.add(source_id)
            digest = _sha256_file(path)
            previous = state.get(source_id)
            status = "new" if previous is None else "unchanged" if previous == digest else "changed"
            records.append({"_local_path": str(path), "source_file_id": source_id,
                            "source_file": path.name, "sha256": digest,
                            "size_bytes": path.stat().st_size, "status": status})
        pending = [item for item in records if item["status"] in {"new", "changed"}]
        run_id = hashlib.sha256(_canonical_json(
            [dataset, sorted((item["source_file_id"], item["sha256"]) for item in pending)]
        )).hexdigest() if pending else None
        return {"dataset": dataset, "run_id": run_id, "files": records,
                "pending_files": pending, "unchanged_count": len(records) - len(pending),
                "has_changes": bool(pending)}

    def commit_ingestion(self, plan: dict, *, metadata: dict | None = None) -> dict:
        dataset = _safe_dataset(plan.get("dataset"))
        run_id = plan.get("run_id")
        pending = plan.get("pending_files")
        if not run_id or not isinstance(pending, list) or not pending:
            raise ValueError("Cannot commit an empty ingestion plan")
        if not re.fullmatch(r"[0-9a-f]{64}", run_id):
            raise ValueError("Invalid ingestion run ID")
        manifest_path = self.manifest_root / dataset / f"{run_id}.json"
        if manifest_path.is_file():
            existing = json.loads(manifest_path.read_text(encoding="utf-8-sig"))
            if existing.get("dataset") != dataset or existing.get("run_id") != run_id:
                raise ValueError(f"Conflicting ingestion manifest for run {run_id}")
            return existing

        committed_files = []
        for item in pending:
            source_path = Path(item["_local_path"]).resolve(strict=True)
            actual_sha = _sha256_file(source_path)
            if actual_sha != item["sha256"]:
                raise ValueError(f"Input changed after planning; retry from a fresh plan: {item['source_file']}")
            archive_path = (self.archive_root / dataset / actual_sha[:2] / actual_sha /
                            Path(item["source_file"]).name)
            archive_path.parent.mkdir(parents=True, exist_ok=True)
            if archive_path.exists():
                if _sha256_file(archive_path) != actual_sha:
                    raise ValueError(f"Archived content checksum mismatch: {actual_sha}")
            else:
                fd, temp_name = tempfile.mkstemp(prefix=".pending_", dir=archive_path.parent)
                temp_path = Path(temp_name)
                try:
                    with source_path.open("rb") as src, os.fdopen(fd, "wb") as dst:
                        shutil.copyfileobj(src, dst, length=1024 * 1024)
                        dst.flush()
                        os.fsync(dst.fileno())
                    if _sha256_file(temp_path) != actual_sha:
                        raise ValueError(f"Source checksum changed while copying: {item['source_file']}")
                    os.replace(temp_path, archive_path)
                except Exception:
                    temp_path.unlink(missing_ok=True)
                    raise
            committed_files.append({
                "source_file_id": item["source_file_id"], "source_file": item["source_file"],
                "sha256": actual_sha, "size_bytes": item["size_bytes"],
                "status": item["status"],
                "archive_path": archive_path.relative_to(self.root).as_posix(),
            })

        manifest = {
            "format_version": 1, "dataset": dataset, "run_id": run_id,
            "status": "completed", "created_at": datetime.now(timezone.utc).isoformat(),
            "file_count": len(committed_files), "files": committed_files,
            "metadata": metadata or {},
        }
        _atomic_json(manifest_path, manifest)
        return manifest

    def verify_manifest(self, manifest: dict | str | Path) -> dict:
        """Verify every archived original against its manifest checksum."""
        if isinstance(manifest, (str, Path)):
            path = Path(manifest)
            if not path.is_absolute():
                path = self.root / path
            value = json.loads(path.read_text(encoding="utf-8-sig"))
        else:
            value = manifest
        errors = []
        for item in value.get("files", []):
            archive = (self.root / item["archive_path"]).resolve()
            if not archive.is_relative_to(self.root):
                errors.append(f"archive path escapes storage root: {item.get('source_file_id')}")
            elif not archive.is_file():
                errors.append(f"missing archived file: {item.get('source_file_id')}")
            elif _sha256_file(archive) != item.get("sha256"):
                errors.append(f"checksum mismatch: {item.get('source_file_id')}")
        if errors:
            raise ValueError("Manifest verification failed: " + "; ".join(errors))
        return {"verified_files": len(value.get("files", [])), "run_id": value.get("run_id")}


def _resolve_spark_schema(schema, *, table_name=None):
    from pyspark.sql.types import StructType

    value = schema
    if isinstance(value, (str, Path)):
        raw_path = str(value)
        config_path, separator, selector = raw_path.partition("#")
        path = Path(config_path)
        if not path.is_absolute():
            path = Settings.from_env(validate_paths=True).resolve_path(path)
        value = load_json(path) if path.suffix.lower() == ".json" else load_config(path)
        if separator:
            parts = selector.split(".")
            if (len(parts) >= 3 and parts[0] == "tables" and isinstance(value, dict)
                    and isinstance(value.get("tables"), dict)):
                candidate = next((".".join(parts[1:index]) for index in range(len(parts), 1, -1)
                                  if ".".join(parts[1:index]) in value["tables"]), None)
                if candidate is not None:
                    value = value["tables"][candidate]
                    for part in parts[1 + len(candidate.split(".")):]:
                        value = value[part]
                else:
                    for part in parts:
                        value = value[part]
            else:
                for part in parts:
                    value = value[part]
    if isinstance(value, StructType):
        return value
    if isinstance(value, dict) and "tables" in value:
        if table_name is None:
            raise ValueError("table_name is required for a schema file containing multiple tables")
        try:
            value = value["tables"][table_name]["spark_schema"]
        except (KeyError, TypeError) as exc:
            raise ValueError(f"No Spark schema for table {table_name!r}") from exc
    elif isinstance(value, dict) and "spark_schema" in value:
        value = value["spark_schema"]
    if not isinstance(value, dict) or value.get("type") != "struct":
        raise ValueError("schema must be a Spark StructType, StructType JSON, or schema file path")
    return StructType.fromJson(value)


def read_dataset(spark, path: str | Path, *, format: str, schema=None, table_name=None,
                 delimiter=",", encoding="UTF-8", header=True, multiline=False):
    """Read CSV, JSON/JSONL, or Parquet using a supplied Spark schema when present."""
    kind = format.strip().lower()
    if kind in {"ndjson", "jsonl"}:
        kind = "json"
        multiline = False
    if kind not in {"csv", "json", "parquet"}:
        raise ValueError("format must be csv, json/jsonl, or parquet")
    reader = spark.read
    if schema is not None:
        reader = reader.schema(_resolve_spark_schema(schema, table_name=table_name))
    if kind == "csv":
        reader = (reader.option("header", str(header).lower())
                  .option("sep", delimiter).option("encoding", encoding)
                  .option("inferSchema", "false").option("mode", "FAILFAST")
                  .option("multiLine", "true").option("quote", '"').option("escape", '"'))
        if schema is not None:
            reader = reader.option("enforceSchema", "false")
        return reader.csv(str(path))
    if kind == "json":
        return reader.option("multiLine", str(multiline).lower()).option("mode", "FAILFAST").json(str(path))
    return reader.parquet(str(path))


def read_csv(spark, path, *, schema=None, delimiter=",", encoding="UTF-8", header=True,
             table_name=None):
    return read_dataset(spark, path, format="csv", schema=schema, table_name=table_name,
                        delimiter=delimiter, encoding=encoding, header=header)


def read_json(spark, path, *, schema=None, multiline=False, table_name=None):
    return read_dataset(spark, path, format="json", schema=schema, table_name=table_name,
                        multiline=multiline)


def read_parquet(spark, path, *, schema=None, table_name=None):
    return read_dataset(spark, path, format="parquet", schema=schema, table_name=table_name)

#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import glob
import json
import logging
import os
import re
import shutil
import sys
import tempfile
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import uuid4

if TYPE_CHECKING:
    from common.settings import Settings

LOGGER = logging.getLogger("bronze.job_posting")

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT = PROJECT_ROOT / "data/bronze/job_posting"
if str(PROJECT_ROOT / "jobs") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "jobs"))


def configured_inputs(config_path: Path, source_name: str | None = None) -> list[Path]:
    """Resolve source globs from configs/sources.yml relative to the project root."""
    from common.settings import load_yaml
    config = load_yaml(config_path)
    sources = config.get("sources") if isinstance(config, dict) else None
    if not isinstance(sources, list):
        raise ValueError("Cấu hình cần có danh sách sources")
    selected = [source for source in sources
                if source.get("domain") == "job_market"
                and (source_name is None or source.get("name") == source_name)]
    if source_name is not None and not selected:
        raise ValueError(f"Không tìm thấy nguồn job_market: {source_name}")
    paths = []
    for source in selected:
        if source.get("format") != "json":
            raise ValueError(f"Nguồn {source.get('name')} không phải JSON")
        pattern = str(PROJECT_ROOT / source["input_glob"])
        matches = sorted(Path(path) for path in glob.glob(pattern))
        if not matches:
            raise FileNotFoundError(f"Không tìm thấy file cho nguồn {source['name']}: {source['input_glob']}")
        paths.extend(matches)
    if not paths:
        raise ValueError("Không có nguồn job_market trong cấu hình")
    return paths

def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def make_spark(master: str | None = None, partitions: int = 1):
    from pyspark import SparkConf
    from pyspark.sql import SparkSession
    master = master or os.getenv('SPARK_MASTER') or SparkConf().get('spark.master', 'local[1]')
    if not re.fullmatch(r'local(?:\[(?:\*|[1-9]\d*)(?:,[1-9]\d*)?\])?', master):
        raise ValueError('This loader uses local staging files; use Spark local mode, e.g. local[1]')
    if partitions < 1:
        raise ValueError('--partitions must be positive')
    builder = (SparkSession.builder.appName('bronze-job-posting').master(master)
               .config('spark.sql.session.timeZone', 'UTC')
               .config('spark.sql.shuffle.partitions', str(partitions))
               .config('spark.default.parallelism', str(partitions))
               .config('spark.sql.adaptive.enabled', 'false')
               .config('spark.ui.enabled', 'false'))
    address = os.getenv('SPARK_LOCAL_IP', '127.0.0.1')
    spark = (builder.config('spark.driver.bindAddress', address)
             .config('spark.driver.host', address).getOrCreate())
    spark.sparkContext.setLogLevel(os.getenv('SPARK_LOG_LEVEL', 'WARN'))
    return spark

def json_text(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":"))

def unique_keys(pairs: list[tuple]) -> dict:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"JSON chứa key trùng: {key!r}")
        result[key] = value
    return result

def reject_constant(value: str) -> None:
    raise ValueError(f"Giá trị không hợp lệ trong JSON: {value}")

def read_records(raw_bytes: bytes, allow_control_chars: bool) -> tuple[list[dict], str]:
    text = raw_bytes.decode("utf-8-sig")
    options = {"object_pairs_hook": unique_keys, "parse_constant": reject_constant}
    parser_mode = "strict"
    try:
        rows = json.loads(text, **options)
    except json.JSONDecodeError as exc:
        if not allow_control_chars or not exc.msg.startswith("Invalid control character"):
            raise ValueError(
                f"JSON không hợp lệ: {exc}. Nếu lỗi là ký tự điều khiển trong chuỗi, "
                "có thể dùng --allow-control-chars; dữ liệu gốc vẫn được giữ nguyên."
            ) from exc
        rows = json.loads(text, strict=False, **options)
        parser_mode = "allow_control_chars"
    if not isinstance(rows, list):
        raise ValueError("Đầu vào phải là JSON array như hai file mẫu.")
    if not rows:
        raise ValueError("File không có bản ghi; dừng để phát hiện lỗi crawl sớm.")
    for number, row in enumerate(rows, start=1):
        if not isinstance(row, dict):
            raise ValueError(f"Bản ghi {number} không phải JSON object.")
        if not isinstance(row.get("source_name"), str) or not row["source_name"].strip():
            raise ValueError(f"Bản ghi {number} thiếu source_name hợp lệ.")
    return rows, parser_mode

def source_partition(source: str) -> str:
    value = source.strip().lower()
    if not re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,79}", value):
        raise ValueError(f"source_name không phù hợp làm partition: {source!r}")
    return value


def spark_envelopes(binary_rows, allow_control_chars: bool, run_id: str,
                    ingested_at: str, source_file: str):
    for binary in binary_rows:
        raw_bytes = bytes(binary['content'])
        rows, parser_mode = read_records(raw_bytes, allow_control_chars)
        source_names = {row['source_name'] for row in rows}
        if len(source_names) != 1:
            raise ValueError('each file must contain exactly one source_name')
        source = source_partition(next(iter(source_names)))
        digest = sha256(raw_bytes)
        for number, payload in enumerate(rows, start=1):
            canonical = json.dumps(payload, ensure_ascii=False, allow_nan=False,
                                   sort_keys=True, separators=(',', ':'))
            metadata = dict(source_name=source, source_file=source_file,
                            source_file_sha256=digest, row_number=number,
                            record_sha256=sha256(canonical.encode('utf-8')),
                            ingested_at=ingested_at, run_id=run_id, parser_mode=parser_mode)
            yield (source, digest, len(raw_bytes), parser_mode, number,
                   json_text({'_metadata': metadata, 'raw_payload': payload}))


def bronze_frame(spark, snapshot_path: Path, source_file: str,
                 allow_control_chars: bool, run_id: str, ingested_at: str):
    from pyspark.sql.types import StructType, StructField, StringType, LongType
    schema = StructType([
        StructField('source_name', StringType(), False),
        StructField('source_file_sha256', StringType(), False),
        StructField('source_size_bytes', LongType(), False),
        StructField('parser_mode', StringType(), False),
        StructField('row_number', LongType(), False),
        StructField('value', StringType(), False),
    ])
    binary = spark.read.format('binaryFile').load(str(snapshot_path)).select('content')
    records = binary.rdd.mapPartitions(lambda rows: spark_envelopes(
        rows, allow_control_chars, run_id, ingested_at, source_file))
    return spark.createDataFrame(records, schema=schema)

def ingest_local(inputs: list[Path], output: Path, allow_control_chars: bool = False,
                 run_id: str | None = None, *, spark=None,
                 master: str | None = None, partitions: int = 1) -> tuple[Path, dict]:
    from common.audit import AuditDataError
    from pyspark import StorageLevel
    from pyspark.sql import functions as F
    paths = [Path(path).resolve(strict=True) for path in inputs]
    if not paths:
        raise ValueError("Cần ít nhất một file đầu vào.")
    if len(paths) != len(set(paths)):
        raise ValueError("Một file đầu vào được truyền nhiều lần trong cùng batch.")
    now = datetime.now(timezone.utc)
    run_id = run_id or uuid4().hex
    output = Path(output).resolve()
    existing_runs = output.rglob(f"run_id={run_id}") if output.exists() else ()
    for existing in existing_runs:
        manifest_path = existing / "manifest.json"
        if existing.is_dir() and manifest_path.is_file():
            manifest = json.loads(manifest_path.read_text(encoding="utf-8-sig"))
            if manifest.get('status') != 'completed' or manifest.get('run_id') != run_id:
                raise ValueError('Existing Bronze manifest is incomplete or conflicts with run_id')
            if len(manifest.get('files', [])) != len(paths):
                raise ValueError('Existing run_id has a different input set')
            for path, item in zip(paths, manifest['files']):
                if path.name != item['source_file'] or file_sha256(path) != item['source_file_sha256']:
                    raise ValueError('Existing run_id has different input content')
                records_path = (existing / item['records_path']).resolve()
                if not records_path.is_relative_to(existing.resolve()):
                    raise ValueError('Unsafe records_path in existing manifest')
                if file_sha256(records_path) != item['records_sha256']:
                    raise ValueError('Existing Bronze records checksum mismatch')
            return existing, manifest
    ingested_at = now.isoformat()
    parent = output / f"ingest_date={now.date().isoformat()}"
    parent.mkdir(parents=True, exist_ok=True)
    final_dir = parent / f"run_id={run_id}"
    stage = Path(tempfile.mkdtemp(prefix=".pending_", dir=parent))
    manifest = {
        "dataset": "bronze.job_posting", "format_version": 1,
        "run_id": run_id, "ingested_at": ingested_at,
        "total_records": 0, "source_counts": {}, "files": [],
    }
    source_counts: Counter = Counter()
    owns_spark = spark is None
    try:
        spark = spark or make_spark(master=master, partitions=partitions)
        spark.sparkContext.addPyFile(str(Path(__file__).resolve()))
        for file_number, path in enumerate(paths, start=1):
            pending_input = stage / f'input_{file_number:04d}.json'
            shutil.copyfile(path, pending_input)
            frame = None
            try:
                frame = bronze_frame(spark, pending_input, path.name, allow_control_chars, run_id, ingested_at).persist(StorageLevel.DISK_ONLY)
                summary = frame.agg(
                    F.count('*').alias('record_count'),
                    F.first('source_name').alias('source_name'),
                    F.first('source_file_sha256').alias('source_file_sha256'),
                    F.first('source_size_bytes').alias('source_size_bytes'),
                    F.first('parser_mode').alias('parser_mode'),
                    F.min('row_number').alias('first_row'), F.max('row_number').alias('last_row'),
                ).first()
                count = summary['record_count']
                if count < 1 or summary['first_row'] != 1 or summary['last_row'] != count:
                    raise ValueError('Spark Bronze record count/row numbers are inconsistent')
                source, file_hash = summary['source_name'], summary['source_file_sha256']
                parser_mode = summary['parser_mode']
                relative_dir = Path(f'source_name={source}') / f'file_{file_number:04d}'
                destination = stage / relative_dir
                destination.mkdir(parents=True)
                spark_output = destination / '_spark_records'
                (frame.coalesce(1).sortWithinPartitions('row_number').select('value')
                 .write.mode('errorifexists').text(str(spark_output)))
                parts = sorted(spark_output.glob('part-*'))
                if len(parts) != 1:
                    raise ValueError(f'Expected one Spark output part, got {len(parts)}')
                records_path = destination / 'records.jsonl'
                parts[0].rename(records_path)
                shutil.rmtree(spark_output)
                written_rows = 0
                with records_path.open('rb') as stream:
                    for line in stream:
                        if not line.strip():
                            raise ValueError('Unexpected blank Bronze record')
                        written_rows += 1
                if written_rows != count:
                    raise ValueError('Written Bronze count differs from Spark DataFrame count')
                pending_input.rename(destination / 'original.json')
            except Exception as exc:
                raise AuditDataError(
                    f'Spark Bronze ingestion failed for {path.name} ({type(exc).__name__})'
                ) from exc
            finally:
                if frame is not None:
                    frame.unpersist(blocking=True)
            manifest["files"].append({
                "source_name": source, "source_file": path.name,
                "source_file_sha256": file_hash, "source_size_bytes": summary['source_size_bytes'],
                "record_count": count, "parser_mode": parser_mode,
                "original_path": (relative_dir / "original.json").as_posix(),
                "records_path": (relative_dir / "records.jsonl").as_posix(),
                "records_sha256": file_sha256(records_path),
            })
            source_counts[source] += count
            LOGGER.info('%s: %d records via PySpark; parser=%s', path.name, count, parser_mode)
        manifest["total_records"] = sum(source_counts.values())
        manifest["source_counts"] = dict(source_counts)
        manifest["status"] = "completed"
        (stage / "manifest.json").write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2, allow_nan=False) + "\n",
            encoding="utf-8",
        )
        stage.rename(final_dir)
    except Exception:
        shutil.rmtree(stage, ignore_errors=True)
        raise
    finally:
        if owns_spark and spark is not None:
            spark.stop()
    return final_dir, manifest

def upload_minio(run_dir: Path, output: Path, bucket: str, prefix: str,
                 create_bucket: bool = False, settings: Settings | None = None) -> str:
    from common.settings import Settings
    try:
        from minio import Minio
    except ImportError as exc:
        raise RuntimeError("Chưa có MinIO SDK. Cài: pip install minio") from exc
    settings = settings or Settings.from_env(require_minio=True)
    settings.require_minio_credentials()
    endpoint, secure = settings.minio_endpoint, settings.minio_secure
    prefix = prefix.strip("/")
    if not prefix or any(part in {"", ".", ".."} for part in prefix.split("/")):
        raise ValueError("Prefix MinIO không hợp lệ.")
    client = Minio(endpoint=endpoint, access_key=settings.minio_access_key,
                   secret_key=settings.minio_secret_key, secure=secure)
    if not client.bucket_exists(bucket_name=bucket):
        if not create_bucket:
            raise ValueError(f"Bucket {bucket!r} chưa có; tạo trước hoặc thêm --create-bucket.")
        client.make_bucket(bucket_name=bucket)
    base_key = prefix + "/" + run_dir.relative_to(Path(output).resolve()).as_posix()
    files = sorted(path for path in run_dir.rglob("*") if path.is_file() and path.name != "manifest.json")
    files.append(run_dir / "manifest.json")
    for path in files:
        object_name = base_key + "/" + path.relative_to(run_dir).as_posix()
        client.fput_object(bucket_name=bucket, object_name=object_name, file_path=str(path),
                           content_type="application/x-ndjson" if path.suffix == ".jsonl" else "application/json")
    return f"s3://{bucket}/{base_key}"

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="PySpark: nạp job_posting vào Bronze JSONL local và MinIO.")
    parser.add_argument(
        "--input",
        nargs="+",
        type=Path,
        default=None,
        help="Ghi đè các file đầu vào; mặc định resolve glob từ configs/sources.yml",
    )
    parser.add_argument("--config", type=Path, default=PROJECT_ROOT / "configs/sources.yml")
    parser.add_argument("--source", help="Chỉ nạp một nguồn theo name trong cấu hình")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument('--master', help='Spark local master; default SPARK_MASTER or spark-submit master or local[1]')
    parser.add_argument('--partitions', type=int, default=1, help='Local processing/shuffle partitions; default 1')

    json_mode = parser.add_mutually_exclusive_group()
    json_mode.add_argument(
        "--allow-control-chars",
        dest="allow_control_chars",
        action="store_true",
        help="Cho phép ký tự điều khiển chưa escape trong chuỗi JSON (mặc định)",
    )
    json_mode.add_argument(
        "--strict-json",
        dest="allow_control_chars",
        action="store_false",
        help="Chỉ chấp nhận JSON nghiêm ngặt",
    )

    upload_mode = parser.add_mutually_exclusive_group()
    upload_mode.add_argument(
        "--minio",
        dest="minio",
        action="store_true",
        help="Upload lên MinIO sau khi ghi local",
    )

    parser.set_defaults(allow_control_chars=True, minio=True)
    parser.add_argument("--bucket", default=os.getenv("MINIO_BUCKET", "bronze"))
    parser.add_argument("--prefix", default=os.getenv("MINIO_PREFIX", "job_posting"))
    parser.add_argument("--create-bucket", action="store_true")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    from common.settings import Settings
    from common.storage import Storage
    from common.audit import RunAudit
    common_root = PROJECT_ROOT / "jobs" / "common"
    audit = RunAudit("bronze.job_posting", code_files=[
        Path(__file__), common_root / "audit.py", common_root / "settings.py",
        common_root / "storage.py",
    ])
    audit.start()
    try:
        settings = Settings.from_env(require_minio=args.minio)
        inputs = args.input if args.input is not None else configured_inputs(args.config, args.source)
        audit.set_versions(mapping_files=[args.config])
        storage = Storage(PROJECT_ROOT / "data" / "ingestion_state" / "job_posting")
        plan = storage.prepare_ingestion(inputs, dataset="job_posting", input_root=PROJECT_ROOT)
        for item in plan["files"]:
            audit.add_input(item["source_file_id"], item["sha256"], size_bytes=item["size_bytes"], status=item["status"])
        if not plan["has_changes"]:
            audit.set_counts(read=0, valid=0, rejected=0, duplicate=0)
            audit.succeed()
            result = {"status": "unchanged", "ingested_files": 0,
                      "unchanged_files": plan["unchanged_count"], "minio_uri": None,
                      "audit_manifest": str(audit.path)}
            print(json.dumps(result, ensure_ascii=False, indent=2))
            return 0
        changed_inputs = [Path(item["_local_path"]) for item in plan["pending_files"]]
        run_dir, manifest = ingest_local(changed_inputs, args.output, args.allow_control_chars,
                                         run_id=plan["run_id"], master=args.master,
                                         partitions=args.partitions)
        LOGGER.info("Local hoàn tất: %s", run_dir)
        result = {"total_records": manifest["total_records"],
                  "source_counts": manifest["source_counts"], "manifest": str(run_dir / "manifest.json")}
        if args.minio:
            result["minio_uri"] = upload_minio(run_dir, args.output, args.bucket, args.prefix, args.create_bucket, settings=settings)
        archive_manifest = storage.commit_ingestion(plan, metadata={"bronze_manifest": str((run_dir / "manifest.json").relative_to(args.output.resolve()))})
        result["archive_manifest"] = str(storage.manifest_root / "job_posting" / f"{archive_manifest['run_id']}.json")
        audit.set_counts(read=manifest["total_records"], valid=manifest["total_records"], rejected=0, duplicate=0)
        audit.succeed()
        result["audit_manifest"] = str(audit.path)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    except Exception as exc:
        if audit.record["status"] == "running":
            audit.fail(exc)
        LOGGER.error("Nạp Bronze thất bại (%s): %s", type(exc).__name__, exc)
        return 1

if __name__ == "__main__":
    sys.exit(main())
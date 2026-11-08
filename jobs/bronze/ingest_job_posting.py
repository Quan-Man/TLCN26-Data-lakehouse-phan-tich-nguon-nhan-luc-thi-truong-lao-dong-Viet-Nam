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
from uuid import uuid4

LOGGER = logging.getLogger("bronze.job_posting")

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT = PROJECT_ROOT / "data/bronze/job_posting"
if str(PROJECT_ROOT / "jobs") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "jobs"))
from common.settings import Settings, load_yaml
from common.storage import Storage
from common.audit import AuditDataError, RunAudit


def configured_inputs(config_path: Path, source_name: str | None = None) -> list[Path]:
    """Resolve source globs from configs/sources.yml relative to the project root."""
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


def json_text(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":"))

def unique_keys(pairs: list[tuple]) -> dict:
    """Không âm thầm ghi đè key JSON bị lặp."""
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
    """Chỉ chuẩn hóa nhãn thư mục, không sửa source_name trong payload."""
    value = source.strip().lower()
    if not re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,79}", value):
        raise ValueError(f"source_name không phù hợp làm partition: {source!r}")
    return value

def ingest_local(inputs: list[Path], output: Path, allow_control_chars: bool = False,
                 run_id: str | None = None) -> tuple[Path, dict]:
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
            return existing, json.loads(manifest_path.read_text(encoding="utf-8-sig"))
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
    try:
        for file_number, path in enumerate(paths, start=1):
            raw_bytes = path.read_bytes()
            try:
                rows, parser_mode = read_records(raw_bytes, allow_control_chars)
                source_names = {row["source_name"] for row in rows}
                if len(source_names) != 1:
                    raise ValueError("each file must contain exactly one source_name")
                source = source_partition(next(iter(source_names)))
            except (ValueError, UnicodeDecodeError) as exc:
                raise AuditDataError(
                    f"Source data validation failed for {path.name} ({type(exc).__name__})"
                ) from exc
            file_hash = sha256(raw_bytes)
            relative_dir = Path(f"source_name={source}") / f"file_{file_number:04d}"
            destination = stage / relative_dir
            destination.mkdir(parents=True)
            (destination / "original.json").write_bytes(raw_bytes)
            with (destination / "records.jsonl").open("w", encoding="utf-8", newline="\n") as stream:
                for row_number, row in enumerate(rows, start=1):
                    canonical = json.dumps(row, ensure_ascii=False, allow_nan=False,
                                           sort_keys=True, separators=(",", ":"))
                    metadata = {
                        "source_name": source, "source_file": path.name,
                        "source_file_sha256": file_hash, "row_number": row_number,
                        "record_sha256": sha256(canonical.encode("utf-8")),
                        "ingested_at": ingested_at, "run_id": run_id,
                        "parser_mode": parser_mode,
                    }
                    stream.write(json_text({"_metadata": metadata, "raw_payload": row}) + "\n")
            records_path = destination / "records.jsonl"
            manifest["files"].append({
                "source_name": source, "source_file": path.name,
                "source_file_sha256": file_hash, "source_size_bytes": len(raw_bytes),
                "record_count": len(rows), "parser_mode": parser_mode,
                "original_path": (relative_dir / "original.json").as_posix(),
                "records_path": (relative_dir / "records.jsonl").as_posix(),
                "records_sha256": sha256(records_path.read_bytes()),
            })
            source_counts[source] += len(rows)
            LOGGER.info("%s: %d bản ghi; parser=%s", path.name, len(rows), parser_mode)
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
    return final_dir, manifest

def upload_minio(run_dir: Path, output: Path, bucket: str, prefix: str,
                 create_bucket: bool = False, settings: Settings | None = None) -> str:
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
    # manifest.json là dấu hoàn tất; luôn upload sau mọi object dữ liệu.
    files = sorted(path for path in run_dir.rglob("*") if path.is_file() and path.name != "manifest.json")
    files.append(run_dir / "manifest.json")
    for path in files:
        object_name = base_key + "/" + path.relative_to(run_dir).as_posix()
        client.fput_object(bucket_name=bucket, object_name=object_name, file_path=str(path),
                           content_type="application/x-ndjson" if path.suffix == ".jsonl" else "application/json")
    return f"s3://{bucket}/{base_key}"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Nạp job_posting gốc vào Bronze local và MinIO."
    )
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
        help="Upload lên MinIO sau khi ghi local (mặc định)",
    )
    upload_mode.add_argument(
        "--local-only",
        dest="minio",
        action="store_false",
        help="Chỉ ghi local, không upload MinIO",
    )

    parser.set_defaults(allow_control_chars=True, minio=True)
    parser.add_argument("--bucket", default=os.getenv("MINIO_BUCKET", "bronze"))
    parser.add_argument("--prefix", default=os.getenv("MINIO_PREFIX", "job_posting"))
    parser.add_argument("--create-bucket", action="store_true")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
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
            audit.add_input(item["source_file_id"], item["sha256"],
                            size_bytes=item["size_bytes"], status=item["status"])
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
                                         run_id=plan["run_id"])
        LOGGER.info("Local hoàn tất: %s", run_dir)
        result = {"total_records": manifest["total_records"],
                  "source_counts": manifest["source_counts"], "manifest": str(run_dir / "manifest.json")}
        if args.minio:
            result["minio_uri"] = upload_minio(run_dir, args.output, args.bucket,
                                               args.prefix, args.create_bucket, settings=settings)
        archive_manifest = storage.commit_ingestion(
            plan, metadata={"bronze_manifest": str((run_dir / "manifest.json").relative_to(args.output.resolve()))}
        )
        result["archive_manifest"] = str(storage.manifest_root / "job_posting" / f"{archive_manifest['run_id']}.json")
        audit.set_counts(read=manifest["total_records"], valid=manifest["total_records"],
                         rejected=0, duplicate=0)
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

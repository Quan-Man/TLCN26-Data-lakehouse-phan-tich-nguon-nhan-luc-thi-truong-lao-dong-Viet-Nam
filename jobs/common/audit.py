"""Per-attempt pipeline audit records with checksums, row counts, and output versions."""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import tempfile
import time
from uuid import uuid4


ROOT = Path(__file__).resolve().parents[2]


class AuditDataError(ValueError):
    """Raised when source rows fail parsing, normalization, or data-quality rules."""


class AuditStateError(RuntimeError):
    """Raised when an audit record is modified after a terminal status."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _checksum_files(paths) -> tuple[str | None, dict[str, str]]:
    values = {}
    for raw_path in paths or ():
        path = Path(raw_path).expanduser().resolve(strict=True)
        try:
            logical_name = path.relative_to(ROOT).as_posix()
        except ValueError:
            logical_name = path.name
        digest = _sha256_file(path)
        if logical_name in values and values[logical_name] != digest:
            raise ValueError(f"Ambiguous version input name: {logical_name}")
        values[logical_name] = digest
    if not values:
        return None, values
    combined = hashlib.sha256(json.dumps(sorted(values.items()), separators=(",", ":")).encode()).hexdigest()
    return combined, values


def _slug(value: str) -> str:
    value = re.sub(r"[^A-Za-z0-9_.-]+", "_", value.strip()).strip("._-")
    if not value or value in {".", ".."}:
        raise ValueError("job_name must contain a safe identifier")
    return value[:100]


def _safe_error_message(value: str) -> str:
    message = str(value)
    message = re.sub(r"(?i)(password|secret|token|access[_ -]?key|credential)(\s*[:=]\s*)[^\s,;]+",
                     r"\1\2[REDACTED]", message)
    message = re.sub(r"(https?://)[^/@\s]+:[^/@\s]+@", r"\1[REDACTED]@", message)
    return message[:1000]


def _atomic_write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=".pending_", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as stream:
            json.dump(payload, stream, ensure_ascii=False, indent=2, allow_nan=False)
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


class RunAudit:
    """Write and incrementally persist one audit record per execution attempt.

    Count definitions are caller-owned. Common convention: ``read`` is rows
    examined, ``valid`` is accepted rows, ``rejected`` is quarantined rows, and
    ``duplicate`` is the subset rejected/deduplicated as duplicate business keys.
    """

    COUNT_FIELDS = ("read", "valid", "rejected", "duplicate")
    TERMINAL = {"success", "success_with_rejects", "failed_task", "failed_data"}

    def __init__(self, job_name: str, *, run_id: str | None = None, task_id: str | None = None,
                 attempt: int | None = None, root: str | Path | None = None,
                 code_version: str | None = None, mapping_version: str | None = None,
                 code_files=(), mapping_files=()):
        self.job_name = _slug(job_name)
        self.run_id = run_id or os.getenv("AIRFLOW_CTX_DAG_RUN_ID") or uuid4().hex
        if not isinstance(self.run_id, str) or not self.run_id:
            raise ValueError("run_id must be a non-empty string")
        self.task_id = task_id or os.getenv("AIRFLOW_CTX_TASK_ID")
        self.attempt = (attempt if attempt is not None
                        else int(os.getenv("AIRFLOW_CTX_TRY_NUMBER", "1")))
        if self.attempt < 1:
            raise ValueError("attempt must be >= 1")
        self.root = Path(root or ROOT / "data" / "audit").expanduser().resolve()
        run_digest = hashlib.sha256(self.run_id.encode("utf-8")).hexdigest()
        self.attempt_id = uuid4().hex
        self.path = (self.root / self.job_name / run_digest /
                     f"attempt-{self.attempt:03d}-{self.attempt_id}.json")
        code_file_hash, code_files_hashes = _checksum_files(code_files)
        mapping_file_hash, mapping_files_hashes = _checksum_files(mapping_files)
        self.record = {
            "format_version": 1,
            "job_name": self.job_name,
            "task_id": self.task_id,
            "run_id": self.run_id,
            "attempt": self.attempt,
            "attempt_id": self.attempt_id,
            "status": "created",
            "started_at": None,
            "ended_at": None,
            "duration_seconds": None,
            "input_checksum": None,
            "inputs": [],
            "counts": {name: 0 for name in self.COUNT_FIELDS},
            "code_version": code_version or code_file_hash,
            "code_files": code_files_hashes,
            "mapping_version": mapping_version or mapping_file_hash,
            "mapping_files": mapping_files_hashes,
            "outputs": {},
            "failure": None,
        }
        self._started_monotonic = None

    def _ensure_open(self):
        if self.record["status"] in self.TERMINAL:
            raise AuditStateError(f"Audit attempt is already terminal: {self.record['status']}")

    def _persist(self):
        _atomic_write(self.path, self.record)

    def start(self):
        if self.record["status"] != "created":
            raise AuditStateError("Audit attempt can only be started once")
        self.record["status"] = "running"
        self.record["started_at"] = _utc_now()
        self._started_monotonic = time.monotonic()
        self._persist()
        return self

    def add_input(self, name: str, checksum: str, *, size_bytes: int | None = None,
                  status: str | None = None):
        self._ensure_open()
        if not name or not checksum:
            raise ValueError("input name and checksum must be non-empty")
        item = {"name": str(name), "checksum": str(checksum)}
        if size_bytes is not None:
            if size_bytes < 0:
                raise ValueError("size_bytes must be non-negative")
            item["size_bytes"] = int(size_bytes)
        if status is not None:
            item["status"] = str(status)
        existing = {entry["name"]: entry for entry in self.record["inputs"]}
        if item["name"] in existing and existing[item["name"]]["checksum"] != item["checksum"]:
            raise ValueError(f"Conflicting input checksums recorded for {name!r}")
        existing[item["name"]] = item
        self.record["inputs"] = [existing[key] for key in sorted(existing)]
        self.record["input_checksum"] = hashlib.sha256(
            json.dumps([(entry["name"], entry["checksum"]) for entry in self.record["inputs"]],
                       ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        self._persist()
        return self

    def add_input_file(self, path: str | Path, *, logical_name: str | None = None,
                       status: str | None = None):
        file_path = Path(path).expanduser().resolve(strict=True)
        if not file_path.is_file():
            raise ValueError(f"Input is not a regular file: {file_path.name}")
        return self.add_input(logical_name or file_path.name, _sha256_file(file_path),
                              size_bytes=file_path.stat().st_size, status=status)

    def set_counts(self, *, read=None, valid=None, rejected=None, duplicate=None,
                   additive: bool = False):
        self._ensure_open()
        for name, value in (("read", read), ("valid", valid),
                            ("rejected", rejected), ("duplicate", duplicate)):
            if value is None:
                continue
            if type(value) is not int or value < 0:
                raise ValueError(f"{name} count must be a non-negative integer")
            self.record["counts"][name] += value if additive else 0
            if not additive:
                self.record["counts"][name] = value
        self._persist()
        return self

    def set_versions(self, *, code_version=None, mapping_version=None,
                     code_files=(), mapping_files=()):
        self._ensure_open()
        code_digest, code_hashes = _checksum_files(code_files)
        mapping_digest, mapping_hashes = _checksum_files(mapping_files)
        if code_version is not None:
            self.record["code_version"] = str(code_version)
        elif code_digest:
            self.record["code_version"] = code_digest
        if mapping_version is not None:
            self.record["mapping_version"] = str(mapping_version)
        elif mapping_digest:
            self.record["mapping_version"] = mapping_digest
        self.record["code_files"].update(code_hashes)
        self.record["mapping_files"].update(mapping_hashes)
        self._persist()
        return self

    def record_output(self, name: str, *, delta_version: int | None = None,
                      checksum: str | None = None, row_count: int | None = None,
                      location: str | None = None):
        self._ensure_open()
        if not name:
            raise ValueError("output name must be non-empty")
        output = dict(self.record["outputs"].get(name, {}))
        if delta_version is not None:
            if type(delta_version) is not int or delta_version < 0:
                raise ValueError("delta_version must be an integer >= 0")
            output["delta_version"] = delta_version
        if checksum is not None:
            output["checksum"] = str(checksum)
        if row_count is not None:
            if type(row_count) is not int or row_count < 0:
                raise ValueError("row_count must be a non-negative integer")
            output["row_count"] = row_count
        if location is not None:
            output["location"] = str(location)
        self.record["outputs"][name] = output
        self._persist()
        return self

    def succeed(self):
        self._ensure_open()
        self.record["status"] = ("success_with_rejects" if self.record["counts"]["rejected"]
                                  else "success")
        self._finish()
        return self.path

    def fail(self, error: BaseException, *, category: str | None = None):
        self._ensure_open()
        data_failure = category == "data" or (category is None and _is_data_error(error))
        self.record["status"] = "failed_data" if data_failure else "failed_task"
        self.record["failure"] = {
            "category": "data" if data_failure else "task",
            "exception_type": type(error).__name__,
            "message": _safe_error_message(error),
        }
        self._finish()
        return self.path

    def _finish(self):
        self.record["ended_at"] = _utc_now()
        if self._started_monotonic is not None:
            self.record["duration_seconds"] = round(time.monotonic() - self._started_monotonic, 6)
        self._persist()

    def __enter__(self):
        return self.start()

    def __exit__(self, exc_type, exc, traceback):
        if exc is None:
            if self.record["status"] == "running":
                self.succeed()
            return False
        if self.record["status"] == "running":
            self.fail(exc)
        return False


def _is_data_error(error: BaseException) -> bool:
    if isinstance(error, AuditDataError):
        return True
    # Import lazily to keep audit usable in minimal jobs.
    try:
        from .normalization import NormalizationError
        from .keys import KeyCollisionError
        return isinstance(error, (NormalizationError, KeyCollisionError))
    except ImportError:
        return False

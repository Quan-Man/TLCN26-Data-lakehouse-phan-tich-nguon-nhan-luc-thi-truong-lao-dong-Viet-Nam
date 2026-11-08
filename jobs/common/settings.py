"""Environment-backed settings and safe project configuration readers.

The module deliberately reads ``os.environ`` only. Docker Compose is responsible
for loading the project .env file and passing selected values into containers.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping
from urllib.parse import urlsplit
import json
import os


_PROJECT_ROOT = Path(__file__).resolve().parents[2]


def env_bool(value: str) -> bool:
    value = value.strip().lower()
    if value in {"true", "1", "yes", "on"}:
        return True
    if value in {"false", "0", "no", "off"}:
        return False
    raise ValueError("Boolean setting must be true/false, 1/0, yes/no, or on/off")


def parse_endpoint(value: str, secure: bool = False) -> tuple[str, bool]:
    """Return (host:port, TLS); an explicit URL scheme takes precedence."""
    if not value or not value.strip():
        raise ValueError("MINIO_ENDPOINT must not be empty")
    value = value.strip()
    parsed = urlsplit(value if "://" in value else
                      ("https://" if secure else "http://") + value)
    if (parsed.scheme not in {"http", "https"} or not parsed.hostname
            or parsed.username is not None or parsed.password is not None
            or parsed.path not in {"", "/"} or parsed.query or parsed.fragment):
        raise ValueError("MINIO_ENDPOINT must be host:port or an http(s) endpoint without credentials/path")
    _ = parsed.port
    return parsed.netloc, parsed.scheme == "https"


def require_env(*names: str, env: Mapping[str, str] | None = None) -> dict[str, str]:
    """Return required non-empty environment values or fail with the missing names."""
    source = os.environ if env is None else env
    missing = [name for name in names if not source.get(name, "").strip()]
    if missing:
        raise ValueError("Missing required environment variable(s): " + ", ".join(missing))
    return {name: source[name].strip() for name in names}


def _read_config_file(path: Path):
    suffix = path.suffix.lower()
    with path.open(encoding="utf-8-sig") as stream:
        if suffix == ".json":
            return json.load(stream)
        if suffix in {".yml", ".yaml"}:
            try:
                import yaml
            except ImportError as exc:
                raise RuntimeError("PyYAML is required to read YAML configuration") from exc
            return yaml.safe_load(stream)
    raise ValueError(f"Unsupported config format for {path.name!r}; use JSON or YAML")


def load_config(path: str | Path, *, config_root: str | Path | None = None):
    """Read JSON/YAML; relative paths resolve under CONFIG_ROOT/project configs."""
    file_path = Path(path)
    if not file_path.is_absolute():
        if config_root is None:
            config_root = os.getenv("CONFIG_ROOT", str(_PROJECT_ROOT / "configs"))
        file_path = Path(config_root) / file_path
    if not file_path.is_file():
        raise FileNotFoundError(f"Required configuration file not found: {file_path}")
    return _read_config_file(file_path)


def load_json(path: str | Path):
    """Read UTF-8 JSON, accepting a Windows BOM."""
    file_path = Path(path)
    if not file_path.is_file():
        raise FileNotFoundError(f"Required JSON file not found: {file_path}")
    if file_path.suffix.lower() != ".json":
        raise ValueError(f"Expected a .json file: {file_path}")
    return _read_config_file(file_path)


def load_yaml(path: str | Path, *, config_root: str | Path | None = None):
    """Read UTF-8 YAML safely, accepting a Windows BOM."""
    file_path = Path(path)
    if not file_path.is_absolute():
        if config_root is None:
            config_root = os.getenv("CONFIG_ROOT", str(_PROJECT_ROOT / "configs"))
        file_path = Path(config_root) / file_path
    if file_path.suffix.lower() not in {".yml", ".yaml"}:
        raise ValueError(f"Expected a .yml or .yaml file: {file_path}")
    if not file_path.is_file():
        raise FileNotFoundError(f"Required YAML file not found: {file_path}")
    return _read_config_file(file_path)


@dataclass(frozen=True)
class Settings:
    project_root: Path
    config_root: Path
    data_root: Path
    minio_endpoint: str
    minio_secure: bool
    minio_access_key: str = field(repr=False)
    minio_secret_key: str = field(repr=False)
    bronze_bucket: str = "bronze"
    silver_bucket: str = "silver"
    gold_bucket: str = "gold"
    spark_master: str = "local[2]"
    spark_timezone: str = "UTC"
    shuffle_partitions: int = 4
    log_level: str = "INFO"

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None, *,
                 required: tuple[str, ...] = (), require_minio: bool = False,
                 validate_paths: bool = True, required_configs: tuple[str, ...] = ()):
        values = os.environ if env is None else env
        if required:
            require_env(*required, env=values)
        secure_setting = env_bool(values.get("MINIO_SECURE", "false"))
        endpoint, secure = parse_endpoint(values.get("MINIO_ENDPOINT", "http://minio:9000"), secure_setting)
        access_key = values.get("MINIO_ACCESS_KEY") or values.get("MINIO_ROOT_USER", "")
        secret_key = values.get("MINIO_SECRET_KEY") or values.get("MINIO_ROOT_PASSWORD", "")
        if require_minio and (not access_key.strip() or not secret_key.strip()):
            raise ValueError(
                "MinIO credentials are required; set MINIO_ACCESS_KEY/MINIO_SECRET_KEY "
                "or MINIO_ROOT_USER/MINIO_ROOT_PASSWORD in the environment"
            )
        try:
            partitions = int(values.get("SPARK_SHUFFLE_PARTITIONS", "4"))
        except (TypeError, ValueError) as exc:
            raise ValueError("SPARK_SHUFFLE_PARTITIONS must be an integer") from exc
        if partitions < 1:
            raise ValueError("SPARK_SHUFFLE_PARTITIONS must be > 0")

        project_root = Path(values.get("PROJECT_ROOT", _PROJECT_ROOT)).expanduser().resolve()
        config_root = Path(values.get("CONFIG_ROOT", project_root / "configs")).expanduser().resolve()
        data_root = Path(values.get("DATA_ROOT", project_root / "data")).expanduser().resolve()
        if validate_paths:
            expected = {
                "project root": project_root,
                "jobs directory": project_root / "jobs",
                "config directory": config_root,
                "data directory": data_root,
            }
            missing = [f"{label}: {path}" for label, path in expected.items() if not path.is_dir()]
            if missing:
                raise FileNotFoundError(
                    "Invalid project/container paths; check PROJECT_ROOT, CONFIG_ROOT, DATA_ROOT "
                    "and Docker mounts: " + "; ".join(missing)
                )
            missing_configs = [str(config_root / name) for name in required_configs
                               if not (config_root / name).is_file()]
            if missing_configs:
                raise FileNotFoundError(
                    "Required configuration file(s) are missing: " + ", ".join(missing_configs)
                )

        return cls(
            project_root=project_root, config_root=config_root, data_root=data_root,
            minio_endpoint=endpoint, minio_secure=secure,
            minio_access_key=access_key.strip(), minio_secret_key=secret_key.strip(),
            bronze_bucket=values.get("BRONZE_BUCKET", "bronze"),
            silver_bucket=values.get("SILVER_BUCKET", "silver"),
            gold_bucket=values.get("GOLD_BUCKET", "gold"),
            spark_master=values.get("SPARK_MASTER", "local[2]"),
            spark_timezone=values.get("SPARK_TIMEZONE", "UTC"),
            shuffle_partitions=partitions, log_level=values.get("LOG_LEVEL", "INFO"),
        )

    def require_minio_credentials(self):
        if not self.minio_access_key or not self.minio_secret_key:
            raise ValueError(
                "MinIO credentials are required; set MINIO_ACCESS_KEY/MINIO_SECRET_KEY "
                "or MINIO_ROOT_USER/MINIO_ROOT_PASSWORD in the environment"
            )

    def require_paths(self, *paths: str | Path) -> tuple[Path, ...]:
        resolved = tuple(self.resolve_path(path) for path in paths)
        missing = [str(path) for path in resolved if not path.exists()]
        if missing:
            raise FileNotFoundError("Required project path(s) missing: " + ", ".join(missing))
        return resolved

    def resolve_path(self, path: str | Path) -> Path:
        value = Path(path).expanduser()
        return value.resolve() if value.is_absolute() else (self.project_root / value).resolve()

    def config_path(self, *parts) -> Path:
        return self.config_root.joinpath(*parts)

    def data_path(self, *parts) -> Path:
        return self.data_root.joinpath(*parts)

    def load_config(self, path: str | Path):
        return load_config(path, config_root=self.config_root)

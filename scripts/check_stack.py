#!/usr/bin/env python3
"""Check the local lakehouse service endpoints and S3 buckets."""
from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import socket
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "jobs") not in sys.path:
    sys.path.insert(0, str(ROOT / "jobs"))

from common.trino_client import TrinoClient


def load_dotenv() -> None:
    """Read simple KEY=value defaults from project .env without overriding process env."""
    path = ROOT / ".env"
    if not path.is_file():
        return
    for line in path.read_text(encoding="utf-8-sig").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key, value = key.strip(), value.strip()
        if key and key.replace("_", "").isalnum() and key not in os.environ:
            if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
                value = value[1:-1]
            os.environ[key] = value


def endpoint(name: str, default_port_key: str, default_port: str, default_host="127.0.0.1") -> str:
    value = os.getenv(name)
    if value:
        return value.rstrip("/")
    port = os.getenv(default_port_key, default_port)
    return f"http://{default_host}:{port}"


def get_url(url: str, timeout: float) -> tuple[int, bytes]:
    request = urllib.request.Request(url, headers={"User-Agent": "lakehouse-stack-check/1.0"})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.status, response.read()


def tcp_check(host: str, port: int, timeout: float) -> str:
    with socket.create_connection((host, port), timeout=timeout):
        return f"TCP connection established to {host}:{port}"


def s3_signing_key(secret: str, day: str, region: str) -> bytes:
    def sign(key: bytes, value: str) -> bytes:
        return hmac.new(key, value.encode("utf-8"), hashlib.sha256).digest()
    key = sign(("AWS4" + secret).encode("utf-8"), day)
    key = sign(key, region)
    key = sign(key, "s3")
    return sign(key, "aws4_request")


def check_bucket(base_url: str, bucket: str, access: str, secret: str, region: str,
                 timeout: float) -> str:
    parsed = urlsplit(base_url)
    if (parsed.scheme not in {"http", "https"} or not parsed.hostname
            or parsed.username or parsed.password or parsed.path not in {"", "/"}
            or parsed.query or parsed.fragment):
        raise ValueError("MINIO_ENDPOINT must be an http(s) endpoint without a path")
    now = datetime.now(timezone.utc)
    stamp, day = now.strftime("%Y%m%dT%H%M%SZ"), now.strftime("%Y%m%d")
    payload_hash = hashlib.sha256(b"").hexdigest()
    host = parsed.netloc
    headers = f"host:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{stamp}\n"
    signed_headers = "host;x-amz-content-sha256;x-amz-date"
    canonical = f"HEAD\n/{bucket}\n\n{headers}\n{signed_headers}\n{payload_hash}"
    scope = f"{day}/{region}/s3/aws4_request"
    string_to_sign = ("AWS4-HMAC-SHA256\n" + stamp + "\n" + scope + "\n"
                      + hashlib.sha256(canonical.encode("utf-8")).hexdigest())
    signature = hmac.new(s3_signing_key(secret, day, region), string_to_sign.encode("utf-8"),
                         hashlib.sha256).hexdigest()
    authorization = (f"AWS4-HMAC-SHA256 Credential={access}/{scope}, "
                     f"SignedHeaders={signed_headers}, Signature={signature}")
    request = urllib.request.Request(
        f"{parsed.scheme}://{host}/{bucket}", method="HEAD",
        headers={"Host": host, "X-Amz-Date": stamp, "X-Amz-Content-Sha256": payload_hash,
                 "Authorization": authorization},
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            if response.status != 200:
                raise RuntimeError(f"unexpected S3 HTTP status {response.status}")
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            raise RuntimeError(f"bucket {bucket!r} is missing") from exc
        if exc.code == 403:
            raise RuntimeError(f"credential or permission denied checking bucket {bucket!r} (HTTP 403)") from exc
        raise RuntimeError(f"S3 bucket check failed for {bucket!r} (HTTP {exc.code})") from exc
    return f"Bucket exists and credentials can access it: {bucket}"


def check_trino(base_url: str, user: str, timeout: float) -> str:
    client = TrinoClient(base_url, user=user, request_timeout=timeout,
                         query_timeout=max(timeout * 4, 5), startup_timeout=timeout)
    client.wait_until_ready(timeout=timeout)
    result = client.execute("SELECT 1", operation="stack connectivity check")
    return f"Trino ready; SELECT 1 completed (query_id={result.query_id})"


def run_check(results: list[dict], name: str, function, *args) -> None:
    started = time.monotonic()
    try:
        detail = function(*args)
        results.append({"component": name, "status": "PASS", "detail": detail,
                        "duration_seconds": round(time.monotonic() - started, 3)})
    except Exception as exc:
        results.append({"component": name, "status": "FAIL",
                        "detail": f"{type(exc).__name__}: {exc}",
                        "duration_seconds": round(time.monotonic() - started, 3)})


def missing_s3_credentials(bucket: str) -> str:
    raise RuntimeError(
        "set MINIO_ROOT_USER/MINIO_ROOT_PASSWORD or MINIO_ACCESS_KEY/MINIO_SECRET_KEY"
    )


def check_airflow(base_url: str, timeout: float) -> str:
    status, body = get_url(base_url.rstrip("/") + "/health", timeout)
    try:
        health = json.loads(body)
    except json.JSONDecodeError:
        raise RuntimeError(f"HTTP {status}; health response is not JSON")
    unhealthy = [name for name, value in health.items()
                 if isinstance(value, dict) and value.get("status") not in {None, "healthy"}]
    if unhealthy:
        raise RuntimeError("Airflow reports unhealthy components: " + ", ".join(unhealthy))
    return f"HTTP {status}; health endpoint responded"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--timeout", type=float, default=3.0,
                        help="Maximum seconds for each endpoint/socket request (default: 3)")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON report")
    args = parser.parse_args()
    if args.timeout <= 0:
        parser.error("--timeout must be greater than zero")
    load_dotenv()
    timeout = args.timeout
    results: list[dict] = []

    minio_url = os.getenv("MINIO_ENDPOINT")
    if not minio_url:
        secure = os.getenv("MINIO_SECURE", "false").strip().lower() in {"true", "1", "yes", "on"}
        scheme = "https" if secure else "http"
        minio_url = f"{scheme}://127.0.0.1:{os.getenv('MINIO_API_PORT', '9000')}"
    trino_url = endpoint("TRINO_URL", "TRINO_PORT", "8085")
    airflow_url = endpoint("AIRFLOW_URL", "AIRFLOW_PORT", "8080")
    hms_host = os.getenv("HIVE_METASTORE_HOST", "127.0.0.1")
    hms_port = int(os.getenv("HIVE_METASTORE_PORT", "9083"))
    postgres_host = os.getenv("POSTGRES_HOST", "127.0.0.1")
    postgres_port = int(os.getenv("POSTGRES_PORT", "5433"))

    run_check(results, "MinIO ready endpoint", get_url, minio_url.rstrip("/") + "/minio/health/ready", timeout)
    access = os.getenv("MINIO_ACCESS_KEY") or os.getenv("MINIO_ROOT_USER", "")
    secret = os.getenv("MINIO_SECRET_KEY") or os.getenv("MINIO_ROOT_PASSWORD", "")
    if not access or not secret:
        for bucket in ("bronze", "silver", "gold"):
            run_check(results, f"S3 bucket {bucket}", missing_s3_credentials, bucket)
    else:
        region = os.getenv("AWS_REGION", "us-east-1")
        for bucket in ("bronze", "silver", "gold"):
            run_check(results, f"S3 bucket {bucket}", check_bucket, minio_url, bucket, access, secret, region, timeout)

    run_check(results, "PostgreSQL TCP", tcp_check, postgres_host, postgres_port, timeout)
    run_check(results, "Hive Metastore TCP", tcp_check, hms_host, hms_port, timeout)
    run_check(results, "Airflow health", check_airflow, airflow_url, timeout)
    run_check(results, "Trino query", check_trino, trino_url,
              os.getenv("TRINO_USER", "lakehouse"), timeout)

    passed = sum(result["status"] == "PASS" for result in results)
    failed = len(results) - passed
    report = {"status": "PASS" if failed == 0 else "FAIL", "passed": passed,
              "failed": failed, "checks": results}
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        for result in results:
            print(f"[{result['status']}] {result['component']} "
                  f"({result['duration_seconds']:.3f}s): {result['detail']}")
        print(f"STACK {report['status']}: {passed} passed, {failed} failed")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())

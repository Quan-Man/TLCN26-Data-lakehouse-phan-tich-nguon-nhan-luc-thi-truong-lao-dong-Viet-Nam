#!/usr/bin/env python3
"""Ensure the Bronze, Silver, and Gold buckets exist in the configured S3 service."""
from __future__ import annotations

import os
import sys
import time
from urllib.parse import urlsplit

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError


BUCKETS = ("bronze", "silver", "gold")
RETRYABLE_CODES = {"RequestTimeout", "RequestTimeoutException", "SlowDown", "InternalError", "ServiceUnavailable"}
AUTH_CODES = {
    "AccessDenied", "InvalidAccessKeyId", "InvalidToken", "SignatureDoesNotMatch",
    "ExpiredToken", "AuthorizationHeaderMalformed",
}


def make_s3_client():
    endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000").strip()
    secure_value = os.getenv("MINIO_SECURE", "false").strip().lower()
    if secure_value not in {"true", "1", "yes", "on", "false", "0", "no", "off"}:
        raise ValueError("MINIO_SECURE must be a boolean value")
    secure = secure_value in {"true", "1", "yes", "on"}
    if "://" not in endpoint:
        endpoint = ("https://" if secure else "http://") + endpoint
    parsed = urlsplit(endpoint)
    if (parsed.scheme not in {"http", "https"} or not parsed.hostname
            or parsed.username or parsed.password or parsed.path not in {"", "/"}
            or parsed.query or parsed.fragment):
        raise ValueError("MINIO_ENDPOINT must be host:port or an http(s) URL without credentials/path")
    _ = parsed.port  # Raise a clear configuration error for malformed ports.
    access_key = os.getenv("MINIO_ACCESS_KEY") or os.getenv("MINIO_ROOT_USER")
    secret_key = os.getenv("MINIO_SECRET_KEY") or os.getenv("MINIO_ROOT_PASSWORD")
    if not access_key or not secret_key:
        raise ValueError("Missing MINIO_ACCESS_KEY/MINIO_SECRET_KEY or MINIO_ROOT_USER/MINIO_ROOT_PASSWORD")
    return boto3.client(
        "s3", endpoint_url=f"{parsed.scheme}://{parsed.netloc}",
        aws_access_key_id=access_key, aws_secret_access_key=secret_key,
        region_name=os.getenv("AWS_REGION", "us-east-1"),
        config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 0}),
    )


def error_details(exc: ClientError) -> tuple[str, int | None, str]:
    response = exc.response
    error = response.get("Error", {})
    metadata = response.get("ResponseMetadata", {})
    return str(error.get("Code", "Unknown")), metadata.get("HTTPStatusCode"), str(error.get("Message", ""))


def ensure_bucket(s3, bucket: str) -> None:
    try:
        s3.head_bucket(Bucket=bucket)
        print(f"Bucket ready: {bucket}", flush=True)
        return
    except ClientError as exc:
        code, status, message = error_details(exc)
        if status == 404 or code in {"NoSuchBucket", "NotFound", "404"}:
            pass  # A definite missing-bucket response; create it below.
        elif status == 403 or code in AUTH_CODES or code in {"Forbidden", "403"}:
            raise RuntimeError(
                f"S3 credential/permission failure checking bucket {bucket!r} "
                f"(code={code}, HTTP={status}): {message}"
            ) from exc
        elif status and status >= 500 or code in RETRYABLE_CODES:
            raise
        else:
            raise RuntimeError(
                f"S3 failed checking bucket {bucket!r} (code={code}, HTTP={status}): {message}"
            ) from exc

    try:
        s3.create_bucket(Bucket=bucket)
        print(f"Bucket created: {bucket}", flush=True)
    except ClientError as exc:
        code, status, message = error_details(exc)
        if code in {"BucketAlreadyOwnedByYou", "AlreadyOwnedByYou"}:
            print(f"Bucket ready: {bucket}", flush=True)
            return
        if status == 403 or code in AUTH_CODES or code in {"Forbidden", "403"}:
            raise RuntimeError(
                f"S3 credential/permission failure creating bucket {bucket!r} "
                f"(code={code}, HTTP={status}): {message}"
            ) from exc
        if code == "BucketAlreadyExists":
            raise RuntimeError(f"Bucket {bucket!r} exists but is owned by another account") from exc
        if status and status >= 500 or code in RETRYABLE_CODES:
            raise
        raise RuntimeError(
            f"S3 failed creating bucket {bucket!r} (code={code}, HTTP={status}): {message}"
        ) from exc


def retryable(exc: Exception) -> bool:
    if isinstance(exc, ClientError):
        code, status, _ = error_details(exc)
        return bool((status and status >= 500) or code in RETRYABLE_CODES
                    or code in {"NoSuchBucket", "NotFound", "404"})
    return isinstance(exc, BotoCoreError)


def main() -> int:
    try:
        s3 = make_s3_client()
        attempts = int(os.getenv("MINIO_STARTUP_ATTEMPTS", "60"))
        delay = float(os.getenv("MINIO_STARTUP_RETRY_SECONDS", "2"))
        if attempts < 1 or delay < 0:
            raise ValueError("MINIO_STARTUP_ATTEMPTS must be positive and MINIO_STARTUP_RETRY_SECONDS non-negative")
        for bucket in BUCKETS:
            for attempt in range(1, attempts + 1):
                try:
                    ensure_bucket(s3, bucket)
                    break
                except Exception as exc:
                    if not retryable(exc) or attempt == attempts:
                        raise
                    print(f"S3 not ready ({attempt}/{attempts}) for {bucket}: {exc}", file=sys.stderr, flush=True)
                    time.sleep(delay)
        print("All required S3 buckets are ready.", flush=True)
        return 0
    except Exception as exc:
        print(f"Bucket bootstrap failed: {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

"""Install version-matched JVM dependencies at image build time."""

from __future__ import annotations

import hashlib
import http.client
import re
import time
from pathlib import Path
import urllib.error
import urllib.request

import pyspark


BASE = "https://repo.maven.apache.org/maven2/"
ARTIFACTS = [
    "io/delta/delta-spark_2.12/3.3.0/delta-spark_2.12-3.3.0.jar",
    "io/delta/delta-storage/3.3.0/delta-storage-3.3.0.jar",
    "org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar",
    "com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar",
]

CHUNK_SIZE = 1024 * 1024
MAX_ATTEMPTS = 5
REQUEST_TIMEOUT_SECONDS = 120
SHA1_PATTERN = re.compile(r"^[0-9a-fA-F]{40}$")
USER_AGENT = "26tlcn-lakehouse-docker-build/1.0"


def request(url: str) -> urllib.request.Request:
    """Create a Maven request that avoids transparent content encoding."""
    return urllib.request.Request(
        url,
        headers={
            "Accept-Encoding": "identity",
            "User-Agent": USER_AGENT,
        },
    )


def sha1_file(path: Path) -> str:
    """Calculate a file SHA-1 without loading the entire JAR into memory."""
    digest = hashlib.sha1()
    with path.open("rb") as stream:
        while chunk := stream.read(CHUNK_SIZE):
            digest.update(chunk)
    return digest.hexdigest()


def fetch_expected_sha1(artifact_url: str) -> str:
    """Read and validate the checksum published by Maven Central."""
    checksum_url = artifact_url + ".sha1"
    last_error: Exception | None = None

    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            with urllib.request.urlopen(
                request(checksum_url), timeout=REQUEST_TIMEOUT_SECONDS
            ) as response:
                expected = response.read().decode("ascii").strip().split()[0]

            if not SHA1_PATTERN.fullmatch(expected):
                raise ValueError(
                    f"Invalid SHA-1 response from {checksum_url}: {expected!r}"
                )
            return expected.lower()
        except (
            OSError,
            IndexError,
            UnicodeError,
            ValueError,
            http.client.HTTPException,
            urllib.error.URLError,
        ) as error:
            last_error = error
            if attempt < MAX_ATTEMPTS:
                delay = min(2 ** (attempt - 1), 16)
                print(
                    f"Checksum request failed (attempt {attempt}/{MAX_ATTEMPTS}); "
                    f"retrying in {delay}s: {error}"
                )
                time.sleep(delay)

    raise RuntimeError(
        f"Unable to read checksum after {MAX_ATTEMPTS} attempts: {checksum_url}"
    ) from last_error


def download_once(url: str, temporary: Path) -> tuple[str, int]:
    """Download one file and return its SHA-1 and byte count."""
    digest = hashlib.sha1()
    downloaded = 0

    with urllib.request.urlopen(
        request(url), timeout=REQUEST_TIMEOUT_SECONDS
    ) as response:
        content_length_header = response.headers.get("Content-Length")
        expected_size = (
            int(content_length_header) if content_length_header is not None else None
        )

        with temporary.open("wb") as output:
            while chunk := response.read(CHUNK_SIZE):
                output.write(chunk)
                digest.update(chunk)
                downloaded += len(chunk)

    if expected_size is not None and downloaded != expected_size:
        raise IOError(
            f"Incomplete download: expected {expected_size} bytes, "
            f"received {downloaded} bytes"
        )

    return digest.hexdigest(), downloaded


def download_verified(artifact: str, jars: Path) -> None:
    """Download one artifact with retries and atomic installation."""
    artifact_url = BASE + artifact
    target = jars / Path(artifact).name
    temporary = target.with_name(target.name + ".part")
    expected_sha1 = fetch_expected_sha1(artifact_url)

    if target.exists():
        actual_sha1 = sha1_file(target)
        if actual_sha1 == expected_sha1:
            print(f"Already verified {target.name}")
            return
        print(
            f"Replacing invalid existing file {target.name}: "
            f"expected {expected_sha1}, got {actual_sha1}"
        )

    last_error: Exception | None = None

    for attempt in range(1, MAX_ATTEMPTS + 1):
        temporary.unlink(missing_ok=True)
        try:
            actual_sha1, downloaded = download_once(artifact_url, temporary)
            if actual_sha1 != expected_sha1:
                raise RuntimeError(
                    f"Checksum mismatch for {target.name}: "
                    f"expected {expected_sha1}, got {actual_sha1} "
                    f"({downloaded} bytes)"
                )

            temporary.replace(target)
            print(
                f"Verified {target.name} "
                f"({downloaded} bytes, sha1={actual_sha1})"
            )
            return
        except (
            OSError,
            RuntimeError,
            ValueError,
            http.client.HTTPException,
            urllib.error.URLError,
        ) as error:
            last_error = error
            temporary.unlink(missing_ok=True)
            if attempt < MAX_ATTEMPTS:
                delay = min(2 ** (attempt - 1), 16)
                print(
                    f"Download failed for {target.name} "
                    f"(attempt {attempt}/{MAX_ATTEMPTS}); "
                    f"retrying in {delay}s: {error}"
                )
                time.sleep(delay)

    raise RuntimeError(
        f"Failed to download {target.name} after {MAX_ATTEMPTS} attempts"
    ) from last_error


def main() -> None:
    jars = Path(pyspark.__file__).resolve().parent / "jars"
    expected_hadoop_jar = jars / "hadoop-client-runtime-3.3.4.jar"
    if not expected_hadoop_jar.exists():
        raise RuntimeError(
            "Unexpected Hadoop version in PySpark; re-check S3A compatibility"
        )

    for artifact in ARTIFACTS:
        download_verified(artifact, jars)


if __name__ == "__main__":
    main()

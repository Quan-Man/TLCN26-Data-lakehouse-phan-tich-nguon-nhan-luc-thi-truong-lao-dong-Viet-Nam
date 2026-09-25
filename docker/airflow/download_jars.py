"""Install version-matched JVM dependencies at image build time, not per DAG run."""
import hashlib
from pathlib import Path
import urllib.request
import pyspark

BASE = "https://repo.maven.apache.org/maven2/"
ARTIFACTS = [
    "io/delta/delta-spark_2.12/3.3.0/delta-spark_2.12-3.3.0.jar",
    "io/delta/delta-storage/3.3.0/delta-storage-3.3.0.jar",
    "org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar",
    "com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar",
]


def main():
    jars = Path(pyspark.__file__).parent / "jars"
    if not (jars / "hadoop-client-runtime-3.3.4.jar").exists():
        raise RuntimeError("Unexpected Hadoop version in PySpark; re-check S3A compatibility")
    for artifact in ARTIFACTS:
        target = jars / Path(artifact).name
        digest = hashlib.sha1()
        with urllib.request.urlopen(BASE + artifact, timeout=120) as response:
            with target.open("wb") as output:
                while chunk := response.read(1024 * 1024):
                    output.write(chunk)
                    digest.update(chunk)
        with urllib.request.urlopen(BASE + artifact + ".sha1", timeout=60) as response:
            expected = response.read().decode().strip().split()[0]
        if digest.hexdigest() != expected:
            target.unlink(missing_ok=True)
            raise RuntimeError(f"Checksum mismatch: {target.name}")
        print(f"Verified {target.name}")


if __name__ == "__main__":
    main()

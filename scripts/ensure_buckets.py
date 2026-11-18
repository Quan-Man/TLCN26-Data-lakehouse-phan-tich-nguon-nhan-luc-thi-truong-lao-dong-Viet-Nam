import os
import time

from minio import Minio

BUCKETS = ("bronze", "silver", "gold")
MAX_RETRIES = 30
RETRY_DELAY = 2


def ensure_buckets():
    client = Minio(
        "minio:9000",
        access_key=os.environ["MINIO_ROOT_USER"],
        secret_key=os.environ["MINIO_ROOT_PASSWORD"],
        secure=False,
    )

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            for bucket in BUCKETS:
                if client.bucket_exists(bucket):
                    print(f"[OK] Bucket already exists: {bucket}")
                else:
                    client.make_bucket(bucket)
                    print(f"[OK] Bucket created: {bucket}")

            print("MinIO buckets are ready.")
            return

        except Exception as exc:
            print(f"[WARNING] Attempt {attempt}/{MAX_RETRIES}: {exc}")

            if attempt == MAX_RETRIES:
                raise

            time.sleep(RETRY_DELAY)


if __name__ == "__main__":
    ensure_buckets()
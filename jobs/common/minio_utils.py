"""MinIO SDK dùng cho file nguồn; bảng Delta phải ghi bằng Spark."""
from pathlib import Path
from .settings import Settings


def create_minio_client(settings=None):
    from minio import Minio
    settings = settings or Settings.from_env(require_minio=True)
    settings.require_minio_credentials()
    return Minio(settings.minio_endpoint, access_key=settings.minio_access_key,
                 secret_key=settings.minio_secret_key, secure=settings.minio_secure)


def ensure_bucket(client, bucket: str):
    from minio.error import S3Error
    if not client.bucket_exists(bucket):
        try:
            client.make_bucket(bucket)
        except S3Error as exc:
            if exc.code != "BucketAlreadyOwnedByYou":
                raise


def upload_file(client, local_path, bucket: str, object_name: str,
                content_type="application/octet-stream"):
    """Bucket phải có sẵn. Cùng object_name sẽ ghi phiên bản mới/ghi đè theo bucket."""
    path = Path(local_path)
    if not path.is_file():
        raise FileNotFoundError(path)
    if not object_name or object_name.startswith("/"):
        raise ValueError("object_name phải khác rỗng và không bắt đầu bằng /")
    return client.fput_object(bucket, object_name, str(path), content_type=content_type)


def download_file(client, bucket: str, object_name: str, local_path):
    path = Path(local_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    return client.fget_object(bucket, object_name, str(path))


def list_objects(client, bucket: str, prefix=""):
    """Trả iterator; không đưa toàn bộ danh sách lớn vào RAM."""
    return client.list_objects(bucket, prefix=prefix, recursive=True)

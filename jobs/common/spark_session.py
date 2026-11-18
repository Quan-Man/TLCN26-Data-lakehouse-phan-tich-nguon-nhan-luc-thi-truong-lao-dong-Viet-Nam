from .settings import Settings


def create_spark_session(app_name: str, settings=None, *, use_minio=True,
                         use_delta=True, extra_conf=None):
    from pyspark.sql import SparkSession

    settings = settings or Settings.from_env(require_minio=use_minio)
    # Tránh getOrCreate trả session cũ không có extension/endpoint vừa yêu cầu.
    if SparkSession.getActiveSession() is not None or SparkSession._instantiatedSession is not None:
        raise RuntimeError("Đã có SparkSession. Dùng lại session đó hoặc stop trước khi tạo mới.")
    conf = {
        "spark.sql.session.timeZone": settings.spark_timezone,
        "spark.sql.shuffle.partitions": str(settings.shuffle_partitions),
    }
    if use_delta:
        conf.update({
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        })
    if use_minio:
        settings.require_minio_credentials()
        scheme = "https" if settings.minio_secure else "http"
        conf.update({
            "spark.hadoop.fs.s3a.endpoint": f"{scheme}://{settings.minio_endpoint}",
            "spark.hadoop.fs.s3a.access.key": settings.minio_access_key,
            "spark.hadoop.fs.s3a.secret.key": settings.minio_secret_key,
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": str(settings.minio_secure).lower(),
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.aws.credentials.provider":
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        })
    conf.update(extra_conf or {})
    builder = SparkSession.builder.appName(app_name).master(settings.spark_master)
    for key, value in conf.items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

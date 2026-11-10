import os
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Register-Education-Gold")
    .master("local[1]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083")
    .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
    .config("spark.hadoop.fs.s3a.access.key", os.environ["MINIO_ROOT_USER"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["MINIO_ROOT_PASSWORD"])
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    )
    .enableHiveSupport()
    .getOrCreate()
)

try:
    path = "s3a://gold/education/dim_university"

    df = spark.read.format("delta").load(path)
    print("Existing rows:", df.count())

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS education.dim_university
        USING DELTA
        LOCATION '{path}'
    """)

    print("Registration completed.")
    spark.sql("SHOW TABLES IN education").show(truncate=False)

finally:
    spark.stop()

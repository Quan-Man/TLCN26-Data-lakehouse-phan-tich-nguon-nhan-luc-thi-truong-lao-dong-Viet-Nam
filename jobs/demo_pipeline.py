"""Synthetic smoke pipeline. Only writes demo paths/tables; never real fact tables."""
import os
from pathlib import Path
import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import SparkSession, functions as F, types as T

ROOT = Path(__file__).resolve().parents[1]


def spark_session():
    return (
        SparkSession.builder.appName("vietnam-lakehouse-demo")
        .master("local[2]")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", os.environ["MINIO_ENDPOINT"])
        .config("spark.hadoop.fs.s3a.access.key", os.environ["MINIO_ROOT_USER"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["MINIO_ROOT_PASSWORD"])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .getOrCreate()
    )


def main():
    spark = spark_session()
    spark.sparkContext.setLogLevel("WARN")
    try:
        # Demo is a full snapshot. Paths are isolated under demo/.
        raw_path = "s3a://bronze/demo/job_postings"
        silver_path = "s3a://silver/demo/job_postings"
        gold_path = "s3a://gold/demo/job_counts"
        source = str(ROOT / "sample_data" / "job_postings.json")
        spark.read.text(source).write.mode("overwrite").text(raw_path)
        schema = T.StructType([
            T.StructField("job_id", T.StringType()),
            T.StructField("title", T.StringType()),
            T.StructField("location", T.StringType()),
            T.StructField("salary_million_vnd", T.LongType()),
        ])
        raw = spark.read.schema(schema).json(raw_path)
        silver = (
            raw.withColumn("location", F.trim("location"))
            .filter(F.col("job_id").isNotNull() & (F.length("job_id") > 0)
                    & F.col("location").isNotNull() & (F.length("location") > 0))
            .dropDuplicates(["job_id"])
        )
        silver.write.format("delta").mode("overwrite").save(silver_path)
        validated = spark.read.format("delta").load(silver_path)
        if raw.count() != 5 or validated.count() != 3:
            raise AssertionError("Expected 5 raw rows and 3 valid unique demo jobs")
        gold = validated.groupBy("location").agg(F.count("job_id").alias("posting_count"))
        gold.write.format("delta").mode("overwrite").save(gold_path)
        result = spark.read.format("delta").load(gold_path).orderBy("location").collect()
        rows = [(row.location, row.posting_count) for row in result]
        if dict(rows) != {"Ha Noi": 1, "Ho Chi Minh": 2}:
            raise AssertionError(f"Unexpected aggregate: {rows}")
        # A small aggregated demo is collected to the driver, then committed atomically.
        # Large production facts should use a staging + bulk-load strategy instead.
        with psycopg2.connect("") as connection:
            with connection.cursor() as cursor:
                cursor.execute("LOCK TABLE demo.job_counts IN EXCLUSIVE MODE")
                cursor.execute("DELETE FROM demo.job_counts")
                execute_values(cursor,
                    "INSERT INTO demo.job_counts(location, posting_count) VALUES %s", rows)
                cursor.execute("SELECT location, posting_count FROM demo.job_counts ORDER BY location")
                if cursor.fetchall() != rows:
                    raise AssertionError("PostgreSQL results differ from Delta Gold")
        print("SMOKE TEST PASSED: Bronze=5, Silver=3, Gold=2 locations, PostgreSQL=2 rows")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

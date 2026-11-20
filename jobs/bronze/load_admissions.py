
import hashlib
import os
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# =========================================================
# 1. CONFIGURATION
# =========================================================

RAW_DIR = Path("/opt/airflow/data/education")

BRONZE_ADMISSIONS = (
    "s3a://bronze/education/university_admissions"
)

BRONZE_DATES = (
    "s3a://bronze/education/admission_dates"
)

YEARS = range(2019, 2027)

# Tên cột đúng theo file CSV thực tế
ADMISSION_COLUMNS = [
    "university_code",
    "university_name",
    "university_type",
    "location",
    "major_code",
    "major_name",
    "education_level",
    "program_name",
    "program_type",
    "language",
    "duration_years",
    "admission_year",
    "quota",
    "admission_methods",
    "tuition_fee",
    "major_group_code",
    "major_group_name",
]

DATE_COLUMNS = [
    "university_code",
    "university_name",
    "admission_year",
    "announcement_date",
]

# Mapping CSV -> Bronze theo ERD
ADMISSION_MAPPING = {
    "university_code": "university_code_raw",
    "university_name": "university_name_raw",
    "university_type": "university_type_raw",
    "location": "province_raw",
    "major_code": "major_code_raw",
    "major_name": "major_name_raw",
    "education_level": "education_level_raw",
    "program_name": "program_name_raw",
    "program_type": "program_type_raw",
    "language": "language_raw",
    "duration_years": "duration_raw",
    "admission_year": "admission_year_raw",
    "quota": "quota_raw",
    "admission_methods": "admission_method_raw",
    "tuition_fee": "tuition_fee_raw",
    "major_group_code": "field_group_code_raw",
    "major_group_name": "field_group_raw",
}

DATE_MAPPING = {
    "university_code": "university_code_raw",
    "university_name": "university_name_raw",
    "admission_year": "admission_year_raw",
    "announcement_date": "announcement_date_raw",
}


# =========================================================
# 2. SPARK SESSION
# =========================================================

def get_spark():

    access_key = os.environ["MINIO_ROOT_USER"]
    secret_key = os.environ["MINIO_ROOT_PASSWORD"]

    return (
        SparkSession.builder
        .appName("EducationRawToBronze")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem"
        )
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            "http://minio:9000"
        )
        .config(
            "spark.hadoop.fs.s3a.access.key",
            access_key
        )
        .config(
            "spark.hadoop.fs.s3a.secret.key",
            secret_key
        )
        .config(
            "spark.hadoop.fs.s3a.path.style.access",
            "true"
        )
        .config(
            "spark.hadoop.fs.s3a.connection.ssl.enabled",
            "false"
        )
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        )
        .getOrCreate()
    )


# =========================================================
# 3. CSV READER
# =========================================================

def read_raw_csv(spark, file_path, expected_columns):

    if not file_path.is_file():
        raise FileNotFoundError(
            f"RAW file not found: {file_path}"
        )

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("encoding", "UTF-8")
        .option("mode", "FAILFAST")
        .csv(str(file_path))
    )

    # Không cho phép thiếu hoặc thừa cột
    if df.columns != expected_columns:
        raise ValueError(
            f"Invalid columns in {file_path.name}\n"
            f"Expected: {expected_columns}\n"
            f"Actual: {df.columns}"
        )

    return df


# =========================================================
# 4. ADD BRONZE METADATA
# =========================================================

def prepare_bronze(df, mapping, source_file, batch_id):

    # Đổi tên cột theo thiết kế Bronze
    df = df.select(
        *[
            F.col(source).alias(target)
            for source, target in mapping.items()
        ]
    )

    # raw_id là ID kỹ thuật của bản ghi trong lần nạp
    # Full overwrite: ID có thể thay đổi giữa các lần chạy
    df = df.withColumn(
        "raw_id",
        F.sha2(
            F.concat_ws(
                "|",
                F.lit(batch_id),
                F.lit(source_file),
                F.monotonically_increasing_id().cast("string")
            ),
            256
        )
    )

    df = (
        df
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn(
            "ingestion_batch_id",
            F.lit(batch_id)
        )
        .withColumn(
            "source_file",
            F.lit(source_file)
        )
    )

    return df.select(
        "raw_id",
        *list(mapping.values()),
        "ingested_at",
        "ingestion_batch_id",
        "source_file"
    )


# =========================================================
# 5. TASK 1: UNIVERSITY ADMISSIONS
# =========================================================

def load_university_admissions(**context):

    spark = get_spark()

    try:
        batch_id = hashlib.sha256(
            context["run_id"].encode("utf-8")
        ).hexdigest()[:24]

        frames = []

        for year in YEARS:

            file_path = RAW_DIR / f"admission_{year}.csv"

            print(f"Reading {file_path.name}")

            df = read_raw_csv(
                spark,
                file_path,
                ADMISSION_COLUMNS
            )

            df = prepare_bronze(
                df,
                ADMISSION_MAPPING,
                file_path.name,
                batch_id
            )

            frames.append(df)

        # Gộp 8 năm vào một bảng Bronze
        df_bronze = frames[0]

        for df in frames[1:]:
            df_bronze = df_bronze.unionByName(df)

        print("Writing university_admissions...")

        (
            df_bronze.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(BRONZE_ADMISSIONS)
        )

        print("University admissions Bronze completed.")

        result = (
            spark.read.format("delta")
            .load(BRONZE_ADMISSIONS)
        )

        print(f"Total records: {result.count()}")
        result.printSchema()
        result.show(5, truncate=False)

    finally:
        spark.stop()


# =========================================================
# 6. TASK 2: ADMISSION DATES
# =========================================================

def load_admission_dates(**context):

    spark = get_spark()

    try:
        batch_id = hashlib.sha256(
            context["run_id"].encode("utf-8")
        ).hexdigest()[:24]

        file_path = RAW_DIR / "admission_dates.csv"

        df = read_raw_csv(
            spark,
            file_path,
            DATE_COLUMNS
        )

        df_bronze = prepare_bronze(
            df,
            DATE_MAPPING,
            file_path.name,
            batch_id
        )

        print("Writing admission_dates...")

        (
            df_bronze.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(BRONZE_DATES)
        )

        print("Admission dates Bronze completed.")

        result = (
            spark.read.format("delta")
            .load(BRONZE_DATES)
        )

        print(f"Total records: {result.count()}")
        result.printSchema()
        result.show(5, truncate=False)

    finally:
        spark.stop()


# =========================================================
# 7. AIRFLOW DAG
# =========================================================


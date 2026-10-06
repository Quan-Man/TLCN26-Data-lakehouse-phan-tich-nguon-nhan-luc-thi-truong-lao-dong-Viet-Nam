# dags/raw_to_bronze_to_silver.py

from datetime import datetime
from pathlib import Path
import re
import unicodedata

from airflow import DAG
from airflow.operators.python import PythonOperator

from pyspark.sql import SparkSession
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window


# =========================================================
# PATH CONFIG
# =========================================================

RAW_PATH = "/opt/airflow/data/education/admission_2024.csv"

BRONZE_PATH = "/opt/airflow/data/bronze/admission"

SILVER_PATH = "/opt/airflow/data/silver/admission"


# =========================================================
# SPARK SESSION
# =========================================================

def get_spark():
    return (
        SparkSession.builder
        .appName("AirflowLakehouse")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .getOrCreate()
    )


# =========================================================
# NORMALIZE COLUMN NAME
# =========================================================

def normalize_column_name(column_name):

    # Bỏ dấu tiếng Việt
    normalized = unicodedata.normalize("NFD", column_name)

    normalized = "".join(
        c for c in normalized
        if unicodedata.category(c) != "Mn"
    )

    # Xử lý đ / Đ
    normalized = (
        normalized
        .replace("đ", "d")
        .replace("Đ", "D")
        .lower()
        .strip()
    )

    # Thay ký tự đặc biệt bằng _
    normalized = re.sub(
        r"[^a-zA-Z0-9_]",
        "_",
        normalized
    )

    # Gộp nhiều dấu _ liên tiếp
    normalized = re.sub(
        r"_+",
        "_",
        normalized
    )

    return normalized.strip("_")


# =========================================================
# TASK 1: RAW -> BRONZE
# =========================================================

def load_to_bronze():

    spark = get_spark()

    try:
        print("======================================")
        print("TASK 1: RAW -> BRONZE")
        print("======================================")

        # -------------------------------------------------
        # Kiểm tra file RAW
        # -------------------------------------------------

        raw_file = Path(RAW_PATH)

        if not raw_file.exists():
            raise FileNotFoundError(
                f"Không tìm thấy file RAW: {RAW_PATH}"
            )

        print(f"RAW file: {RAW_PATH}")

        # -------------------------------------------------
        # Đọc CSV
        # -------------------------------------------------

        print("Đọc dữ liệu RAW...")

        df = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(RAW_PATH)
        )

        print("Schema gốc:")
        df.printSchema()

        print("Tên cột gốc:")
        print(df.columns)

        # -------------------------------------------------
        # Chuẩn hóa tên cột
        # -------------------------------------------------

        print("Chuẩn hóa tên cột...")

        for old_col in df.columns:

            new_col = normalize_column_name(old_col)

            if old_col != new_col:

                print(
                    f"Rename: {old_col} -> {new_col}"
                )

                df = df.withColumnRenamed(
                    old_col,
                    new_col
                )

        print("Tên cột sau chuẩn hóa:")
        print(df.columns)

        # -------------------------------------------------
        # Tạo thư mục Bronze nếu chưa có
        # -------------------------------------------------

        print("Kiểm tra thư mục BRONZE...")

        bronze_path = Path(BRONZE_PATH)

        bronze_path.mkdir(
            parents=True,
            exist_ok=True
        )

        print(
            f"BRONZE path đã sẵn sàng: {BRONZE_PATH}"
        )

        # -------------------------------------------------
        # Ghi Delta
        # -------------------------------------------------

        print("Ghi dữ liệu vào BRONZE...")

        (
            df.write
            .format("delta")
            .mode("overwrite")
            .save(BRONZE_PATH)
        )

        print("Load Bronze thành công!")

        print("Dữ liệu Bronze:")
        df.show(5, truncate=False)

    finally:

        spark.stop()


# =========================================================
# TASK 2: BRONZE -> SILVER
# =========================================================

def transform_to_silver():

    spark = get_spark()

    try:
        print("======================================")
        print("TASK 2: BRONZE -> SILVER")
        print("======================================")

        # -------------------------------------------------
        # Kiểm tra Bronze
        # -------------------------------------------------

        bronze_path = Path(BRONZE_PATH)

        if not bronze_path.exists():
            raise FileNotFoundError(
                f"Không tìm thấy Bronze: {BRONZE_PATH}"
            )

        # -------------------------------------------------
        # Đọc Bronze
        # -------------------------------------------------

        print("Đọc dữ liệu từ BRONZE...")

        df = (
            spark.read
            .format("delta")
            .load(BRONZE_PATH)
        )

        print("Schema Bronze:")
        df.printSchema()

        # -------------------------------------------------
        # Tạo STT
        # -------------------------------------------------

        print("Thêm cột STT...")

        #
        # Ưu tiên sắp xếp theo dữ liệu thật
        # thay vì monotonically_increasing_id()
        #
        sort_columns = []

        if "ma_truong" in df.columns:
            sort_columns.append("ma_truong")

        if "nam_ts" in df.columns:
            sort_columns.append("nam_ts")

        if "ma_nganh" in df.columns:
            sort_columns.append("ma_nganh")

        # Nếu không tìm thấy các cột trên
        # thì dùng cột đầu tiên làm điều kiện sort
        if not sort_columns:
            sort_columns = [df.columns[0]]

        print(
            f"Sắp xếp STT theo: {sort_columns}"
        )

        window = Window.orderBy(
            *sort_columns
        )

        df_silver = df.withColumn(
            "stt",
            row_number().over(window)
        )

        # Đưa STT lên đầu
        columns = [
            "stt"
        ] + [
            col
            for col in df_silver.columns
            if col != "stt"
        ]

        df_silver = df_silver.select(
            *columns
        )

        print("Dữ liệu SILVER:")

        df_silver.show(
            5,
            truncate=False
        )

        # -------------------------------------------------
        # Tạo thư mục Silver nếu chưa có
        # -------------------------------------------------

        print("Kiểm tra thư mục SILVER...")

        silver_path = Path(SILVER_PATH)

        silver_path.mkdir(
            parents=True,
            exist_ok=True
        )

        print(
            f"SILVER path đã sẵn sàng: {SILVER_PATH}"
        )

        # -------------------------------------------------
        # Ghi Silver
        # -------------------------------------------------

        print("Ghi dữ liệu vào SILVER...")

        (
            df_silver.write
            .format("delta")
            .mode("overwrite")
            .save(SILVER_PATH)
        )

        print("ETL Silver thành công!")

    finally:

        spark.stop()


# =========================================================
# AIRFLOW DAG
# =========================================================

with DAG(
    dag_id="raw_bronze_silver_pipeline",

    description="Load RAW -> Bronze -> Silver",

    start_date=datetime(
        2026,
        1,
        1
    ),

    schedule=None,

    catchup=False,

    tags=[
        "lakehouse",
        "bronze",
        "silver",
        "pyspark",
        "delta",
    ],

) as dag:

    bronze_task = PythonOperator(
        task_id="load_raw_to_bronze",
        python_callable=load_to_bronze,
    )

    silver_task = PythonOperator(
        task_id="etl_bronze_to_silver",
        python_callable=transform_to_silver,
    )

    bronze_task >> silver_task
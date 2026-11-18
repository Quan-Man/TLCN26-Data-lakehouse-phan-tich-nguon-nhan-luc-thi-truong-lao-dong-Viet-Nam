"""Compatibility imports for schema-driven Spark readers."""
from .storage import read_csv, read_json, read_parquet, read_dataset


def add_ingestion_metadata(df, *, source_name: str, run_id: str, source_file=None):
    """Attach source/run metadata to a Spark DataFrame before transformations."""
    from pyspark.sql import functions as F
    if not source_name.strip() or not run_id.strip():
        raise ValueError("source_name and run_id must be non-empty")
    new_columns = {"source_name", "source_file", "_run_id", "_ingested_at"}
    collisions = new_columns.intersection(df.columns)
    if collisions:
        raise ValueError(f"Metadata columns already exist: {sorted(collisions)}")
    return (df.withColumn("source_name", F.lit(source_name))
            .withColumn("source_file", F.lit(source_file) if source_file is not None else F.input_file_name())
            .withColumn("_run_id", F.lit(run_id))
            .withColumn("_ingested_at", F.current_timestamp()))


def add_ingestion_metadata(df, *, source_name: str, run_id: str, source_file=None):
    """Thêm metadata tại lúc đọc file nguồn, trước shuffle/merge.

    Dùng _ingested_at/_run_id để không ghi đè timestamp thu thập của nguồn.
    source_file bỏ trống -> input_file_name(); dữ liệu không từ file cần truyền rõ.
    """
    from pyspark.sql import functions as F
    if not source_name.strip() or not run_id.strip():
        raise ValueError("source_name và run_id không được rỗng")
    new_columns = {"source_name", "source_file", "_run_id", "_ingested_at"}
    collisions = new_columns.intersection(df.columns)
    if collisions:
        raise ValueError(f"Metadata đã tồn tại: {sorted(collisions)}; cần xử lý rõ trước khi thêm")
    return (df.withColumn("source_name", F.lit(source_name))
            .withColumn("source_file", F.lit(source_file) if source_file is not None else F.input_file_name())
            .withColumn("_run_id", F.lit(run_id))
            .withColumn("_ingested_at", F.current_timestamp()))

"""Đọc StructType JSON; không coi văn bản checks trong JSON là code thực thi."""
from .settings import load_json


def load_spark_schema(path, table_name=None):
    from pyspark.sql.types import StructType
    data = load_json(path)
    if table_name is not None:
        try:
            data = data["tables"][table_name]["spark_schema"]
        except (KeyError, TypeError) as exc:
            raise ValueError(f"Không có tables[{table_name!r}].spark_schema") from exc
    if not isinstance(data, dict) or data.get("type") != "struct":
        raise ValueError("Cần Spark StructType JSON; truyền table_name với file nhiều bảng")
    return StructType.fromJson(data)


def require_columns(df, columns):
    missing = sorted(set(columns) - set(df.columns))
    if missing:
        raise ValueError(f"Thiếu cột: {missing}")


def quoted_col(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def validate_primary_key(df, keys):
    """Action Spark: từ chối NULL, khóa chuỗi rỗng và khóa trùng."""
    from functools import reduce
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType
    if not keys or len(set(keys)) != len(keys):
        raise ValueError("Phải cung cấp danh sách khóa khác rỗng, không lặp")
    require_columns(df, keys)
    invalid = []
    types = {field.name: field.dataType for field in df.schema.fields}
    for key in keys:
        col = F.col(quoted_col(key))
        condition = col.isNull()
        if isinstance(types[key], StringType):
            condition = condition | (F.trim(col) == "")
        invalid.append(condition)
    if df.filter(reduce(lambda a, b: a | b, invalid)).limit(1).count():
        raise ValueError(f"Khóa NULL/rỗng: {keys}")
    count_name = "__common_key_count"
    while count_name in keys:
        count_name += "_"
    grouped = df.groupBy(*[F.col(quoted_col(k)) for k in keys]).agg(F.count(F.lit(1)).alias(count_name))
    if grouped.filter(F.col(count_name) > 1).limit(1).count():
        raise ValueError(f"Trùng khóa: {keys}; cần quy tắc chọn bản ghi trước khi ghi")

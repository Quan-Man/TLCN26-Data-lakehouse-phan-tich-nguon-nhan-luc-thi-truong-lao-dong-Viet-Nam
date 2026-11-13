# -*- coding: utf-8 -*-
"""Build Economy Gold Delta dimensions and exactly two facts from three Silver Delta datasets.

Spark 3.5 / Delta 3.3. No Bronze/Silver Hive registration.

Usage:
    python /opt/airflow/jobs/gold/build_economy_gold.py

Optional environment:
    ECONOMY_SILVER_PREFIX=s3a://silver/economic
    ECONOMY_GOLD_PREFIX=s3a://gold/economic
    # Candidate does not register Hive tables
    HIVE_METASTORE_URI=thrift://hive-metastore:9083
    ECONOMY_GOLD_DATABASE=economy_gold
    ECONOMY_GOLD_ALLOW_UNKNOWN_INDICATORS=false
    ECONOMY_GOLD_STRICT_DQ=true
    ECONOMIC_PROTECT_EXISTING=false  # default: allow rerun of Economy Gold Delta

IMPORTANT: Indicator mapping is fail-closed. Geographic names not explicitly
recognized are flagged UNVERIFIED, not silently labeled PROVINCE.
"""
from __future__ import annotations

import os
import re
import argparse
from datetime import date
from functools import reduce

from pyspark.sql import DataFrame, SparkSession, Window, functions as F, types as T

SILVER = "s3a://silver/economic"
GOLD = "s3a://gold/economic"
REGISTER_HIVE = False  # Only the explicit --register-only mode connects to Hive
HIVE_URI = os.getenv("HIVE_METASTORE_URI", "thrift://hive-metastore:9083")
GOLD_DB = os.getenv("ECONOMY_GOLD_DATABASE", "economic")
SHARED_LOCATION = "s3a://gold/shared/dim_location"  # READ ONLY; do not write
STRICT_DQ = os.getenv("ECONOMY_GOLD_STRICT_DQ", "true").lower() == "true"
ALLOW_UNKNOWN = os.getenv("ECONOMY_GOLD_ALLOW_UNKNOWN_INDICATORS", "false").lower() == "true"

# Mapping rules are intentionally conservative. Extend with actual Silver indicator_name
# values, after inspecting the output of print_indicator_inventory().
# group: economic | enterprise | labor
# aggregation_method: sum | average | weighted_average | none
# The derived fields growth_rate / absolute_change are calculated only when
# consecutive annual values exist at the exact same dimensional grain.
INDICATOR_RULES = {
    # Economic indicators in the actual Silver inventory.
    "agriculture_share": ("economic", "none"),
    "agriculture_value": ("economic", "sum"),
    "cpi": ("economic", "none"),
    "gdp_index": ("economic", "none"),
    "gdp_total_share": ("economic", "none"),
    "gdp_total_value": ("economic", "sum"),
    "iip": ("economic", "none"),
    "industry_construction_share": ("economic", "none"),
    "industry_construction_value": ("economic", "sum"),
    "product_tax_share": ("economic", "none"),
    "product_tax_value": ("economic", "sum"),
    "retail_sales": ("economic", "sum"),
    "service_share": ("economic", "none"),
    "service_value": ("economic", "sum"),
    # Enterprise observations belong to the economic fact.
    "enterprise_count": ("enterprise", "sum"),
    "investment": ("enterprise", "sum"),
    "new_enterprises": ("enterprise", "sum"),
    "project_count": ("enterprise", "sum"),
    "registered_capital_usd_million": ("enterprise", "sum"),
    # Labour indicators belong to the labour fact.
    "average_monthly_income": ("labor", "average"),
    "employment": ("labor", "sum"),
    "labour_force": ("labor", "sum"),
    "productivity": ("labor", "none"),
    "share": ("labor", "none"),
    "trained_labour_rate": ("labor", "none"),
    "underemployment_rate": ("labor", "none"),
    "unemployment_rate": ("labor", "none"),
}



# Same Spark / Delta / MinIO / Hive configuration as Education.
def spark_session(register_hive: bool = False) -> SparkSession:
    access_key = os.environ["MINIO_ROOT_USER"]
    secret_key = os.environ["MINIO_ROOT_PASSWORD"]
    spark = (
        SparkSession.builder
        .appName("DW_GOLD_Economy")
        .master("local[1]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.codegen.wholeStage", "false")
        .config("spark.sql.codegen.factoryMode", "NO_CODEGEN")
        .config("spark.sql.ansi.enabled", "false")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def hash_key(*columns: str):
    # Null-safe and unambiguous deterministic SHA256 surrogate key.
    chunks = [F.when(F.col(c).isNull(), F.lit("N:"))
              .otherwise(F.concat(F.lit("S:"), F.length(F.col(c).cast("string")).cast("string"),
                                  F.lit(":"), F.col(c).cast("string"))) for c in columns]
    return F.sha2(F.concat_ws("|", *chunks), 256)


def required(df: DataFrame, cols: list[str], name: str):
    missing = sorted(set(cols) - set(df.columns))
    if missing:
        raise ValueError(f"{name} missing required columns: {missing}; actual={df.columns}")


def read_silver(spark: SparkSession, table: str) -> DataFrame:
    path = f"{SILVER}/{table}"
    print(f"Reading Silver {path}", flush=True)
    return spark.read.format("delta").load(path)


def normalize_text(df: DataFrame, col: str) -> DataFrame:
    if col in df.columns:
        return df.withColumn(col, F.when(F.trim(F.col(col).cast("string")) == "", None)
                             .otherwise(F.trim(F.col(col).cast("string"))))
    return df.withColumn(col, F.lit(None).cast("string"))


def prepare(df: DataFrame, group: str) -> DataFrame:
    id_col = {"economic": "economic_statistic_id", "labor": "labour_statistic_id", "enterprise": "enterprise_statistic_id"}[group]
    required(df, [id_col, "indicator_id", "indicator_name", "location_id",
                  "location_name", "year", "indicator_value", "unit", "source_file"], group)
    for c in ["industry_id", "industry_name", "occupation_id", "occupation_name",
              "residence_type_id", "residence_type_name", "source_raw_id", "loaded_at", "value_status", "indicator_type", "industry_scope", "occupation_scope", "residence_type_scope", "location_level"]:
        df = normalize_text(df, c) if c != "loaded_at" else (
            df if c in df.columns else df.withColumn(c, F.lit(None).cast("timestamp")))
    # Silver must explicitly classify residence scope; do not guess from NULLs.
    if df.filter(F.col("residence_type_scope").isNull()).limit(1).count():
        raise ValueError(f"{group}: missing residence_type_scope; rebuild Silver first")
    df = (df.withColumn("economic_statistic_id", F.col(id_col))
          .withColumn("indicator_name", F.lower(F.trim(F.col("indicator_name"))))
          .withColumn("year", F.col("year").cast("int"))
          .withColumn("indicator_value", F.col("indicator_value").cast("double"))
          .withColumn("_source_group", F.lit(group)))
    if df.filter(F.col("economic_statistic_id").isNull() | F.col("year").isNull() |
                 F.col("indicator_id").isNull() | F.col("location_id").isNull()).limit(1).count():
        raise ValueError(f"{group}: null required identifier or year")
    if df.filter((F.col("year") < 1900) | (F.col("year") > 2100)).limit(1).count():
        raise ValueError(f"{group}: invalid year")
    return df


def print_indicator_inventory(df: DataFrame):
    print("Silver indicator inventory (check INDICATOR_RULES):", flush=True)
    df.groupBy("_source_group", "indicator_name", "unit").count().orderBy(
        "_source_group", "indicator_name").show(500, truncate=False)


def indicator_rules(spark: SparkSession) -> DataFrame:
    rows = [(k, v[0], v[1]) for k, v in sorted(INDICATOR_RULES.items())]
    return spark.createDataFrame(rows, "indicator_name string, indicator_group string, aggregation_method string")


def enforce_unique(df: DataFrame, columns: list[str], name: str):
    if df.groupBy(*columns).count().filter("count > 1").limit(1).count():
        df.groupBy(*columns).count().filter("count > 1").show(20, truncate=False)
        raise ValueError(f"Duplicate keys in {name}: {columns}")


def canonical_labels(df: DataFrame, id_col: str, name_col: str, label: str) -> DataFrame:
    """Resolve case/whitespace-only label variants, preserve original IDs.

    Real conflicting names still fail instead of silently merging distinct labels.
    """
    d = (
        df.where(F.col(id_col).isNotNull())
        .select(
            F.col(id_col),
            F.regexp_replace(F.trim(F.col(name_col)), r"\s+", " ").alias(name_col),
        )
        .withColumn(
            name_col,
            F.when(F.col(name_col) == "", F.lit(None).cast("string"))
            .otherwise(F.col(name_col)),
        )
        .withColumn("_normalized_name", F.lower(F.col(name_col)))
    )
    conflicts = (
        d.where(F.col("_normalized_name").isNotNull())
        .groupBy(id_col)
        .agg(F.countDistinct("_normalized_name").alias("n"))
        .where(F.col("n") > 1)
    )
    if conflicts.limit(1).count():
        print(f"Conflicting labels for {label}; review Silver mappings:", flush=True)
        (d.join(conflicts.select(id_col), id_col, "inner")
          .select(id_col, name_col).distinct()
          .orderBy(id_col, name_col).show(50, truncate=False))
        raise ValueError(f"{label}: one ID maps to multiple different labels")

    # Prefer a conventional title-case label when available, otherwise choose
    # a deterministic spelling. This does not change any source identifier.
    preferred = F.when(
        F.col(name_col) == F.initcap(F.lower(F.col(name_col))), F.lit(0)
    ).otherwise(F.lit(1))
    from pyspark.sql.window import Window
    w = Window.partitionBy(id_col).orderBy(
        F.col(name_col).isNull().asc(), preferred.asc(), F.col(name_col).asc()
    )
    return (
        d.withColumn("_rn", F.row_number().over(w))
        .where(F.col("_rn") == 1)
        .select(id_col, name_col)
    )


def make_dim_date(df: DataFrame) -> DataFrame:
    # Annual observations are represented by Jan 1; not actual announcement dates.
    years = df.select("year").distinct()
    d = years.withColumn("full_date", F.make_date(F.col("year"), F.lit(1), F.lit(1)))
    return d.select(
        F.date_format("full_date", "yyyyMMdd").cast("int").alias("date_key"),
        "full_date", F.dayofmonth("full_date").alias("day_number"),
        F.date_format("full_date", "EEEE").alias("day_name"),
        F.weekofyear("full_date").alias("week_number"),
        F.month("full_date").alias("month_number"),
        F.date_format("full_date", "MMMM").alias("month_name"),
        F.quarter("full_date").alias("quarter_number"),
        F.year("full_date").alias("year_number"),
        F.date_format("full_date", "yyyy-MM").alias("year_month"),
        F.concat(F.year("full_date"), F.lit("-Q"), F.quarter("full_date")) .alias("year_quarter"),
        F.dayofweek("full_date").isin(1, 7).alias("is_weekend"))


# Explicit geographic labels; unknown labels stay UNVERIFIED for manual review.
# Province labels reflect raw naming, not a claim about 2025/2026 administrative boundaries.
KNOWN_REGIONS = (
    "đồng bằng sông hồng", "trung du và miền núi phía bắc", "bắc trung bộ và duyên hải miền trung",
    "tây nguyên", "đông nam bộ", "đồng bằng sông cửu long",
    "bắc trung bộ", "duyên hải nam trung bộ", "bắc trung bộ và duyên hải trung bộ",
    "trung du và miền núi bắc bộ", "đồng bằng sông hồng và vùng phụ cận",
)
KNOWN_PROVINCES = (
    "hà nội", "tp. hà nội", "tp. hồ chí minh", "thành phố hồ chí minh", "hồ chí minh",
    "hải phòng", "đà nẵng", "cần thơ", "huế", "thừa thiên huế", "an giang", "bà rịa - vũng tàu",
    "bắc giang", "bắc kạn", "bạc liêu", "bắc ninh", "bến tre", "bình định", "bình dương",
    "bình phước", "bình thuận", "cà mau", "cao bằng", "đắk lắk", "đắk nông", "điện biên",
    "đồng nai", "đồng tháp", "gia lai", "hà giang", "hà nam", "hà tĩnh", "hải dương",
    "hậu giang", "hòa bình", "hưng yên", "khánh hòa", "kiên giang", "kon tum", "lai châu",
    "lâm đồng", "lạng sơn", "lào cai", "long an", "nam định", "nghệ an", "ninh bình",
    "ninh thuận", "phú thọ", "phú yên", "quảng bình", "quảng nam", "quảng ngãi",
    "quảng ninh", "quảng trị", "sóc trăng", "sơn la", "tây ninh", "thái bình", "thái nguyên",
    "thanh hóa", "tiền giang", "trà vinh", "tuyên quang", "vĩnh long", "vĩnh phúc", "yên bái",
)


def make_dim_location(df: DataFrame) -> DataFrame:
    """Candidate dimension, not an independently published location dimension."""
    d = canonical_labels(df, "location_id", "location_name", "dim_location candidates")
    return d.select(
        hash_key("location_id").alias("location_key"), "location_id",
        F.lit(None).cast("string").alias("location_code"), "location_name",
        F.when(F.lower(F.trim(F.col("location_name"))).isin("việt nam", "viet nam", "cả nước"), "COUNTRY")
         .when(F.lower(F.trim(F.col("location_name"))).isin(*KNOWN_REGIONS), "REGION")
         .when(F.lower(F.trim(F.col("location_name"))).isin(*KNOWN_PROVINCES), "PROVINCE")
         .otherwise("UNVERIFIED").alias("location_level"),
        F.when(F.lower(F.trim(F.col("location_name"))).isin(*KNOWN_PROVINCES),
               F.col("location_name")).otherwise(F.lit(None).cast("string")).alias("province_name"),
        F.lit(None).cast("string").alias("economic_region"),
        F.lit(None).cast("date").alias("valid_from"),
        F.lit(None).cast("date").alias("valid_to"),
        F.lit(None).cast("boolean").alias("is_current"))


def shared_location_mapping(spark: SparkSession, candidates: DataFrame):
    """Use exact name matching, matching Education's shared-location convention.

    Do not overwrite shared dimension. Refuse ambiguous or missing mappings.
    """
    path = spark._jvm.org.apache.hadoop.fs.Path(SHARED_LOCATION)
    fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
    if not fs.exists(path):
        raise RuntimeError("Shared dim_location does not exist. Prepare and approve a shared-location mapping before Gold build; refusing to initialize it.")
    shared = spark.read.format("delta").load(SHARED_LOCATION)
    required(shared, ["location_key", "location_name"], "shared dim_location")
    mapping = shared.select("location_name", "location_key").where(
        F.col("location_name").isNotNull() & F.col("location_key").isNotNull()).distinct()
    names = candidates.select("location_name").distinct()
    ambiguous = mapping.join(names, "location_name", "inner").groupBy(
        "location_name").count().where("count > 1")
    if ambiguous.limit(1).count():
        ambiguous.show(30, truncate=False)
        raise ValueError("Ambiguous shared location versions: explicit historical mapping required")
    missing = names.join(mapping, "location_name", "left_anti")
    if missing.limit(1).count():
        missing.show(30, truncate=False)
        raise ValueError("Unmapped locations; review Gold dim_location_candidates and update shared dim_location")
    return shared, mapping


# Technical members are dimension rows, not actual occupations/industries.
# Keep raw TOTAL observations in the facts, without counting them as occupations.
TOTAL_LABELS = ("tổng số", "tong so", "tổng cộng", "tong cong", "cả nước", "ca nuoc")
SPECIAL_NAMES = {
    "industry": {"ALL_INDUSTRIES": "Tất cả ngành (tổng hợp)",
                 "NOT_APPLICABLE": "Không phân theo ngành", "UNKNOWN": "Chưa xác định ngành"},
    "occupation": {"ALL_OCCUPATIONS": "Tất cả nghề (tổng hợp)",
                   "NOT_APPLICABLE": "Không phân theo nghề", "UNKNOWN": "Chưa xác định nghề"},
    "residence": {"ALL_RESIDENCE_TYPES": "Tất cả khu vực cư trú (tổng hợp)",
                  "NOT_APPLICABLE": "Không phân theo khu vực cư trú", "UNKNOWN": "Chưa xác định khu vực cư trú"},
}


def is_total(name_col: str):
    return F.lower(F.trim(F.col(name_col))).isin(*TOTAL_LABELS)


def special_dim(df: DataFrame, kind: str, id_col: str, name_col: str, key_col: str) -> DataFrame:
    # Filter aggregate labels from the actual member list; append technical members.
    actual = canonical_labels(
        df.filter(F.col(id_col).isNotNull() & F.col(name_col).isNotNull()
                  & ~is_total(name_col)), id_col, name_col, f"dim_{kind}")
    actual = actual.select(hash_key(id_col).alias(key_col), name_col, id_col)
    spark = df.sparkSession
    rows = [(code, label) for code, label in SPECIAL_NAMES[kind].items()]
    technical = spark.createDataFrame(rows, [id_col, name_col])
    technical = technical.select(hash_key(id_col).alias(key_col), name_col, id_col)
    return actual.unionByName(technical)


def make_dim_industry(df: DataFrame) -> DataFrame:
    d = special_dim(df, "industry", "industry_id", "industry_name", "industry_key")
    return d.select("industry_key", "industry_id", "industry_name")


def make_dim_occupation(df: DataFrame) -> DataFrame:
    return special_dim(df, "occupation", "occupation_id", "occupation_name", "occupation_key")


def make_dim_residence(df: DataFrame) -> DataFrame:
    d = special_dim(df, "residence", "residence_type_id", "residence_type_name", "residence_type_key")
    return d.select("residence_type_key", "residence_type_id", "residence_type_name")


def mapped_id(df: DataFrame, id_col: str, name_col: str, scope_col: str,
              all_code: str, specific_code: str):
    # A source NULL is a missing breakdown, NOT a claim about a specific member.
    # Fail on inconsistent scope rather than silently inventing a category.
    scope = F.upper(F.trim(F.coalesce(F.col(scope_col), F.lit(""))))
    total = is_total(name_col)
    missing = F.col(name_col).isNull()
    return (F.when(total | scope.isin("ALL", all_code), F.lit(all_code))
            .when(missing & scope.isin("", "NOT_APPLICABLE"), F.lit("NOT_APPLICABLE"))
            .when(scope == "UNKNOWN", F.lit("UNKNOWN"))
            .when(~missing & F.col(id_col).isNotNull() &
                  scope.isin("", "SPECIFIC", specific_code), F.col(id_col))
            .otherwise(F.lit(None).cast("string")))


def assign_dimension_keys(df: DataFrame) -> DataFrame:
    # All three scopes are authoritative from Silver.
    d = df.withColumn("_mapped_industry_id", mapped_id(
        df, "industry_id", "industry_name", "industry_scope",
        "ALL_INDUSTRIES", "SPECIFIC_INDUSTRY"))
    d = d.withColumn("_mapped_occupation_id", mapped_id(
        d, "occupation_id", "occupation_name", "occupation_scope",
        "ALL_OCCUPATIONS", "SPECIFIC_OCCUPATION"))
    d = d.withColumn("_mapped_residence_id", mapped_id(
        d, "residence_type_id", "residence_type_name", "residence_type_scope",
        "ALL_RESIDENCE_TYPES", "SPECIFIC_RESIDENCE_TYPE"))
    for c in ("_mapped_industry_id", "_mapped_occupation_id", "_mapped_residence_id"):
        bad = d.filter(F.col(c).isNull())
        if bad.limit(1).count():
            bad.select("source_file", "indicator_name", "industry_name",
                       "occupation_name", "residence_type_name", c).show(20, False)
            raise ValueError(f"Cannot map {c}: review Silver scope/name consistency")
    return d


def make_dim_indicator(df: DataFrame) -> DataFrame:
    # Silver classifies indicator_type; Gold mapping supplies aggregation_method.
    d = df.select("indicator_id", "indicator_name", "unit", "indicator_group",
                  "indicator_type", "aggregation_method").distinct()
    enforce_unique(d, ["indicator_id"], "dim_economic_indicator metadata")
    if d.filter(F.col("indicator_type").isNull()).limit(1).count():
        raise ValueError("Silver indicator_type missing; rebuild Silver first")
    return d.select(hash_key("indicator_id").alias("indicator_key"), "indicator_id",
                    "indicator_name", "indicator_group", "indicator_type", "unit",
                    F.lit("year").alias("frequency"), "aggregation_method")


def fact_base(df: DataFrame) -> DataFrame:
    return (df.withColumn("date_key", F.concat(F.col("year").cast("string"), F.lit("0101")).cast("int"))
              .withColumn("industry_key", hash_key("_mapped_industry_id"))
              .withColumn("occupation_key", hash_key("_mapped_occupation_id"))
              .withColumn("residence_type_key", hash_key("_mapped_residence_id"))
              .withColumn("indicator_key", hash_key("indicator_id"))
              .withColumn("source_statistic_id", F.col("economic_statistic_id"))
              .withColumn("loaded_at", F.current_timestamp()))


def with_yoy(df: DataFrame, group_cols: list[str]) -> DataFrame:
    # Match the same dimensions in the immediately preceding calendar year.
    # Validate grain before this self-join to avoid row multiplication.
    prev = df.select(*[F.col(c).alias("p_" + c) for c in group_cols],
                     (F.col("year") + F.lit(1)).alias("p_next_year"),
                     F.col("indicator_value").alias("previous_year_value"))
    cond = [df[c].eqNullSafe(prev["p_" + c]) for c in group_cols]
    cond.append(df["year"] == prev["p_next_year"])
    joined = df.join(prev, reduce(lambda x, y: x & y, cond), "left")
    joined = joined.drop(*["p_" + c for c in group_cols], "p_next_year")
    # Percentage/rate/index measures must not be interpreted as generic YoY growth.
    not_comparable = (F.col("indicator_type").isin("RATE", "SHARE", "INDEX") |
                      (F.lower(F.trim(F.coalesce(F.col("unit"), F.lit("")))) == "%"))
    ready = (F.col("indicator_value").isNotNull() &
             F.col("previous_year_value").isNotNull() & ~not_comparable)
    return (joined
        .withColumn("absolute_change", F.when(ready,
            F.col("indicator_value") - F.col("previous_year_value")))
        .withColumn("growth_rate", F.when(ready & (F.col("previous_year_value") != 0),
            (F.col("indicator_value") - F.col("previous_year_value")) /
            F.abs(F.col("previous_year_value")) * F.lit(100.0)))
        .withColumn("yoy_status",
            F.when(F.col("indicator_value").isNull(), "MISSING_VALUE")
             .when(not_comparable, "NOT_APPLICABLE")
             .when(F.col("previous_year_value").isNull(), "NO_PREVIOUS_YEAR")
             .when(F.col("previous_year_value") == 0, "ZERO_BASE")
             .otherwise("CALCULATED")))


def build_economic_fact(df: DataFrame) -> DataFrame:
    econ = df.filter(F.col("_source_group").isin("economic", "enterprise"))
    enforce_unique(econ, ["indicator_id", "location_id", "industry_id", "year"], "economic fact grain")
    econ = with_yoy(econ, ["indicator_id", "location_id", "industry_id"])
    econ = assign_dimension_keys(econ)
    econ = fact_base(econ)
    return econ.select(hash_key("economic_statistic_id").alias("economic_fact_key"),
        "date_key", "location_key", "industry_key", "loaded_at", "indicator_key",
        "indicator_value", "growth_rate", "previous_year_value", "absolute_change",
        "value_status", "yoy_status", "source_statistic_id")


def build_labor_fact(df: DataFrame) -> DataFrame:
    labor = df.filter(F.col("_source_group") == "labor")
    grain = ["indicator_id", "location_id", "industry_id", "occupation_id", "residence_type_id"]
    enforce_unique(labor, grain + ["year"], "labor fact grain")
    labor = with_yoy(labor, grain)
    labor = assign_dimension_keys(labor)
    labor = fact_base(labor)
    # Labor share/productivity require validated denominators/numerators. NULL is deliberate.
    # The actual indicators (including labor_share and labor_productivity) remain in
    # indicator_key + indicator_value; do not double-count them as synthetic measures.
    return labor.select(hash_key("economic_statistic_id").alias("labor_fact_key"),
        "date_key", "location_key", "industry_key", "occupation_key", "residence_type_key",
        "indicator_key", "indicator_value", "previous_year_value", "growth_rate",
        "absolute_change", "value_status", "yoy_status", "source_statistic_id", "loaded_at")


def verify_fk(fact: DataFrame, dim: DataFrame, fk: str, pk: str, fact_name: str):
    missing = fact.filter(F.col(fk).isNotNull()).join(dim.select(pk),
        fact[fk] == dim[pk], "left_anti")
    if missing.limit(1).count():
        raise ValueError(f"{fact_name}: orphan FK {fk} -> {pk}")


def validate_outputs(tables: dict[str, DataFrame], total_silver: int):
    for name, df in tables.items():
        pk = df.columns[0]
        enforce_unique(df, [pk], name)
        if df.filter(F.col(pk).isNull()).limit(1).count():
            raise ValueError(f"{name}: NULL primary key")
    facts = ["fact_economic_indicator", "fact_labor_market"]
    total_fact = sum(tables[n].count() for n in facts)
    if total_fact != total_silver:
        raise ValueError(f"Silver count {total_silver} != fact count {total_fact}")
    for name in facts:
        f = tables[name]
        for fk, dim, pk in [("date_key", "dim_date", "date_key"),
                            ("location_key", "dim_location", "location_key"),
                            ("industry_key", "dim_industry", "industry_key"),
                            ("indicator_key", "dim_economic_indicator", "indicator_key")]:
            verify_fk(f, tables[dim], fk, pk, name)
    for fk, dim, pk in [("occupation_key", "dim_occupation", "occupation_key"),
                        ("residence_type_key", "dim_residence_type", "residence_type_key")]:
        verify_fk(tables["fact_labor_market"], tables[dim], fk, pk, "fact_labor_market")
    # Validate YoY nullability, and report coverage rather than treating NULL as 0.
    for name in facts:
        f = tables[name]
        bad = f.filter(
            F.col("previous_year_value").isNull()
            & (F.col("growth_rate").isNotNull() | F.col("absolute_change").isNotNull())
        )
        if bad.limit(1).count():
            raise ValueError(f"{name}: derived value exists without previous-year observation")
        print(f"YoY coverage for {name}:", flush=True)
        f.agg(
            F.count(F.lit(1)).alias("rows"),
            F.count("indicator_value").alias("numeric_values"),
            F.count("previous_year_value").alias("consecutive_previous_year_values"),
            F.count("growth_rate").alias("relative_growth_rates"),
            F.count("absolute_change").alias("absolute_changes"),
        ).show(truncate=False)
    print(f"DQ PASS: {total_silver} Silver observations -> {total_fact} fact observations", flush=True)


def _same_published_data(new: DataFrame, old: DataFrame) -> bool:
    """Avoid a Delta commit and new Parquet files on identical reruns.

    loaded_at is audit metadata, not a business-data change. Compare full row
    multisets (not just counts) to avoid silently missing corrections.
    """
    if new.schema != old.schema:
        return False
    cols = [c for c in new.columns if c != "loaded_at"]
    left = new.select(*cols)
    right = old.select(*cols)
    return (left.exceptAll(right).limit(1).count() == 0 and
            right.exceptAll(left).limit(1).count() == 0)


def write_gold(spark: SparkSession, tables: dict[str, DataFrame]):
    """Idempotent Delta publication; no directory deletion, no cache flush.

    Identical data leaves the existing Delta version and Parquet files untouched.
    Changed data is committed using Delta overwrite (not physical path deletion).
    """
    for name, df in tables.items():
        path = GOLD_TABLES["shared"][name] if name in GOLD_TABLES["shared"] else GOLD_TABLES["economic"][name]
        hp = spark._jvm.org.apache.hadoop.fs.Path(path)
        fs = hp.getFileSystem(spark._jsc.hadoopConfiguration())
        if fs.exists(hp):
            old = spark.read.format("delta").load(path)
            if _same_published_data(df, old):
                print(f"[NO CHANGE] {name}: keep existing Delta version and Parquet files", flush=True)
                continue
        print(f"[DELTA COMMIT] {name} -> {path}", flush=True)
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)


def build():
    if GOLD != "s3a://gold/economic" or SILVER != "s3a://silver/economic" or SHARED_LOCATION != "s3a://gold/shared/dim_location":
        raise RuntimeError("Unexpected official Gold configuration")
    spark = spark_session(register_hive=False)
    try:
        # Rerun policy: existing Economy Gold Delta tables can be overwritten.
        # Never remove directories or Delta logs. A non-Delta existing directory
        # is rejected to prevent silently replacing unrelated objects.
        # Set ECONOMIC_PROTECT_EXISTING=true to restore fail-closed behavior.
        protect_existing = os.getenv("ECONOMIC_PROTECT_EXISTING", "false").lower() == "true"
        for path in [*GOLD_TABLES["economic"].values(), *GOLD_TABLES["shared"].values()]:
            hp = spark._jvm.org.apache.hadoop.fs.Path(path)
            fs = hp.getFileSystem(spark._jsc.hadoopConfiguration())
            if fs.exists(hp):
                delta_log = spark._jvm.org.apache.hadoop.fs.Path(path.rstrip("/") + "/_delta_log")
                if not fs.exists(delta_log):
                    raise RuntimeError(f"Existing non-Delta path {path}; refusing overwrite")
                if protect_existing:
                    raise RuntimeError(f"Existing Gold path {path}; ECONOMIC_PROTECT_EXISTING=true")
                print(f"[RERUN] Existing Delta table will be overwritten: {path}", flush=True)
        print("[RERUN] Validated existing Gold paths. Shared source is read-only.", flush=True)
        economic = prepare(read_silver(spark, "economic_statistics"), "economic")
        labor = prepare(read_silver(spark, "labour_statistics"), "labor")
        enterprise = prepare(read_silver(spark, "enterprise_statistics"), "enterprise")
        total = economic.count() + labor.count() + enterprise.count()
        all_data = economic.unionByName(labor, allowMissingColumns=True).unionByName(
            enterprise, allowMissingColumns=True).cache()
        print_indicator_inventory(all_data)
        rules = indicator_rules(spark)
        all_data = all_data.withColumnRenamed("aggregation_method", "silver_aggregation_method")
        all_data = all_data.join(F.broadcast(rules), "indicator_name", "left")
        unknown = all_data.filter(F.col("indicator_group").isNull()).select(
            "_source_group", "indicator_name").distinct()
        if unknown.limit(1).count():
            print("Unmapped indicators; update INDICATOR_RULES:", flush=True)
            unknown.show(200, truncate=False)
            if not ALLOW_UNKNOWN:
                raise ValueError("Unknown indicators; mapping is required before Gold build")
        # Source table is authoritative for classification; indicator rules metadata
        # must agree, to avoid silently moving observations across facts.
        mismatch = all_data.filter(F.col("indicator_group").isNotNull() &
            (F.col("indicator_group") != F.col("_source_group")))
        if mismatch.limit(1).count():
            mismatch.select("_source_group", "indicator_name", "indicator_group").distinct().show(100)
            raise ValueError("Indicator mapping conflicts with Silver source group")
        all_data = all_data.withColumn("indicator_group", F.coalesce("indicator_group", "_source_group"))
        all_data = all_data.withColumn("aggregation_method", F.coalesce(
            "aggregation_method", F.col("silver_aggregation_method"), F.lit("none")))
        # The Silver semantic value_status must agree with the actual measurement.
        invalid_status = all_data.filter(
            F.col("value_status").isNull() |
            ((F.col("indicator_value").isNull()) & (F.col("value_status") != "MISSING_SOURCE")) |
            ((F.col("indicator_value").isNotNull()) & (F.col("value_status") != "VALID")))
        if invalid_status.limit(1).count():
            raise ValueError("Invalid Silver value_status / indicator_value combination")
        enforce_unique(all_data, ["economic_statistic_id"], "Silver union")
        candidates = make_dim_location(all_data)
        print("Geography level inventory (UNVERIFIED requires manual review):", flush=True)
        candidates.groupBy("location_level").count().show(truncate=False)
        unverified = candidates.filter(F.col("location_level") == "UNVERIFIED")
        if unverified.limit(1).count():
            unverified.select("location_name", "location_id").show(100, truncate=False)
            if STRICT_DQ:
                raise ValueError("Unverified geography labels: extend explicit geographic mapping before Gold write")
        # Production uses the shared dimension as authoritative, without writing to it.
        shared_location, location_mapping = shared_location_mapping(spark, candidates)
        all_data = all_data.join(F.broadcast(location_mapping), "location_name", "left").cache()
        if all_data.filter(F.col("location_key").isNull()).limit(1).count():
            raise ValueError("Missing shared location_key after mapping")
        # Economy-local reference snapshot, never writes to s3a://gold/shared/dim_location.
        economy_location_keys = all_data.select("location_key").distinct()
        local_location = shared_location.join(F.broadcast(economy_location_keys), "location_key", "inner")
        location_columns = ["location_key", "location_id", "location_code", "location_name",
                            "location_level", "province_name", "economic_region",
                            "valid_from", "valid_to", "is_current"]
        required(local_location, location_columns, "shared dim_location schema")
        local_location = local_location.select(*location_columns)
        tables = {
            "dim_date": make_dim_date(all_data),
            "dim_location": local_location,
            "dim_industry": make_dim_industry(all_data),
            "dim_economic_indicator": make_dim_indicator(all_data),
            "dim_occupation": make_dim_occupation(all_data),
            "dim_residence_type": make_dim_residence(all_data),
            "fact_economic_indicator": build_economic_fact(all_data),
            "fact_labor_market": build_labor_fact(all_data),
        }
        validate_outputs(tables, total)
        for name, df in tables.items():
            print(f"{name}: {df.count()} rows", flush=True)
        write_gold(spark, tables)
        print("Economy Gold build completed successfully", flush=True)
    finally:
        spark.stop()


GOLD_TABLES = {
    "economic": {
        "dim_date": f"{GOLD}/dim_date",
        "dim_industry": f"{GOLD}/dim_industry",
        "dim_economic_indicator": f"{GOLD}/dim_economic_indicator",
        "dim_occupation": f"{GOLD}/dim_occupation",
        "dim_residence_type": f"{GOLD}/dim_residence_type",
        "fact_economic_indicator": f"{GOLD}/fact_economic_indicator",
        "fact_labor_market": f"{GOLD}/fact_labor_market",
    },
    "shared": {"dim_location": f"{GOLD}/dim_location"},
}


def register_only():
    """Register Economy tables after successful independent DQ; never touch shared."""
    spark = spark_session(register_hive=True)
    try:
        schema = "economic"
        if not spark.catalog.databaseExists(schema):
            raise RuntimeError("Hive schema economic missing; create delta.economic first")
        for name, path in {**GOLD_TABLES["economic"], **GOLD_TABLES["shared"]}.items():
            spark.read.format("delta").load(path).limit(1).collect()
            spark.sql(f"CREATE TABLE IF NOT EXISTS `{schema}`.`{name}` USING DELTA LOCATION '{path}'")
            print(f"[HIVE] Checked {schema}.{name} -> {path}", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--register-only", action="store_true")
    args = parser.parse_args()
    if args.register_only:
        register_only()
    else:
        build()

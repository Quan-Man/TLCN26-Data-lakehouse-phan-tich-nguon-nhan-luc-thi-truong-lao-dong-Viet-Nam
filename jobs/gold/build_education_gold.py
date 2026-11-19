"""Build Education Gold Delta star schema from validated Education Silver.
Run: cd /opt/airflow && python -m jobs.gold.build_education_gold
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from jobs.gold.build_shared_dimensions import attach_date_key, location_normalize

SILVER = os.getenv('EDUCATION_SILVER_PREFIX', 's3a://silver/education').rstrip('/')
GOLD = os.getenv('EDUCATION_GOLD_PREFIX', 's3a://gold/education').rstrip('/')
SHARED_DATE = os.getenv('GOLD_DATE_PATH', 's3a://gold/shared/dim_date')
SHARED_LOCATION = os.getenv('GOLD_LOCATION_PATH', 's3a://gold/shared/dim_location')


# ---------------------------------------------------------
# Spark + Delta Lake + MinIO + Hive Metastore
# ---------------------------------------------------------
def spark_session():
    access_key = os.environ["MINIO_ROOT_USER"]
    secret_key = os.environ["MINIO_ROOT_PASSWORD"]

    spark = (
        SparkSession.builder
        .appName("DW_GOLD_Education")
        .master("local[2]")
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
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.ansi.enabled", "false")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# Bảng Gold cần đăng ký để Trino truy vấn bằng tên bảng.
GOLD_TABLES = {
    "education": {
        "dim_university": f"{GOLD}/dim_university",
        "dim_major": f"{GOLD}/dim_major",
        "dim_program": f"{GOLD}/dim_program",
        "fact_admission": f"{GOLD}/fact_admission",
    },
    "shared": {
        "dim_location": SHARED_LOCATION,
        "dim_date": SHARED_DATE,
    },
}


def register_gold_tables(spark):
    """Đăng ký các bảng Delta đã có vào Hive Metastore (không ghi lại dữ liệu)."""
    for schema, tables in GOLD_TABLES.items():
        # Không tự tạo schema ở warehouse local: cần schema có sẵn trong Hive.
        if not spark.catalog.databaseExists(schema):
            raise RuntimeError(
                f"Hive schema '{schema}' chưa tồn tại. "
                f"Hãy tạo schema qua Trino: CREATE SCHEMA IF NOT EXISTS delta.{schema}"
            )

        for table, path in tables.items():
            full_name = f"{schema}.{table}"
            print(f"[HIVE] Checking {full_name} -> {path}")
            spark.read.format("delta").load(path).limit(1).collect()
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {full_name}
                USING DELTA
                LOCATION '{path}'
            """)
            print(f"[HIVE] Registered {full_name}")

    print("[HIVE] All Gold tables registered successfully")


def read(spark, name):
    return spark.read.format('delta').load(f'{SILVER}/{name}')


def write(df, path, mode='overwrite'):
    (df.write.format('delta').mode(mode)
     .option('overwriteSchema', 'true' if mode == 'overwrite' else 'false')
     .save(path))
    print(f'[GOLD] Wrote {path}')


def assert_unique(df, key, label):
    if df.filter(F.col(key).isNull()).limit(1).count():
        raise ValueError(f'{label}: NULL key {key}')
    if df.groupBy(key).count().filter('count > 1').limit(1).count():
        raise ValueError(f'{label}: duplicate key {key}')


def require_complete_join(df, col, label):
    if df.filter(F.col(col).isNull()).limit(1).count():
        raise ValueError(f'{label}: missing {col}; refusing to write incorrect Gold')


def hash_key(col):
    return F.sha2(F.col(col).cast('string'), 256)


def location_dimension(spark, universities):
    """Read shared province keys; never create or modify shared dimensions."""
    shared = spark.read.format('delta').load(SHARED_LOCATION)
    required = {'location_key', 'location_id', 'province_name'}
    if not required.issubset(shared.columns):
        raise ValueError('Shared dim_location schema mismatch; run shared Gold first')
    assert_unique(shared, 'location_key', 'shared.dim_location')
    assert_unique(shared, 'location_id', 'shared.dim_location')
    lookup, keys = {}, {}
    def add(value, identity):
        if value:
            lookup.setdefault(location_normalize(value), set()).add(identity)
    for row in shared.select(*sorted(required)).collect():
        keys[row.location_id] = row.location_key
        if row.location_key != '0':
            add(row.province_name, row.location_id)
            add(row.location_id, row.location_id)
    root = os.getenv('SHARED_MAPPING_ROOT', '/opt/airflow/configs/mappings').rstrip('/')
    for row in spark.read.option('header', True).csv(f'{root}/location_aliases.csv').collect():
        r = row.asDict()
        if str(r.get('requires_context', '')).lower() in ('true', '1', 'yes'):
            continue
        if r['location_id'] in keys and keys[r['location_id']] != '0':
            add(r['alias'], r['location_id'])
    if 'LOC_VN63_79' in keys:
        add('Thành phố Hồ Chí Minh', 'LOC_VN63_79')
        add('TP. Hồ Chí Minh', 'LOC_VN63_79')
    rows, missing = [], []
    for row in universities.select('location_name').distinct().collect():
        name = row.location_name
        matches = lookup.get(location_normalize(name), set())
        if len(matches) != 1:
            missing.append((name, sorted(matches)))
        else:
            rows.append((name, keys[next(iter(matches))]))
    if missing:
        raise ValueError(f'Unmapped/ambiguous Education locations: {missing[:50]}')
    return spark.createDataFrame(rows, 'location_name string, location_key string')


def build(spark):
    universities = read(spark, 'universities')
    majors = read(spark, 'majors')
    programs = read(spark, 'programs')
    admissions = read(spark, 'admissions')
    announcements = read(spark, 'admission_announcements')
    groups = read(spark, 'quota_groups')

    for df, key, label in [(universities, 'university_id', 'universities'),
                           (majors, 'major_id', 'majors'),
                           (programs, 'program_id', 'programs'),
                           (admissions, 'admission_id', 'admissions'),
                           (announcements, 'announcement_id', 'announcements'),
                           (groups, 'quota_group_id', 'quota_groups')]:
        assert_unique(df, key, label)

    locations = location_dimension(spark, universities)
    dim_u = (universities.join(locations, 'location_name', 'left')
             .withColumn('university_key', hash_key('university_id'))
             .select('university_key', 'university_id', 'university_code',
                     'university_name', 'university_type', 'location_key'))
    require_complete_join(dim_u, 'location_key', 'dim_university')
    required_program_cols = {'program_id', 'university_id', 'major_id', 'admission_code', 'program_name', 'program_type', 'education_level', 'language', 'duration_years'}
    if not required_program_cols.issubset(programs.columns):
        raise ValueError(f'Silver programs missing required columns: {sorted(required_program_cols - set(programs.columns))}')
    dim_m = (majors.withColumn('major_key', hash_key('major_id'))
             .select('major_key', 'major_id', 'major_code', 'major_name',
                      'field_group_code', 'field_group_name', 'major_code_status'))
    dim_p = (programs.withColumn('program_key', hash_key('program_id'))
              .withColumn('major_key', hash_key('major_id'))
              .withColumn('university_key', hash_key('university_id'))
              .select('program_key', 'program_id', 'major_key', 'university_key',
                      'admission_code', 'program_name', 'program_type',
                      'education_level', 'language', 'duration_years'))
    for fk, dimension in [('major_key', dim_m), ('university_key', dim_u)]:
        if dim_p.where(F.col(fk).isNull()).limit(1).count() or \
                dim_p.join(dimension.select(fk), fk, 'left_anti').limit(1).count():
            raise ValueError(f'dim_program: invalid {fk}')

    # Shared calendar is the single source of date keys.
    dim_d = spark.read.format('delta').load(SHARED_DATE)
    assert_unique(dim_d, 'date_key', 'shared.dim_date')
    assert_unique(dim_d, 'full_date', 'shared.dim_date')
    if dim_d.filter((F.col('date_key') == 0) &
                    (F.col('full_date') == F.lit('1900-01-01').cast('date'))).count() != 1:
        raise ValueError('Shared dim_date missing Unknown date_key=0')
    if announcements.filter(F.col('announcement_date').isNotNull() &
                            F.col('announcement_date').cast('date').isNull()).limit(1).count():
        raise ValueError('Invalid announcement_date in Silver')

    # Prevent accidental row multiplication: university/year -> at most one date.
    ann = announcements.select('university_id', 'admission_year', 'announcement_date')
    if ann.groupBy('university_id', 'admission_year').count().filter('count > 1').limit(1).count():
        raise ValueError('Multiple announcement dates for one university/year; resolve before fact join')

    # Admission code is preserved in both fact and program dimension.
    # program -> university and major keys must be resolvable.
    p_fk = programs.select('program_id', 'university_id', 'major_id')
    f = (admissions.join(p_fk, 'program_id', 'left')
         .join(ann, ['university_id', 'admission_year'], 'left')
         .join(dim_u.select('university_id', 'university_key', 'location_key'), 'university_id', 'left')
         .withColumn('admission_fact_key', hash_key('admission_id'))
         .withColumn('program_key', hash_key('program_id'))
         .withColumn('major_key', hash_key('major_id')))
    f = attach_date_key(f, dim_d, 'announcement_date', 'date_key')
    for col in ('university_key', 'major_key', 'program_key', 'location_key'):
        require_complete_join(f, col, 'fact_admission')
    if f.count() != admissions.count():
        raise ValueError('Fact row count differs from Silver admissions; abort')
    for fk, dimension in [('major_key', dim_m), ('program_key', dim_p),
                          ('university_key', dim_u)]:
        if f.join(dimension.select(fk), fk, 'left_anti').limit(1).count():
            raise ValueError(f'fact_admission: {fk} not found in dimension')

    # Additive quota measure: one shared quota is allocated to ONE representative
    # admission row, not to every member. Keep original quota NULL for members.
    shared = f.filter(F.col('quota_group_id').isNotNull())
    group_stats = shared.groupBy('quota_group_id').agg(
        F.count('*').alias('member_count'),
        F.min('admission_id').alias('representative_admission_id'))
    g = groups.select('quota_group_id', 'group_quota',
                      F.col('university_id').alias('group_university_id'),
                      F.col('admission_year').alias('group_admission_year'))
    f = f.join(group_stats, 'quota_group_id', 'left').join(g, 'quota_group_id', 'left')
    if f.filter(F.col('quota_group_id').isNotNull() & (
            F.col('group_quota').isNull() |
            (F.col('university_id') != F.col('group_university_id')) |
            (F.col('admission_year') != F.col('group_admission_year')) |
            F.col('quota').isNotNull())).limit(1).count():
        raise ValueError('Shared quota members inconsistent with quota_groups')
    if groups.join(group_stats, 'quota_group_id', 'left_anti').limit(1).count():
        raise ValueError('A quota group has no member admissions')
    f = (f.withColumn('quota_total_contribution',
            F.when(F.col('quota_group_id').isNull(), F.col('quota'))
             .when(F.col('admission_id') == F.col('representative_admission_id'), F.col('group_quota'))
             .otherwise(F.lit(0)))
          # Fee midpoint and spread; missing/invalid amounts stay NULL.
          # For explicitly free tuition, the interval is exactly 0.
          .withColumn('tuition_avg',
              F.when(F.col('tuition_is_free') == True, F.lit(0.0))
               .when(F.col('tuition_min').isNotNull() & F.col('tuition_max').isNotNull()
                     & F.col('tuition_unit').isNotNull()
                     & (F.col('tuition_min') >= 0)
                     & (F.col('tuition_max') >= F.col('tuition_min')),
                     (F.col('tuition_min') + F.col('tuition_max')) / F.lit(2.0)))
          .withColumn('loaded_at', F.current_timestamp())
         .select('admission_fact_key', 'admission_id', 'date_key', 'university_key',
                 'major_key', 'location_key', 'program_key', 'admission_year',
                 'admission_code', 'quota', 'quota_status', 'quota_group_id',
                  'quota_total_contribution', 'tuition_min', 'tuition_max',
                 'tuition_unit', 'tuition_status', 'tuition_is_free',
                  'tuition_avg', 'loaded_at'))
    assert_unique(f, 'admission_fact_key', 'fact_admission')

    # Validate before publishing outputs; write fact last.
    for name, df, key in [('dim_university', dim_u, 'university_key'),
                          ('dim_major', dim_m, 'major_key'),
                          ('dim_program', dim_p, 'program_key')]:
        assert_unique(df, key, name)
        write(df, f'{GOLD}/{name}')
    write(f, f'{GOLD}/fact_admission')
    print(f'[GOLD] PASS: fact_admission = {f.count()} rows')


def main():
    """Build Gold hoặc chỉ đăng ký bảng đã có (dùng trong Airflow)."""
    import argparse

    parser = argparse.ArgumentParser(description="Education Gold Delta pipeline")
    parser.add_argument(
        "--register-only",
        action="store_true",
        help="Chỉ đăng ký bảng vào Hive Metastore, không build lại Gold",
    )
    args = parser.parse_args()

    spark = spark_session()
    try:
        if args.register_only:
            register_gold_tables(spark)
        else:
            print("[GOLD] Starting Gold warehouse build...")
            build(spark)
            print("[GOLD] Build finished; run Gold DQ before registration")
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
"""Education Bronze -> six Silver Delta datasets. Spark 3.5 / Delta 3.3."""
import os
import re
import hashlib
import unicodedata
from datetime import datetime
from pyspark.sql import SparkSession, functions as F, types as T, Window

BRONZE = os.getenv('EDUCATION_BRONZE_PREFIX', 's3a://bronze/education')
SILVER = os.getenv('EDUCATION_SILVER_PREFIX', 's3a://silver/education')
MAJOR_MAPPING_PATH = os.getenv('EDUCATION_MAJOR_MAPPING_PATH', '/opt/airflow/configs/mappings/major_mapping.csv')
MISSING = ['', 'null', 'none', 'nan', 'n/a', 'chưa có', 'chưa công bố', 'không rõ']


def spark_session():
    user = os.environ['MINIO_ROOT_USER']
    password = os.environ['MINIO_ROOT_PASSWORD']
    return (SparkSession.builder.appName('Education-Bronze-to-Silver').master('local[2]')
        .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
        .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
        .config('spark.hadoop.fs.s3a.endpoint', os.getenv('MINIO_ENDPOINT', 'http://minio:9000'))
        .config('spark.hadoop.fs.s3a.access.key', user)
        .config('spark.hadoop.fs.s3a.secret.key', password)
        .config('spark.hadoop.fs.s3a.path.style.access', 'true')
        .config('spark.hadoop.fs.s3a.connection.ssl.enabled', 'false')
        .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
        .config('spark.sql.shuffle.partitions', '8')
        .config('spark.sql.ansi.enabled', 'false')
        .getOrCreate())


def clean(c):
    s = F.regexp_replace(F.trim(F.col(c).cast('string')), r'\s+', ' ')
    return F.when(s.isNull() | F.lower(s).isin(*MISSING), F.lit(None).cast('string')).otherwise(s)


def key(*cols):
    # SHA256 identifiers; unambiguous null markers and length prefixes to avoid collisions.
    parts = [F.when(F.col(c).isNull(), F.lit('N:')).otherwise(F.concat(F.lit('S:'), F.length(F.col(c)).cast('string'), F.lit(':'), F.col(c))) for c in cols]
    return F.sha2(F.concat_ws('|', *parts), 256)


def latest_per(df, id_col, order_cols):
    # Canonical record selected reproducibly, favoring later admission year and source id.
    w = Window.partitionBy(id_col).orderBy(*[F.col(c).desc_nulls_last() for c in order_cols])
    return df.withColumn('_rn', F.row_number().over(w)).where('_rn = 1').drop('_rn')


def parse_tuition(v):
    """Normalize tuition, preserving original text and user-reviewed exceptions."""
    if v is None or not str(v).strip():
        return (None, None, None, 'MISSING', False)
    s = re.sub(r'\s+', ' ', str(v).strip().lower())
    if 'miễn học phí' in s or 'miễn 100% học phí' in s:
        return (0.0, 0.0, None, 'FREE', True)

    # Explicit, manually reviewed corrections. Never silently infer these
    # from the source string; keep tuition_fee_raw for traceability.
    if re.fullmatch(r'45\.000\.000\s*đồng/sinh viên\.?', s):
        return (45000000.0, 45000000.0, 'SEMESTER', 'PARSED', False)
    if re.fullmatch(r'28\.400\.000\s*[-–—]\s*34\.200\.000', s):
        return (28400000.0, 34200000.0, 'SEMESTER', 'PARSED', False)
    if re.fullmatch(r'3400\.000\s*đ/năm', s):
        return (34000000.0, 34000000.0, 'YEAR', 'PARSED', False)

    if ('theo quy định' in s or 'nhân 2.2' in s or 'nhân 2,2' in s
            or ('hoặc' in s and 'chương trình' in s)):
        return (None, None, None, 'REVIEW', False)

    amount_re = r'(?<![\d.,])\d{1,3}(?:\.\d{3})+(?!\d)'
    # A secondary annual price is explicitly given in parentheses.
    annual = re.search(r'\(\s*(' + amount_re + r')\s*(?:đ|vnđ|vnd)?\s*/\s*năm\s*\)', s)
    if annual:
        value = float(annual.group(1).replace('.', ''))
        return (value, value, 'YEAR', 'PARSED', False)

    if re.search(r'/\s*tchp|/\s*tín chỉ', s):
        unit = 'CREDIT'
    elif re.search(r'/\s*học kỳ|/\s*kỳ', s):
        unit = 'SEMESTER'
    elif re.search(r'/\s*tháng', s):
        unit = 'MONTH'
    elif re.search(r'/\s*năm|\s+đồng/năm', s):
        unit = 'YEAR'
    elif re.search(r'/\s*(?:toàn\s*)?kh(?:óa|oá|oa)', s):
        unit = 'COURSE'
    else:
        unit = None

    if unit is None:
        return (None, None, None, 'REVIEW', False)

    primary = s.split('(')[0].strip()
    values = [float(x.replace('.', '')) for x in re.findall(amount_re, primary)]
    if not values or len(values) > 2:
        # Support explicit textual million units, e.g. 45 triệu đồng/năm.
        m = re.fullmatch(r'(\d+(?:[.,]\d+)?)\s*triệu\s*(?:đồng|đ|vnđ)?\s*/\s*năm', primary)
        if m:
            amount = float(m.group(1).replace(',', '.')) * 1000000
            return (amount, amount, 'YEAR', 'PARSED', False)
        return (None, None, unit, 'REVIEW', False)
    is_range = bool(re.search(r'\s(?:-|–|—|~|đến)\s', primary))
    if len(values) == 2 and not is_range:
        return (None, None, unit, 'REVIEW', False)
    if len(values) == 1 and is_range:
        return (None, None, unit, 'REVIEW', False)
    return (min(values), max(values), unit,
            'ESTIMATED' if 'dự kiến' in s or 'khoảng' in s else 'PARSED', False)

TUITION_SCHEMA = T.StructType([
    T.StructField('minimum', T.DoubleType()), T.StructField('maximum', T.DoubleType()),
    T.StructField('unit', T.StringType()), T.StructField('status', T.StringType()),
    T.StructField('free', T.BooleanType())])
TUITION_UDF = F.udf(parse_tuition, TUITION_SCHEMA)



def apply_major_mapping(spark, admissions):
    """Map university + original admission code + year without multiplying admissions."""
    required = [
        'university_code', 'university_name', 'source_major_code', 'major_name',
        'admission_years', 'total_admission_records',
        'major_code_from_images_2021', 'mapping_status', 'mapping_note',
    ]
    mapping = (spark.read.option('header', True).option('encoding', 'UTF-8')
               .option('multiLine', True).option('quote', '"').option('escape', '"')
               .csv(MAJOR_MAPPING_PATH))
    if set(mapping.columns) != set(required):
        raise ValueError(f'Mapping must have exactly 9 agreed columns; got {mapping.columns}')
    mapping = mapping.select(*required)
    mapping = (mapping
        .withColumn('map_university', F.upper(F.trim(F.col('university_code'))))
        .withColumn('map_source', F.upper(F.trim(F.col('source_major_code'))))
        .withColumn('map_status', F.upper(F.trim(F.col('mapping_status'))))
        .withColumn('map_candidate', F.upper(F.trim(F.col('major_code_from_images_2021')))))
    valid_status = ['VERIFIED', 'GROUP_CODE', 'NEEDS_REVIEW', 'NO_MATCH', 'AMBIGUOUS']
    if mapping.where(~F.col('map_status').isin(*valid_status) | F.col('map_status').isNull()).limit(1).count():
        raise ValueError('Unknown or missing mapping_status in major_mapping.csv')
    # Keep blank-source mapping rows: they can match by university + year +
    # normalized major name, but only when the admission code itself is blank.
    mapping = mapping.withColumn('map_major_name',
        F.lower(F.regexp_replace(F.trim(F.col('major_name')), r'\s+', ' ')))
    mapping = (mapping
        .withColumn('map_year', F.explode(F.split(F.coalesce(F.col('admission_years'), F.lit('')), r'\s*[,;]\s*')))
        .withColumn('map_year', F.trim(F.col('map_year'))))
    if mapping.where(~F.col('map_year').rlike(r'^\d{4}$')).limit(1).count():
        raise ValueError('Invalid admission_years value in major_mapping.csv')
    mapping = mapping.withColumn('map_year', F.col('map_year').cast('int'))
    dup = (mapping.where(F.col('map_source').isNotNull() & (F.col('map_source') != ''))
           .groupBy('map_university', 'map_source', 'map_year')
           .count().where(F.col('count') > 1).limit(5).collect())
    if dup:
        raise ValueError(f'Ambiguous mapping for university/code/year: {dup}')
    # Codes confirmed by the team's mapping: canonical 7-digit or 5-digit+letter+digit.
    # Program suffixes in mapping (_CLC, _TT, _UN, ...) are NOT part of major_code.
    # Preserve confirmed alphanumeric major codes (e.g. 75202A1) intact.
    mapped_raw = F.col('map_candidate')
    mapped_canonical = (F.when(mapped_raw.rlike(r'^\d{7}$|^\d{5}[A-Z]\d$'), mapped_raw)
        .when(mapped_raw.rlike(r'^\d{7}[_-][A-Z0-9]+$'), F.substring(mapped_raw, 1, 7)))
    mapping = mapping.withColumn('map_standard_code',
        F.when(F.col('map_status') == 'VERIFIED', mapped_canonical))
    invalid_verified = mapping.where(
        (F.col('map_status') == 'VERIFIED') & F.col('map_standard_code').isNull())
    if invalid_verified.limit(1).count():
        print(f'WARNING: VERIFIED mapping rows without recognized canonical code: {invalid_verified.count()}')

    verified_catalog = (mapping.where((F.col('map_status') == 'VERIFIED') &
                                      F.col('map_standard_code').isNotNull())
                        .select(F.col('map_standard_code').alias('_catalog_major_code'))
                        .distinct())

    # A blank-source VERIFIED mapping is eligible only for an admission whose
    # source code is missing. Require exactly one canonical major per school,
    # year and normalized name; otherwise fail rather than guessing.
    name_mapping = (mapping.where(
        (F.col('map_source').isNull() | (F.col('map_source') == '')) &
        (F.col('map_status') == 'VERIFIED') &
        F.col('map_standard_code').isNotNull() &
        F.col('map_major_name').isNotNull())
        .select(F.col('map_university').alias('_nm_university'),
                F.col('map_year').alias('_nm_year'),
                F.col('map_major_name').alias('_nm_major_name'),
                F.col('map_standard_code').alias('_nm_major_code'))
        .distinct())
    ambiguous_names = (name_mapping.groupBy('_nm_university', '_nm_year', '_nm_major_name')
                       .count().where('count > 1').limit(5).collect())
    if ambiguous_names:
        raise ValueError(f'Ambiguous blank-source major name mappings: {ambiguous_names}')

    code_mapping = (mapping.where(F.col('map_source').isNotNull() & (F.col('map_source') != ''))
                    .select('map_university', 'map_source', 'map_year',
                            'map_status', 'map_standard_code'))
    before_count = admissions.count()
    before_quota = admissions.select(F.sum(F.col('quota_raw').cast('long')).alias('q')).first()['q']
    result = admissions.join(code_mapping,
        (admissions['university_code_raw'] == code_mapping['map_university']) &
        (admissions['major_code_raw'] == code_mapping['map_source']) &
        (admissions['admission_year'] == code_mapping['map_year']), 'left')
    if result.count() != before_count:
        raise ValueError('Mapping JOIN changed the number of admission records')
    after_quota = result.select(F.sum(F.col('quota_raw').cast('long')).alias('q')).first()['q']
    if before_quota != after_quota:
        raise ValueError('Mapping JOIN changed the numeric admission quota total')

    # Fill missing source admission codes from uniquely verified name mapping.
    # This specifically covers QSQ / 2020 / Khoa học Dữ liệu -> 7460108,
    # without touching group codes or already populated admission codes.
    result = result.withColumn('_admission_major_name',
        F.lower(F.regexp_replace(F.trim(F.col('major_name_raw')), r'\s+', ' ')))
    result = result.join(F.broadcast(name_mapping),
        (result['university_code_raw'] == name_mapping['_nm_university']) &
        (result['admission_year'] == name_mapping['_nm_year']) &
        (result['_admission_major_name'] == name_mapping['_nm_major_name']) &
        result['major_code_raw'].isNull(), 'left')
    if result.count() != before_count:
        raise ValueError('Name fallback JOIN changed the number of admission records')
    result = (result
        .withColumn('major_code_raw', F.coalesce(F.col('major_code_raw'), F.col('_nm_major_code')))
        .withColumn('map_standard_code', F.coalesce(F.col('map_standard_code'), F.col('_nm_major_code')))
        .withColumn('map_status', F.when(F.col('_nm_major_code').isNotNull(), F.lit('VERIFIED'))
                    .otherwise(F.col('map_status'))))
    print('Verified missing admission codes filled by university/year/name mapping:',
          result.where(F.col('_nm_major_code').isNotNull()).count())

    raw = F.col('major_code_raw')
    # Priority: complete alphanumeric major > exact 7-digit major > major with
    # program suffix. Do not truncate 8-digit numeric codes or short school codes.
    # User-confirmed project rule: 7-digit base extracted from a program code
    # is a verified major; this is a dataset-specific convention, not proof
    # that every 7-digit number is an official nationwide major code.
    exact_major = raw.rlike(r'^\d{7}$|^\d{5}[A-Z]\d$')
    special_major_with_suffix = raw.rlike(r'^\d{5}[A-Z]\d[_-][A-Z0-9]+$')
    seven_digit_with_suffix = raw.rlike(r'^\d{7}(?:[_-][A-Z0-9]+|[A-ZÀ-Ỹ][A-Z0-9À-Ỹ]*)$')
    # Verified project-specific admission-code suffixes left unresolved by the
    # generic parser. Preserve the FULL admission_code; only major_code is cut.
    # QSK 2024: 7340120_408_IB, 7340201_404_IB
    # GHA 2020-2021: 7580201QT-01, 7580201QT-02
    # GSA 2025: 7580205-ĐS
    scoped_suffix = (
        ((F.col('university_code_raw') == 'QSK') &
         (F.col('admission_year') == 2024) &
         raw.rlike(r'^734(?:0120_408_IB|0201_404_IB)$')) |
        ((F.col('university_code_raw') == 'GHA') &
         F.col('admission_year').isin(2020, 2021) &
         raw.rlike(r'^7580201QT-0[12]$')) |
        ((F.col('university_code_raw') == 'GSA') &
         (F.col('admission_year') == 2025) &
         (raw == '7580205-ĐS'))
    )
    candidate_code = (F.when(exact_major, raw)
        .when(special_major_with_suffix, F.substring(raw, 1, 7))
        .when(seven_digit_with_suffix, F.substring(raw, 1, 7))
        .when(scoped_suffix, F.substring(raw, 1, 7)))
    result = result.withColumn('_candidate_major_code', candidate_code)
    result = result.join(F.broadcast(verified_catalog),
        result['_candidate_major_code'] == verified_catalog['_catalog_major_code'], 'left')
    confirmed_in_catalog = F.col('_catalog_major_code').isNotNull()
    confirmed_by_project_rule = exact_major | special_major_with_suffix | seven_digit_with_suffix | scoped_suffix

    # Never let a NULL map_standard_code erase a successfully extracted major.
    # GROUP_CODE is the exception: it intentionally has no canonical major.
    result = result.withColumn('major_code_normalized',
        F.when(F.col('map_status') == 'GROUP_CODE', F.lit(None).cast('string'))
         .otherwise(F.coalesce(F.col('map_standard_code'), F.col('_candidate_major_code'))))
    result = result.withColumn('_major_code_status',
        F.when(F.col('map_status').isin('GROUP_CODE', 'NO_MATCH', 'AMBIGUOUS'), F.col('map_status'))
         .when(F.col('map_status') == 'VERIFIED',
               F.when(F.col('major_code_normalized').isNotNull(), F.lit('VERIFIED'))
                .otherwise(F.lit('NEEDS_REVIEW')))
         .when(confirmed_in_catalog | confirmed_by_project_rule, F.lit('VERIFIED'))
         .otherwise(F.lit('NEEDS_REVIEW')))
    # Explicit NEEDS_REVIEW in mapping stays under review unless its candidate
    # has been independently confirmed by the catalog or project rule.
    print('Education major code extraction diagnostics:')
    result.select(
        F.count('*').alias('admission_rows'),
        F.sum(F.when(F.col('major_code_normalized').isNull(), 1).otherwise(0)).alias('null_major_codes'),
        F.sum(F.when(F.col('_major_code_status') == 'NEEDS_REVIEW', 1).otherwise(0)).alias('needs_review_rows')
    ).show(truncate=False)
    print('Education mapping statuses:')
    result.groupBy('_major_code_status').count().show(truncate=False)
    return result.drop('map_university', 'map_source', 'map_year',
                       'map_status', 'map_standard_code',
                       '_candidate_major_code', '_catalog_major_code',
                       '_nm_university', '_nm_year', '_nm_major_name', '_nm_major_code',
                       '_admission_major_name')


def build():
    spark = spark_session()
    try:
        b = spark.read.format('delta').load(f'{BRONZE}/university_admissions')
        dates = spark.read.format('delta').load(f'{BRONZE}/admission_dates')
        required = ['raw_id','university_code_raw','university_name_raw','university_type_raw','province_raw',
            'major_code_raw','major_name_raw','education_level_raw','program_name_raw','program_type_raw',
            'language_raw','duration_raw','admission_year_raw','quota_raw','admission_method_raw',
            'tuition_fee_raw','field_group_code_raw','field_group_raw']
        missing = set(required)-set(b.columns)
        if missing:
            raise ValueError(f'Bronze admissions missing columns: {sorted(missing)}')
        for name in required[1:]:
            b = b.withColumn(name, clean(name))
        b = b.withColumn('university_code_raw', F.upper(F.col('university_code_raw')))
        b = b.withColumn('major_code_raw', F.upper(F.col('major_code_raw')))
        b = b.withColumn('admission_year', F.col('admission_year_raw').cast('int'))
        if b.where(F.col('raw_id').isNull()).limit(1).count():
            raise ValueError('Bronze raw_id contains NULL')
        if b.groupBy('raw_id').count().where('count > 1').limit(1).count():
            raise ValueError('Bronze raw_id duplicated; fix ingestion first')
        if b.where(F.col('university_code_raw').isNull() | F.col('admission_year').isNull()).limit(1).count():
            raise ValueError('Invalid university_code/admission_year; quarantine records before Silver')
        b = apply_major_mapping(spark, b)
        # Normalize known aliases only; retain other raw geographic labels.
        loc = F.lower(F.col('province_raw'))
        b = b.withColumn('location_name', F.when(loc.isin('tp.hcm','tp. hcm','tp. hồ chí minh','tp hồ chí minh','thành phố hồ chí minh'), F.lit('Thành phố Hồ Chí Minh')).otherwise(F.col('province_raw')))
        b = b.withColumn('university_id', key('university_code_raw'))
        # Missing major codes never get fabricated; distinct name+university fallback only for technical identity.
        b = b.withColumn('_major_identity', F.when(F.col('major_code_normalized').isNotNull(), F.concat(F.lit('CODE:'), F.col('major_code_normalized')))
            .otherwise(F.concat(F.lit('UNRESOLVED:'), F.col('university_code_raw'), F.lit(':'),
                                F.coalesce(F.col('major_code_raw'), F.col('major_name_raw'), F.col('raw_id')))))
        b = b.withColumn('major_id', key('_major_identity'))
        # Include the original code so two distinct admissions/program codes
        # cannot collapse when they share the same canonical major code.
        b = b.withColumn('program_id', key('university_id','major_id','major_code_raw','program_name_raw','program_type_raw','education_level_raw','language_raw','duration_raw'))
        b = b.withColumn('admission_id', key('raw_id'))
        b = b.withColumn('duration_years', F.col('duration_raw').cast('double'))
        # The 2026 source explicitly marks 12 QSB international joint programs
        # with 'Chung chỉ tiêu 150'. They share ONE quota pool of 150 seats.
        # Match the original evidence and the known scope; never infer shared
        # quotas from numeric 150 alone.
        shared_codes = ['306','307','308','309','310','314','315','319','323','325','342','345']
        quota_text = F.lower(F.regexp_replace(F.trim(F.col('quota_raw')), r'\s+', ' '))
        shared_pattern = quota_text.rlike(r'^chung\s+chỉ\s+tiêu\s+150$')
        shared_scope = (
            (F.col('university_code_raw') == 'QSB') &
            (F.col('admission_year') == 2026) &
            F.col('major_code_raw').isin(*shared_codes) &
            (F.col('program_type_raw') == 'Chương trình liên kết quốc tế')
        )
        shared = shared_pattern & shared_scope
        # Refuse to silently leave unexpected shared-quota rows ungrouped.
        unexpected_shared = b.where(
            quota_text.rlike(r'chung\s+chỉ\s+tiêu') & ~shared
        ).select('university_code_raw','admission_year','major_code_raw',
                 'program_type_raw','quota_raw').limit(20).collect()
        if unexpected_shared:
            raise ValueError(f'Unmapped shared-quota rows: {unexpected_shared}')
        shared_rows = b.where(shared).select('major_code_raw').collect()
        observed_codes = [r['major_code_raw'] for r in shared_rows]
        if len(observed_codes) != len(shared_codes) or set(observed_codes) != set(shared_codes):
            raise ValueError(f'Expected 12 QSB shared quota rows, got {observed_codes}')
        group_id = key('university_code_raw', 'admission_year_raw', '_quota_group_name')
        b = b.withColumn('_quota_group_name', F.when(shared, F.lit('Chương trình liên kết quốc tế - chỉ tiêu chung 150')))
        b = b.withColumn('quota_group_id', F.when(shared, group_id))
        b = b.withColumn('quota', F.when(F.col('quota_raw').rlike(r'^\d+$'), F.col('quota_raw').cast('long')))
        b = b.withColumn('quota_status', F.when(shared, F.lit('SHARED'))
            .when(F.col('quota_raw').isNull(), F.lit('MISSING'))
            .when(F.col('quota').isNotNull(), F.lit('KNOWN'))
            .otherwise(F.lit('REVIEW')))
        b = b.withColumn('quota_note', F.when(shared, F.lit('12 chương trình liên kết quốc tế dùng chung 150 chỉ tiêu; không phân bổ theo từng chương trình')))
        b = b.withColumn('tuition_parsed', TUITION_UDF(F.col('tuition_fee_raw')))
        b = b.withColumn('processed_at', F.current_timestamp())
        universities = latest_per(b, 'university_id', ['admission_year','raw_id']).select(
            'university_id',F.col('university_code_raw').alias('university_code'),
            F.col('university_name_raw').alias('university_name'),F.col('university_type_raw').alias('university_type'),'location_name')
        majors_ranked = b.withColumn('_major_verified_rank',
            F.when(F.col('_major_code_status') == 'VERIFIED', F.lit(1)).otherwise(F.lit(0)))
        majors = latest_per(majors_ranked, 'major_id', ['_major_verified_rank','admission_year','raw_id']).select(
            'major_id',F.col('major_code_normalized').alias('major_code'),F.col('major_name_raw').alias('major_name'),
            F.col('field_group_code_raw').alias('field_group_code'),F.col('field_group_raw').alias('field_group_name'),
            F.col('_major_code_status').alias('major_code_status'))
        programs = latest_per(b, 'program_id', ['admission_year','raw_id']).select(
            'program_id','university_id','major_id',
            F.col('major_code_raw').alias('admission_code'),
            # Normalize displayed labels only; preserve source-based program_id.
            F.regexp_replace(
                F.regexp_replace(
                    F.regexp_replace(
                        F.col('program_name_raw'),
                        r'(?i)chương\s+trình\s+tiêu\s+chuẩn', 'Chương trình đại trà'),
                    r'(?i)chương\s+trình\s+chuẩn', 'Chương trình đại trà'),
                r'(?i)(?<!\w)chuẩn(?!\w)', 'đại trà'
            ).alias('program_name'),
            F.regexp_replace(
                F.regexp_replace(F.col('program_type_raw'),
                    r'(?i)chương\s+trình\s+tiêu\s+chuẩn', 'Chương trình đại trà'),
                    r'(?i)chương\s+trình\s+chuẩn', 'Chương trình đại trà'
            ).alias('program_type'),
            F.when(
                F.lower(F.trim(F.col('education_level_raw'))).isin(
                    'trường đại học chính quy', 'đại học chính quy'),
                F.lit('Đại học chính quy')
            ).otherwise(F.col('education_level_raw')).alias('education_level'),
            F.col('language_raw').alias('language'),'duration_years')
        admissions = b.select('admission_id','program_id','admission_year',
            F.col('major_code_raw').alias('admission_code'), # agreed source-code mapping
            F.col('major_code_raw').alias('source_major_code'),'quota','quota_status',
            'quota_group_id','quota_note',
            F.col('admission_method_raw').alias('admission_methods'),
            'tuition_fee_raw',F.col('tuition_parsed.minimum').alias('tuition_min'),
            F.col('tuition_parsed.maximum').alias('tuition_max'),
            F.col('tuition_parsed.unit').alias('tuition_unit'),
            F.col('tuition_parsed.status').alias('tuition_status'),
            F.col('tuition_parsed.free').alias('tuition_is_free'),
            F.col('raw_id').alias('source_raw_id'),'processed_at')
        # One group record for the entire shared quota pool, not one per major.
        groups = (b.where(F.col('quota_status') == 'SHARED')
            .select('quota_group_id','university_id','admission_year',
                F.col('_quota_group_name').alias('group_name'),
                F.lit(150).cast('long').alias('group_quota'),
                F.lit('SHARED').alias('quota_status'))
            .dropDuplicates(['quota_group_id']))
        if groups.count() != 1:
            raise ValueError('Expected exactly one 2026 QSB shared quota group')
        for c in ['university_code_raw','university_name_raw','admission_year_raw','announcement_date_raw']:
            if c not in dates.columns: raise ValueError(f'Bronze dates missing {c}')
            dates = dates.withColumn(c, clean(c))
        dates = dates.withColumn('university_code_raw',F.upper(F.col('university_code_raw')))
        dates = dates.withColumn('university_id',key('university_code_raw'))
        dates = dates.withColumn('admission_year',F.col('admission_year_raw').cast('int'))
        # Dates in source may use d/M/yyyy, dd/MM/yyyy, d-M-yyyy or ISO yyyy-MM-dd.
        # Build dates from numeric components instead of parsing incompatible patterns
        # (Spark 3.5 may raise DateTimeParseException for a nonmatching pattern).
        raw_date = F.trim(F.col('announcement_date_raw'))
        slash_or_dash = F.regexp_replace(raw_date, '-', '/')
        dmy = slash_or_dash.rlike(r'^\d{1,2}/\d{1,2}/\d{4}$')
        iso = raw_date.rlike(r'^\d{4}-\d{1,2}-\d{1,2}$')
        dmy_parts = F.split(slash_or_dash, '/')
        iso_parts = F.split(raw_date, '-')
        dates = dates.withColumn(
            'announcement_date',
            F.when(dmy, F.make_date(
                dmy_parts.getItem(2).cast('int'),
                dmy_parts.getItem(1).cast('int'),
                dmy_parts.getItem(0).cast('int')))
            .when(iso, F.make_date(
                iso_parts.getItem(0).cast('int'),
                iso_parts.getItem(1).cast('int'),
                iso_parts.getItem(2).cast('int')))
            .otherwise(F.lit(None).cast('date'))
        )
        dates = dates.withColumn('date_status',F.when(raw_date.isNull(),'MISSING').when(F.col('announcement_date').isNull(),'INVALID').otherwise('VALID'))
        dates = dates.withColumn('announcement_id',key('university_id','admission_year_raw','announcement_date_raw'))
        dates = dates.withColumn('processed_at',F.current_timestamp())
        invalid_dates = dates.filter(F.col('date_status') == 'INVALID').count()
        print(f'Announcement dates needing review: {invalid_dates}')
        announcements = latest_per(dates,'announcement_id',['announcement_date_raw']).select(
            'announcement_id','university_id','admission_year','announcement_date','date_status','processed_at')
        if dates.where(F.col('university_code_raw').isNull() | F.col('admission_year').isNull()).limit(1).count():
            raise ValueError('Invalid announcement university/year')
        # Save last after validating every input and deriving all datasets.
        tables = {'universities':universities,'majors':majors,'programs':programs,
                  'admissions':admissions,'quota_groups':groups,'admission_announcements':announcements}
        for name, frame in tables.items():
            path=f'{SILVER}/{name}'
            frame.write.format('delta').mode('overwrite').option('overwriteSchema','true').save(path)
            print(f'SILVER {name}: {spark.read.format("delta").load(path).count()} rows -> {path}')
    finally:
        spark.stop()


if __name__ == '__main__':
    build()

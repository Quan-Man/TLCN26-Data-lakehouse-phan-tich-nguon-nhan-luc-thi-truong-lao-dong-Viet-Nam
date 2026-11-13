"""Bronze -> 3 Silver Delta tables for 18 economy/labour CSV sources.
Preserves source identifiers, explicit missing values and semantic scope.
"""
import os
import re
import unicodedata
from pyspark.sql import SparkSession, functions as F, types as T

BRONZE_PATH = 's3a://bronze/economic'
SILVER_ROOT = 's3a://silver/economic'

# Classification is already stored in Bronze as data_group.
GROUPS = ('economic', 'labour', 'enterprise')

# Confirmed national-level source files. Keep this explicit rather than
# interpreting every missing location as Vietnam.
NATIONAL_SOURCE_GROUPS = {
    '1_GDP.csv': 'economic',
    '15_nghe_nghiep.csv': 'labour',
    '16_co_cau_lao_dong.csv': 'labour',
    '18_nang_suat.csv': 'labour',
    '11_fdi.csv': 'enterprise',
    '4_von_dau_tu.csv': 'enterprise',
}
NATIONAL_LOCATION_NAME = 'Việt Nam'

# These are known source labels, not inferred geographic locations.
NATIONAL_ALIASES = ('cả nước', 'ca nuoc', 'việt nam', 'viet nam', 'vn', 'toàn quốc', 'toan quoc')
REGION_NAMES = ('đồng bằng sông hồng', 'trung du và miền núi phía bắc',
    'bắc trung bộ và duyên hải miền trung', 'tây nguyên',
    'đông nam bộ', 'đồng bằng sông cửu long')
TOTAL_LABELS = ('tổng số', 'tong so', 'tổng cộng', 'total')
MISSING_TOKENS = ('', 'null', 'none', 'nan', 'n/a', 'chưa có',
                  'chưa công bố', 'không rõ', '-', '--')

HCMC_ALIASES = ('tp.hồ chí minh', 'tp. hồ chí minh', 'tp hồ chí minh',
                'thành phố hồ chí minh', 'hồ chí minh', 'ho chi minh')

# Generic indicator names require a source-specific meaning.
INDICATOR_BY_SOURCE = {
    ('14_thieu_viec.csv', 'rate'): 'underemployment_rate',
    ('7_that_nghiep.csv', 'rate'): 'unemployment_rate',
    ('17_dao_tao.csv', 'rate'): 'trained_labour_rate',
}




def make_spark():
    user = os.environ['MINIO_ROOT_USER']
    password = os.environ['MINIO_ROOT_PASSWORD']
    return (SparkSession.builder.appName('silver-economic-labour').master('local[2]')
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


def clean_string(name):
    x = F.trim(F.col(name).cast('string'))
    return F.when(x.isNull() | (x == '') | (F.lower(x).isin('null', 'none', 'nan', 'n/a')), F.lit(None)).otherwise(x)


def id_from(name, prefix):
    x = F.col(name)
    return F.when(x.isNotNull(), F.sha2(F.concat_ws('|', F.lit(prefix), F.lower(x)), 256))


def normalize_value(column):
    """Parse common numeric formats, rejecting ambiguous text rather than guessing."""
    x = F.regexp_replace(F.trim(F.col(column)), r'\s+', '')
    # decimal dot: 1234.56, thousands comma: 1,234.56
    dot = x.rlike(r'^[-+]?\d+(\.\d+)?$')
    comma_group = x.rlike(r'^[-+]?\d{1,3}(,\d{3})+(\.\d+)?$')
    # decimal comma: 12,5 (not ambiguous with thousands grouped as 1,234)
    decimal_comma = x.rlike(r'^[-+]?\d+,\d{1,2}$')
    return (F.when(dot, x.cast(T.DecimalType(24, 6)))
            .when(comma_group, F.regexp_replace(x, ',', '').cast(T.DecimalType(24, 6)))
            .when(decimal_comma, F.regexp_replace(x, ',', '.').cast(T.DecimalType(24, 6))))



# Mirrors raw-shaped Bronze tables; every value is still a source string.
TABLES = {1:'gdp',2:'gdp_growth',3:'new_enterprises',4:'investment',5:'labour_force',
 6:'employment',7:'unemployment',8:'income',9:'iip',10:'cpi',11:'fdi',
 12:'retail_sales',13:'enterprise_count',14:'underemployment',15:'occupation',
 16:'labour_structure',17:'training',18:'productivity'}
YEAR_COLUMN = re.compile(r'^(?P<measure>.+)_(?P<year>20\d{2})$')
META_COLUMNS = {'year','unit','notes','location','industry','occupation','area_type',
 'region','indicator','project_count_unit','capital_unit','employment_unit','share_unit','geography_version'}
FIXED_MEASURES = {1:{'gdp_total_value','agriculture_value','industry_construction_value',
 'service_value','product_tax_value','gdp_total_share','agriculture_share',
 'industry_construction_share','service_share','product_tax_share'},
 11:{'project_count','registered_capital_usd_million'}}
RAW_METADATA = {'raw_id','source_file','source_row_number','source_record_hash',
 'data_group','ingestion_batch_id','loaded_at'}

def read_raw_bronze(spark,root):
    parts=[]
    for number,table in TABLES.items():
        df=spark.read.format('delta').load(f'{root}/{table}')
        missing=RAW_METADATA-set(df.columns)
        if missing: raise ValueError(f'{table}: missing Bronze metadata: {sorted(missing)}')
        measures=[c for c in df.columns if c not in RAW_METADATA and c not in META_COLUMNS]
        if not measures: raise ValueError(f'{table}: no measurements')
        elements=[]
        for col in measures:
            match=YEAR_COLUMN.fullmatch(col)
            if match: measure,year=match.group('measure'),match.group('year')
            elif col in FIXED_MEASURES.get(number,set()): measure,year=col,None
            else: raise ValueError(f'Unexpected measurement column in {table}: {col}')
            year_expr=F.lit(year) if year else F.col('year').cast('string')
            if number==1: unit=F.lit('%') if col.endswith('_share') else (F.col('unit') if 'unit' in df.columns else F.lit(None).cast('string'))
            elif number==16: unit=F.coalesce(F.col('share_unit'),F.lit('%')) if col.startswith('share_') else F.coalesce(F.col('employment_unit'),F.col('unit') if 'unit' in df.columns else F.lit(None))
            elif number==11: unit=F.col('project_count_unit') if col=='project_count' else F.col('capital_unit')
            else: unit=F.col('unit') if 'unit' in df.columns else F.lit(None).cast('string')
            elements.append(F.struct(F.lit(measure).alias('indicator_name_raw'),
                F.lit(col).alias('indicator_code_raw'),F.col(col).cast('string').alias('indicator_value_raw'),
                unit.cast('string').alias('unit_raw'),year_expr.alias('year_raw')))
        # Keep genuinely missing year cells in otherwise populated source rows.
        # Drop wholly blank CSV padding rows before unpivoting.
        row_has_measure = None
        for measurement_col in measures:
            present = (F.col(measurement_col).isNotNull()
                       & (F.trim(F.col(measurement_col).cast('string')) != ''))
            row_has_measure = present if row_has_measure is None else (row_has_measure | present)
        def optional(c): return F.col(c).cast('string') if c in df.columns else F.lit(None).cast('string')
        loc=optional('indicator') if number==10 else optional('region') if number==14 else optional('location')
        residence=optional('area_type') if number==14 else F.lit(None).cast('string')
        # Preserve original source-row JSON for audit, including all original columns.
        original_cols=[c for c in df.columns if c not in RAW_METADATA]
        part=(df.withColumn('_measure',F.explode(F.array(*elements)))
            .filter(row_has_measure)
            .select(F.sha2(F.concat_ws('|',F.col('raw_id'),F.col('_measure.indicator_code_raw')),256).alias('raw_id'),
                F.col('raw_id').alias('source_raw_id'),
                F.col('_measure.indicator_name_raw').alias('indicator_name_raw'),
                F.col('_measure.indicator_code_raw').alias('indicator_code_raw'),
                F.col('_measure.indicator_value_raw').alias('indicator_value_raw'),
                F.col('_measure.unit_raw').alias('unit_raw'),loc.alias('location_raw'),
                F.col('_measure.year_raw').alias('year_raw'),optional('industry').alias('industry_raw'),
                optional('occupation').alias('occupation_raw'),residence.alias('residence_type_raw'),
                optional('geography_version').alias('geography_version_raw'),
                F.concat(F.lit('economic_'),F.lpad(F.lit(str(number)),2,'0')).alias('source_name'),
                'ingestion_batch_id','source_file','data_group',
                F.to_json(F.struct(*[F.col(c) for c in original_cols])).alias('raw_payload'),
                optional('notes').alias('notes'),'source_row_number',
                F.col('_measure.indicator_code_raw').alias('source_column'),
                'source_record_hash','loaded_at'))
        parts.append(part)
    result=parts[0]
    for part in parts[1:]: result=result.unionByName(part)
    return result

def run_silver(input_path=BRONZE_PATH, output_root=SILVER_ROOT):
    spark = make_spark()
    if input_path.rstrip('/') != BRONZE_PATH or output_root.rstrip('/') != SILVER_ROOT:
        raise ValueError('Only official economic Bronze/Silver paths are allowed')
    bronze = read_raw_bronze(spark, input_path)
    required = {
        'raw_id', 'indicator_name_raw', 'indicator_code_raw', 'indicator_value_raw',
        'unit_raw', 'location_raw', 'year_raw', 'industry_raw', 'occupation_raw',
        'residence_type_raw', 'geography_version_raw', 'source_name', 'source_file',
        'ingestion_batch_id', 'loaded_at'
    }
    missing_cols = sorted(required - set(bronze.columns))
    if missing_cols:
        raise ValueError(f'Bronze missing columns: {missing_cols}')

    if 'data_group' not in bronze.columns:
        raise ValueError('Bronze lacks data_group: rerun DAG 04_bronze_ingestion using updated Bronze job')
    invalid_groups = bronze.filter(
        F.col('data_group').isNull() | (~F.col('data_group').isin(*GROUPS))
    )
    if invalid_groups.limit(1).count():
        invalid_groups.select('source_file', 'data_group').distinct().show(30, truncate=False)
        raise ValueError('Bronze has missing/invalid data_group')
    bronze.groupBy('data_group').count().show(truncate=False)

    if bronze.filter(F.col('raw_id').isNull()).limit(1).count():
        raise ValueError('Bronze contains null raw_id')
    if bronze.groupBy('raw_id').count().filter('count > 1').limit(1).count():
        raise ValueError('Bronze contains duplicate raw_id')

    # Validate the national-level mapping before writing any Silver table.
    national_files = list(NATIONAL_SOURCE_GROUPS)
    national_rows = bronze.filter(F.col('source_file').isin(*national_files))
    wrong_group = national_rows.filter(
        ~F.col('data_group').eqNullSafe(
            F.create_map(*[
                element
                for filename, group in NATIONAL_SOURCE_GROUPS.items()
                for element in (F.lit(filename), F.lit(group))
            ])[F.col('source_file')]
        )
    )
    if wrong_group.limit(1).count():
        wrong_group.select('source_file', 'data_group').distinct().show(truncate=False)
        raise ValueError('National source file has unexpected data_group')

    # Do not overwrite an explicit province or other non-national location.
    national_location = F.lower(F.trim(F.col('location_raw').cast('string')))
    unexpected_location = national_rows.filter(
        national_location.isNotNull()
        & (~national_location.isin('', 'null', 'none', 'nan', 'n/a',
                                    'việt nam', 'viet nam', 'vn', 'toàn quốc', 'toan quoc'))
    )
    if unexpected_location.limit(1).count():
        unexpected_location.select('source_file', 'location_raw').distinct().show(30, truncate=False)
        raise ValueError('National source contains a non-national location: review input')

    # Canonicalize location BEFORE generating location_id.
    # In 14_thieu_viec.csv, region='Tổng số' means national aggregate;
    # area_type='CẢ NƯỚC' means all residence types, not a residence type.
    location_raw_clean = clean_string('location_raw')
    location_lower = F.lower(location_raw_clean)
    canonical_location = (F.when(
        F.col('source_file').eqNullSafe('14_thieu_viec.csv')
        & location_lower.eqNullSafe('tổng số'), F.lit(NATIONAL_LOCATION_NAME))
        .when(location_lower.isin(*NATIONAL_ALIASES), F.lit(NATIONAL_LOCATION_NAME))
        .when(location_lower.isin(*HCMC_ALIASES), F.lit('TP. Hồ Chí Minh'))
        .otherwise(location_raw_clean))

    indicator_source_map = F.create_map(*[
        item for (filename, indicator), canonical in INDICATOR_BY_SOURCE.items()
        for item in (F.lit(filename + '|' + indicator), F.lit(canonical))
    ])

    d = (bronze
         .withColumn('silver_group', F.col('data_group'))
         .withColumn('indicator_name_clean', clean_string('indicator_name_raw'))
         .withColumn('indicator_name_clean', F.coalesce(
             indicator_source_map[F.concat_ws('|', F.col('source_file'), F.col('indicator_name_clean'))],
             F.col('indicator_name_clean')))
         .withColumn('location_clean', canonical_location)
         .withColumn(
             'location_clean',
             F.when(
                 F.col('source_file').isin(*national_files),
                 F.lit(NATIONAL_LOCATION_NAME)
             ).otherwise(F.col('location_clean'))
         )
         .withColumn('industry_clean', clean_string('industry_raw'))
         .withColumn('occupation_clean', clean_string('occupation_raw'))
         .withColumn('residence_clean', clean_string('residence_type_raw'))
         .withColumn('residence_clean', F.when(
             F.col('source_file').eqNullSafe('14_thieu_viec.csv')
             & F.lower(F.col('residence_clean')).isin(*NATIONAL_ALIASES),
             F.lit(None).cast('string')).otherwise(F.col('residence_clean')))
         .withColumn('unit_clean', clean_string('unit_raw'))
         .withColumn('year_clean', clean_string('year_raw'))
         .withColumn('value_clean', clean_string('indicator_value_raw'))
         .withColumn('economic_statistic_id', F.col('raw_id'))
         .withColumn('labour_statistic_id', F.col('raw_id'))
         .withColumn('enterprise_statistic_id', F.col('raw_id'))
         .withColumn('indicator_id', id_from('indicator_name_clean', 'indicator'))
         .withColumn('location_id', id_from('location_clean', 'location'))
         .withColumn('industry_id', id_from('industry_clean', 'industry'))
         .withColumn('occupation_id', id_from('occupation_clean', 'occupation'))
         .withColumn('residence_type_id', id_from('residence_clean', 'residence'))
         .withColumn('year', F.col('year_clean').cast('int'))
         .withColumn('indicator_value', normalize_value('value_clean'))
         .withColumn('unit', F.when(F.col('indicator_name_clean').endswith('_share'), F.lit('%'))
                     .when(F.col('indicator_name_clean').isin('gdp_total_value', 'agriculture_value', 'industry_construction_value', 'service_value', 'product_tax_value'), F.lit('Tỷ đồng'))
                     .otherwise(F.col('unit_clean')))
         .withColumn('geography_version', clean_string('geography_version_raw'))
         .withColumn('period_type', F.lit('year'))
         .withColumn('indicator_name', F.col('indicator_name_clean'))
         .withColumn('location_name', F.col('location_clean'))
         .withColumn('industry_name', F.col('industry_clean'))
         .withColumn('occupation_name', F.col('occupation_clean'))
          .withColumn('residence_type_name', F.col('residence_clean'))
          # Extra semantic attributes; existing IDs and columns stay unchanged.
          .withColumn('source_location_name', clean_string('location_raw'))
          .withColumn('location_level',
              F.when(F.col('location_name') == NATIONAL_LOCATION_NAME, F.lit('COUNTRY'))
               .when(F.lower(F.col('location_name')).isin(*REGION_NAMES), F.lit('REGION'))
               .otherwise(F.lit('PROVINCE')))
          .withColumn('is_geographic_aggregate', F.col('location_level') != 'PROVINCE')
          .withColumn('industry_scope',
              F.when(F.col('industry_name').isNull(), F.lit('NOT_APPLICABLE'))
               .when(F.lower(F.col('industry_name')).isin(*TOTAL_LABELS), F.lit('ALL_INDUSTRIES'))
               .otherwise(F.lit('SPECIFIC_INDUSTRY')))
          .withColumn('occupation_scope',
              F.when(F.col('occupation_name').isNull(), F.lit('NOT_APPLICABLE'))
               .when(F.lower(F.col('occupation_name')).isin(*TOTAL_LABELS), F.lit('ALL_OCCUPATIONS'))
               .otherwise(F.lit('SPECIFIC_OCCUPATION')))
          .withColumn('residence_type_scope',
              F.when(
                  F.col('source_file').eqNullSafe('14_thieu_viec.csv')
                  & F.lower(F.trim(F.col('residence_type_raw'))).isin(*NATIONAL_ALIASES),
                  F.lit('ALL_RESIDENCE_TYPES'))
               .when(F.col('residence_type_name').isNull(), F.lit('NOT_APPLICABLE'))
               .when(F.lower(F.col('residence_type_name')).isin(*TOTAL_LABELS),
                     F.lit('ALL_RESIDENCE_TYPES'))
               .otherwise(F.lit('SPECIFIC_RESIDENCE_TYPE')))
          .withColumn('value_status',
              F.when(F.col('indicator_value').isNull(), F.lit('MISSING_SOURCE'))
               .otherwise(F.lit('VALID')))
          .withColumn('indicator_type',
              F.when(F.col('indicator_name').endswith('_share'), F.lit('SHARE'))
               .when(F.col('indicator_name').contains('rate'), F.lit('RATE'))
               .when(F.col('indicator_name').isin('gdp_index', 'iip', 'cpi'), F.lit('INDEX'))
               .when(F.col('indicator_name').rlike('(?i)capital|investment|income|gdp_total_value|_value$|retail_sales|productivity'), F.lit('AMOUNT'))
               .otherwise(F.lit('COUNT')))
          .withColumn('aggregation_method',
              F.when(F.col('indicator_type').isin('RATE', 'SHARE', 'INDEX'), F.lit('NONE'))
               .when(F.col('indicator_name') == 'productivity', F.lit('NONE'))
               .otherwise(F.lit('SUM'))))

    print('Geographic levels after normalization:')
    d.groupBy('location_level', 'location_name').count().orderBy('location_level', 'location_name').show(100, truncate=False)

    # Missing measurements are allowed: NULL is not zero and may mean unpublished.
    # Reject malformed non-empty numeric strings, invalid years and absent indicators.
    missing_tokens = MISSING_TOKENS
    raw_value = F.lower(F.trim(F.col('indicator_value_raw').cast('string')))
    value_is_missing = raw_value.isNull() | raw_value.isin(*missing_tokens)
    invalid = d.filter(
        F.col('year').isNull()
        | (~F.col('year').between(2000, 2100))
        | F.col('indicator_id').isNull()
        | (F.col('indicator_value').isNull() & ~value_is_missing)
    )
    invalid_count = invalid.count()
    if invalid_count:
        invalid.select('source_file', 'raw_id', 'indicator_name_raw',
                       'year_raw', 'indicator_value_raw').show(30, truncate=False)
        raise ValueError(f'{invalid_count} invalid observations (excluding genuine missing values)')

    print('Location NULL by source after national-level normalization:')
    (d.groupBy('source_file')
      .agg(F.count('*').alias('total'),
           F.sum(F.when(F.col('location_id').isNull(), 1).otherwise(0)).alias('null_location'))
      .orderBy('source_file').show(50, truncate=False))

    print('NULL overview (expected NULLs are allowed for non-applicable dimensions):')
    for group in GROUPS:
        subset = d.filter(F.col('silver_group') == group)
        subset.agg(*[
            F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c + '_null')
            for c in ('indicator_value', 'industry_id', 'occupation_id',
                      'residence_type_id', 'geography_version')
        ]).show(truncate=False)

    # A name and its ID must be present together (or both absent).
    for name, ident in (('location_name', 'location_id'),
                        ('indicator_name', 'indicator_id'),
                        ('industry_name', 'industry_id'),
                        ('occupation_name', 'occupation_id'),
                        ('residence_type_name', 'residence_type_id')):
        inconsistent = d.filter(F.col(name).isNull() != F.col(ident).isNull())
        if inconsistent.limit(1).count():
            raise ValueError(f'Inconsistent dimension pair: {name}/{ident}')

    null_count = d.filter(F.col('indicator_value').isNull()).count()
    print(f'Data quality warning: {null_count} missing indicator_value observations retained as NULL')

    # Same indicator, year, geography and dimensions from the same source must be unique.
    grain = ['silver_group', 'source_name', 'indicator_id', 'location_id',
             'industry_id', 'occupation_id', 'residence_type_id', 'year']
    duplicates = d.groupBy(*grain).count().filter(F.col('count') > 1)
    if duplicates.limit(1).count():
        duplicates.show(20, truncate=False)
        raise ValueError('Duplicate business grain in Silver source')
    print('Missing values by source:')
    d.groupBy('source_file', 'value_status').count().orderBy('source_file', 'value_status').show(100, truncate=False)

    # Preserve existing IDs and enrich Silver with readable attributes.
    # No indicator_code/location_code in Silver; preserve source columns in Bronze.
    provenance = ['source_raw_id', 'source_column', 'source_file',
                  'ingestion_batch_id', 'loaded_at']
    semantic_cols = ['source_location_name', 'location_level',
        'is_geographic_aggregate', 'industry_scope', 'occupation_scope',
        'residence_type_scope', 'value_status', 'indicator_type', 'aggregation_method']
    economic_cols = ['economic_statistic_id','indicator_id','indicator_name',
        'location_id','location_name','industry_id','industry_name',
        'period_type','year','indicator_value','unit','geography_version',
        'source_name'] + semantic_cols + provenance
    labour_cols = ['labour_statistic_id','indicator_id','indicator_name',
        'location_id','location_name','industry_id','industry_name',
        'occupation_id','occupation_name','residence_type_id','residence_type_name',
        'year','indicator_value','unit'] + semantic_cols + provenance
    enterprise_cols = ['enterprise_statistic_id','indicator_id','indicator_name',
        'location_id','location_name','industry_id','industry_name',
        'year','indicator_value','unit'] + semantic_cols + provenance

    schemas = {'economic': economic_cols, 'labour': labour_cols,
               'enterprise': enterprise_cols}
    totals = {}
    # Fail before any overwrite if a missing location remains.
    remaining_null_locations = d.filter(F.col('location_id').isNull()).count()
    if remaining_null_locations:
        raise ValueError(
            f'{remaining_null_locations} location_id values remain NULL; '
            'review source scope before writing Silver'
        )
    for group, columns in schemas.items():
        print(f'{group} Silver schema: {len(columns)} columns')
        part = d.filter(F.col('silver_group') == group).select(*columns)
        count = part.count()
        path = f'{output_root.rstrip("/")}/{group}_statistics'
        (part.write.format('delta').mode('overwrite')
         .option('overwriteSchema', 'true').save(path))
        print(f'Silver {group}_statistics: {count} records -> {path}')
        totals[group] = count
    if sum(totals.values()) != bronze.count():
        raise ValueError('Silver row count does not match Bronze')
    return totals


if __name__ == '__main__':
    print(run_silver())

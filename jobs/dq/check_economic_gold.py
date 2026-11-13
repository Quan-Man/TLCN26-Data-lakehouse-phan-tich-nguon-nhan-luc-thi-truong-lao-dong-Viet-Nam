# -*- coding: utf-8 -*-
"""Read-only Economy Gold DQ for the approved 6-dimension / 2-fact schema.

Run after build_economy_gold.py, before Hive/Trino publication.
"""
from pyspark.sql import functions as F
from jobs.gold.build_economy_gold import spark_session, GOLD_TABLES, SILVER

SCHEMAS = {
    'dim_date': {'date_key','full_date','day_number','day_name','week_number',
                 'month_number','month_name','quarter_number','year_number',
                 'year_month','year_quarter','is_weekend'},
    'dim_location': {'location_key','location_id','location_code','location_name',
                     'location_level','province_name','economic_region',
                     'valid_from','valid_to','is_current'},
    'dim_industry': {'industry_key','industry_id','industry_name'},
    'dim_economic_indicator': {'indicator_key','indicator_id','indicator_name',
                                'indicator_group','indicator_type','unit',
                                'frequency','aggregation_method'},
    'dim_occupation': {'occupation_key','occupation_name','occupation_id'},
    'dim_residence_type': {'residence_type_key','residence_type_id','residence_type_name'},
    'fact_economic_indicator': {'economic_fact_key','date_key','location_key',
        'industry_key','loaded_at','indicator_key','indicator_value','growth_rate',
        'previous_year_value','absolute_change','value_status','yoy_status','source_statistic_id'},
    'fact_labor_market': {'labor_fact_key','date_key','location_key','industry_key',
        'occupation_key','residence_type_key','indicator_key','indicator_value',
        'previous_year_value','growth_rate','absolute_change','value_status',
        'yoy_status','source_statistic_id','loaded_at'},
}
PKS = {'dim_date':'date_key','dim_location':'location_key','dim_industry':'industry_key',
       'dim_economic_indicator':'indicator_key','dim_occupation':'occupation_key',
       'dim_residence_type':'residence_type_key',
       'fact_economic_indicator':'economic_fact_key','fact_labor_market':'labor_fact_key'}
FKS = {'date_key':('dim_date','date_key'), 'location_key':('dim_location','location_key'),
       'industry_key':('dim_industry','industry_key'),
       'indicator_key':('dim_economic_indicator','indicator_key'),
       'occupation_key':('dim_occupation','occupation_key'),
       'residence_type_key':('dim_residence_type','residence_type_key')}
SILVER_IDS = {'economic_statistics':'economic_statistic_id',
              'labour_statistics':'labour_statistic_id',
              'enterprise_statistics':'enterprise_statistic_id'}


def check():
    spark = spark_session(register_hive=False)
    try:
        tables = {name: spark.read.format('delta').load(path)
                  for group in GOLD_TABLES.values() for name, path in group.items()}
        if set(tables) != set(SCHEMAS):
            raise ValueError('Gold table list does not match approved 8-table model')
        for name, df in tables.items():
            actual = set(df.columns)
            if actual != SCHEMAS[name]:
                raise ValueError(f'{name}: missing={SCHEMAS[name]-actual}; extra={actual-SCHEMAS[name]}')
            pk = PKS[name]
            if df.filter(F.col(pk).isNull()).limit(1).count():
                raise ValueError(f'{name}: NULL primary key')
            if df.groupBy(pk).count().filter('count > 1').limit(1).count():
                raise ValueError(f'{name}: duplicate primary key')
            print(f'DQ PASS {name}: {df.count()} rows', flush=True)

        # Aggregates are technical members, not ordinary occupations.
        occupation = tables['dim_occupation']
        if occupation.filter(F.lower(F.trim(F.col('occupation_name'))).isin(
                'tổng số', 'tong so')).limit(1).count():
            raise ValueError('dim_occupation still contains raw TỔNG SỐ')
        expected_special = {
            'dim_industry': {'ALL_INDUSTRIES', 'NOT_APPLICABLE', 'UNKNOWN'},
            'dim_occupation': {'ALL_OCCUPATIONS', 'NOT_APPLICABLE', 'UNKNOWN'},
            'dim_residence_type': {'ALL_RESIDENCE_TYPES', 'NOT_APPLICABLE', 'UNKNOWN'},
        }
        id_columns = {'dim_industry': 'industry_id',
                      'dim_occupation': 'occupation_id',
                      'dim_residence_type': 'residence_type_id'}
        for table, codes in expected_special.items():
            present = {r[0] for r in tables[table].select(id_columns[table])
                       .where(F.col(id_columns[table]).isin(*codes)).collect()}
            if present != codes:
                raise ValueError(f'{table}: missing technical members {codes - present}')

        silver_ids = None
        for table, key in SILVER_IDS.items():
            ids = spark.read.format('delta').load(f'{SILVER}/{table}').select(
                F.col(key).alias('source_statistic_id'))
            silver_ids = ids if silver_ids is None else silver_ids.unionByName(ids)
        facts = ['fact_economic_indicator','fact_labor_market']
        fact_ids = tables[facts[0]].select('source_statistic_id').unionByName(
            tables[facts[1]].select('source_statistic_id'))
        if silver_ids.count() != fact_ids.count():
            raise ValueError('Silver/Gold observation count mismatch')
        for label, ids in [('Silver', silver_ids), ('Gold', fact_ids)]:
            if ids.filter('source_statistic_id IS NULL').limit(1).count():
                raise ValueError(f'{label}: NULL lineage ID')
            if ids.groupBy('source_statistic_id').count().filter('count > 1').limit(1).count():
                raise ValueError(f'{label}: duplicate lineage ID')
        if silver_ids.join(fact_ids,'source_statistic_id','left_anti').limit(1).count():
            raise ValueError('Silver observations missing in Gold')
        if fact_ids.join(silver_ids,'source_statistic_id','left_anti').limit(1).count():
            raise ValueError('Gold observations not found in Silver')

        for name in facts:
            df = tables[name]
            for fk, (dim_name, pk) in FKS.items():
                if fk not in df.columns:
                    continue
                if fk in ('date_key','location_key','indicator_key') and df.filter(F.col(fk).isNull()).limit(1).count():
                    raise ValueError(f'{name}: NULL required FK {fk}')
                orphan = df.filter(F.col(fk).isNotNull()).join(
                    tables[dim_name].select(pk), df[fk] == tables[dim_name][pk], 'left_anti')
                if orphan.limit(1).count():
                    raise ValueError(f'{name}: orphan {fk}')
            bad = df.filter(
                (F.col('indicator_value').isNull() & (F.col('value_status') != 'MISSING_SOURCE')) |
                (F.col('indicator_value').isNotNull() & (F.col('value_status') != 'VALID')) |
                F.col('value_status').isNull() |
                ~F.col('yoy_status').isin('CALCULATED','NO_PREVIOUS_YEAR',
                    'MISSING_VALUE','NOT_APPLICABLE','ZERO_BASE') |
                (F.col('yoy_status') == 'CALCULATED') &
                    (F.col('growth_rate').isNull() | F.col('absolute_change').isNull()) |
                (F.col('yoy_status') != 'CALCULATED') & F.col('growth_rate').isNotNull() |
                (F.col('previous_year_value').isNull() &
                    (F.col('growth_rate').isNotNull() | F.col('absolute_change').isNotNull())))
            if bad.limit(1).count():
                bad.select('source_statistic_id','value_status','yoy_status').show(10,False)
                raise ValueError(f'{name}: invalid value / YoY status')
            df.groupBy('value_status','yoy_status').count().show(truncate=False)
        print(f'DQ PASS: {silver_ids.count()} Silver observations = {fact_ids.count()} Gold observations',flush=True)
    finally:
        spark.stop()


if __name__ == '__main__':
    check()

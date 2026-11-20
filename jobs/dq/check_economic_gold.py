from pyspark.sql import functions as F
from jobs.gold.build_economy_gold import spark_session, candidate, GOLD_TABLES, validate_outputs, FACT, FACT_COLUMNS

def check():
    spark=spark_session()
    try:
        expected=candidate(spark)
        actual={name:spark.read.format('delta').load(path) for group in GOLD_TABLES.values() for name,path in group.items()}
        validate_outputs(actual,expected[FACT].count())
        for name in GOLD_TABLES['economic']:
            a,b=actual[name],expected[name]
            if {f.name:f.dataType for f in a.schema}!={f.name:f.dataType for f in b.schema}: raise ValueError(f'{name}: schema mismatch')
            cols=[c for c in b.columns if c!='loaded_at']
            if a.select(*cols).exceptAll(b.select(*cols)).limit(1).count() or b.select(*cols).exceptAll(a.select(*cols)).limit(1).count(): raise ValueError(f'{name}: differs from Silver-derived candidate')
            print(f'DQ PASS: {name}={a.count()}',flush=True)
        # Independently verify the arithmetic and null policy on published facts.
        f=actual[FACT].join(actual['dim_economic_indicator'].select('indicator_key','indicator_type'),'indicator_key')
        v=F.col('indicator_value'); p=F.col('previous_year_value'); g=F.col('growth_rate')
        ready=v.isNotNull() & p.isNotNull() & (p!=0) & F.upper(F.col('indicator_type')).isin('COUNT','AMOUNT','AVERAGE','RATE','SHARE')
        expected_growth=(v.cast('double')-p.cast('double'))/F.abs(p.cast('double'))*100
        bad=f.filter((ready & (g.isNull() | (F.abs(g-expected_growth)>1e-7*F.greatest(F.lit(1.0),F.abs(expected_growth))))) | (~ready & g.isNotNull()) | ~F.col('absolute_change').eqNullSafe(v-p))
        if bad.limit(1).count(): raise ValueError('Invalid Gold growth_rate or absolute_change')
        f.groupBy('indicator_type').agg(F.count('*').alias('rows'),F.count('growth_rate').alias('growth_rows')).show(truncate=False)
        actual[FACT].groupBy('value_status').count().show(truncate=False)
    finally: spark.stop()

if __name__=='__main__': check()
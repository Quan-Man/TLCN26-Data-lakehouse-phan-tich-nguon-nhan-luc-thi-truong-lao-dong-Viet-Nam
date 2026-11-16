"""Read-only Gold Education Data Quality checks.
Run: cd /opt/airflow && python -m jobs.dq.check_education_gold
"""
import os
from pyspark.sql import functions as F
from jobs.silver.clean_admissions import spark_session

GOLD = os.getenv('EDUCATION_GOLD_PREFIX', 's3a://gold/education').rstrip('/')
SILVER = os.getenv('EDUCATION_SILVER_PREFIX', 's3a://silver/education').rstrip('/')
SHARED_LOCATION = os.getenv('GOLD_LOCATION_PATH', 's3a://gold/shared/dim_location')


def main():
    spark = spark_session()
    failures = []
    try:
        dims = {n: spark.read.format('delta').load(f'{GOLD}/{n}') for n in
                ('dim_university', 'dim_major', 'dim_program', 'dim_date')}
        dims['dim_location'] = spark.read.format('delta').load(SHARED_LOCATION)
        fact = spark.read.format('delta').load(f'{GOLD}/fact_admission')
        silver_fact = spark.read.format('delta').load(f'{SILVER}/admissions')
        groups = spark.read.format('delta').load(f'{SILVER}/quota_groups')

        def check(name, condition):
            passed = bool(condition)
            print(f"[{'PASS' if passed else 'FAIL'}] {name}")
            if not passed:
                failures.append(name)

        check('Fact count equals Silver admissions', fact.count() == silver_fact.count())
        for label, df, key in [('fact', fact, 'admission_fact_key'),
                                ('university', dims['dim_university'], 'university_key'),
                                ('major', dims['dim_major'], 'major_key'),
                                ('program', dims['dim_program'], 'program_key'),
                                ('date', dims['dim_date'], 'date_key'),
                                ('location', dims['dim_location'], 'location_key')]:
            check(f'{label} key non-null', df.filter(F.col(key).isNull()).limit(1).count() == 0)
            check(f'{label} key unique', df.groupBy(key).count().filter('count > 1').limit(1).count() == 0)
        for fk, dim in [('university_key', 'dim_university'),
                        ('major_key', 'dim_major'),
                        ('program_key', 'dim_program'),
                        ('location_key', 'dim_location'),
                        ('date_key', 'dim_date')]:
            missing = (fact.filter(F.col(fk).isNotNull())
                       .join(dims[dim].select(fk).distinct(), fk, 'left_anti'))
            check(f'FK {fk} -> {dim}', missing.limit(1).count() == 0)
        for fk in ('university_key', 'major_key', 'program_key', 'location_key'):
            check(f'Fact {fk} non-null', fact.filter(F.col(fk).isNull()).limit(1).count() == 0)
        check('Fact admission_id unique', fact.groupBy('admission_id').count().filter('count > 1').limit(1).count() == 0)
        check('Shared members have NULL quota', fact.filter(F.col('quota_group_id').isNotNull() & F.col('quota').isNotNull()).limit(1).count() == 0)
        check('No negative quota contribution', fact.filter(F.col('quota_total_contribution') < 0).limit(1).count() == 0)

        # For each shared group, total contribution must equal its group quota.
        actual = (fact.filter(F.col('quota_group_id').isNotNull())
                  .groupBy('quota_group_id')
                  .agg(F.sum('quota_total_contribution').alias('actual_quota')))
        mismatch = (groups.select('quota_group_id', 'group_quota')
                    .join(actual, 'quota_group_id', 'left')
                    .filter(F.col('actual_quota').isNull() | (F.col('group_quota') != F.col('actual_quota'))))
        check('Each shared quota counted exactly once', mismatch.limit(1).count() == 0)
        print(f'DQ GOLD EDUCATION: {"PASS" if not failures else "FAIL"} ({len(failures)} failures)')
        if failures:
            raise RuntimeError('Gold DQ failures: ' + '; '.join(failures))
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
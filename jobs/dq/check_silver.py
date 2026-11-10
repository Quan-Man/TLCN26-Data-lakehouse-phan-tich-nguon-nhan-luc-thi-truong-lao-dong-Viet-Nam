"""Read-only Data Quality checks for Education Silver Delta tables.
Run: python /opt/airflow/jobs/dq/check_silver.py
"""
import os
import sys
from pathlib import Path
from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'silver'))
from clean_admissions import spark_session

ROOT = os.getenv('EDUCATION_SILVER_PREFIX', 's3a://silver/education')
REQUIRED = {
    'universities': ['university_id', 'university_code', 'university_name', 'location_name'],
    'majors': ['major_id', 'major_code', 'major_name', 'major_code_status'],
    'programs': ['program_id', 'university_id', 'major_id', 'program_name'],
    'admissions': ['admission_id', 'program_id', 'admission_year', 'quota', 'quota_status',
                   'quota_group_id', 'source_major_code', 'source_raw_id',
                   'tuition_min', 'tuition_max', 'tuition_unit', 'tuition_status', 'tuition_is_free'],
    'quota_groups': ['quota_group_id', 'university_id', 'admission_year', 'group_quota', 'quota_status'],
    'admission_announcements': ['announcement_id', 'university_id', 'admission_year',
                                'announcement_date', 'date_status'],
}
PRIMARY_KEYS = {
    'universities': 'university_id', 'majors': 'major_id', 'programs': 'program_id',
    'admissions': 'admission_id', 'quota_groups': 'quota_group_id',
    'admission_announcements': 'announcement_id',
}
EXPECTED_SHARED_CODES = {'306','307','308','309','310','314','315','319','323','325','342','345'}


def main():
    spark = spark_session()
    spark.sparkContext.setLogLevel('ERROR')
    failures = []
    warnings = []

    def check(label, bad_count, severity='FAIL'):
        bad_count = int(bad_count)
        status = 'PASS' if bad_count == 0 else severity
        print(f'[{status}] {label}: {bad_count} issue(s)', flush=True)
        if bad_count:
            (failures if severity == 'FAIL' else warnings).append((label, bad_count))

    def exists(df, condition):
        return 1 if df.filter(condition).limit(1).count() else 0

    def orphan_count(child, child_key, parent, parent_key):
        return child.filter(F.col(child_key).isNotNull()).join(
            parent.select(F.col(parent_key).alias('_parent_key')).distinct(),
            F.col(child_key) == F.col('_parent_key'), 'left_anti'
        ).count()

    try:
        tables = {}
        for name, cols in REQUIRED.items():
            path = f'{ROOT}/{name}'
            try:
                df = spark.read.format('delta').load(path)
            except Exception as exc:
                print(f'[FAIL] Cannot read {name}: {exc}', flush=True)
                failures.append((f'Cannot read {name}', 1))
                continue
            absent = sorted(set(cols) - set(df.columns))
            if absent:
                print(f'[FAIL] {name} missing columns: {absent}', flush=True)
                failures.append((f'{name} missing columns', len(absent)))
                continue
            tables[name] = df.cache()
            print(f'\nTABLE {name}: {df.count()} rows', flush=True)
            pk = PRIMARY_KEYS[name]
            check(f'{name}.{pk} NULL', df.filter(F.col(pk).isNull()).count())
            check(f'{name}.{pk} duplicate', df.groupBy(pk).count().filter('count > 1').count())

        if len(tables) != len(REQUIRED):
            raise RuntimeError('Some tables are unavailable; see failures above')

        u, m, p, a, g, d = (tables[n] for n in REQUIRED)
        for label, child, fk, parent, pk in [
            ('programs.university_id', p, 'university_id', u, 'university_id'),
            ('programs.major_id', p, 'major_id', m, 'major_id'),
            ('admissions.program_id', a, 'program_id', p, 'program_id'),
            ('quota_groups.university_id', g, 'university_id', u, 'university_id'),
            ('admission_announcements.university_id', d, 'university_id', u, 'university_id'),
            ('admissions.quota_group_id (when present)', a, 'quota_group_id', g, 'quota_group_id'),
        ]:
            if fk != 'quota_group_id':
                check(f'{label} NULL', child.filter(F.col(fk).isNull()).count())
            check(f'{label} orphan', orphan_count(child, fk, parent, pk))

        check('admissions.admission_year outside 2019..2026',
              a.filter(F.col('admission_year').isNull() | ~F.col('admission_year').between(2019, 2026)).count())
        check('announcements.admission_year outside 2019..2026',
              d.filter(F.col('admission_year').isNull() | ~F.col('admission_year').between(2019, 2026)).count())
        check('admissions negative quota', a.filter(F.col('quota') < 0).count())
        check('quota_groups nonpositive group_quota', g.filter(F.col('group_quota').isNull() | (F.col('group_quota') <= 0)).count())
        check('admissions SHARED with non-NULL individual quota or missing group',
              a.filter((F.col('quota_status') == 'SHARED') &
                       (F.col('quota').isNotNull() | F.col('quota_group_id').isNull())).count())
        check('admissions non-SHARED with group id',
              a.filter((F.col('quota_status') != 'SHARED') & F.col('quota_group_id').isNotNull()).count())
        check('admissions KNOWN with NULL quota',
              a.filter((F.col('quota_status') == 'KNOWN') & F.col('quota').isNull()).count())
        check('admissions invalid quota_status',
              a.filter(F.col('quota_status').isNull() | ~F.col('quota_status').isin('SHARED','KNOWN','MISSING','REVIEW')).count())
        check('announcements invalid date_status/date combination',
              d.filter((F.col('date_status') == 'VALID') & F.col('announcement_date').isNull()).count() +
              d.filter((F.col('date_status') != 'VALID') & F.col('announcement_date').isNotNull()).count())
        check('announcements INVALID dates (review)', d.filter(F.col('date_status') == 'INVALID').count(), 'WARN')
        check('tuition minimum > maximum',
              a.filter(F.col('tuition_min').isNotNull() & F.col('tuition_max').isNotNull() &
                       (F.col('tuition_min') > F.col('tuition_max'))).count())
        check('tuition negative', a.filter((F.col('tuition_min') < 0) | (F.col('tuition_max') < 0)).count())
        check('tuition FREE with nonzero fee',
              a.filter((F.col('tuition_is_free') == True) &
                       ((F.col('tuition_min') != 0) | (F.col('tuition_max') != 0))).count())

        # Tuition status/unit consistency (REVIEW and MISSING may legitimately have NULL amounts).
        valid_units = ['CREDIT', 'SEMESTER', 'MONTH', 'YEAR', 'COURSE']
        valid_statuses = ['PARSED', 'ESTIMATED', 'REVIEW', 'MISSING', 'FREE']
        check('tuition invalid status', a.filter(
            F.col('tuition_status').isNull() |
            ~F.col('tuition_status').isin(*valid_statuses)).count())
        check('tuition invalid unit', a.filter(
            F.col('tuition_unit').isNotNull() &
            ~F.col('tuition_unit').isin(*valid_units)).count())
        check('tuition only one amount present', a.filter(
            F.col('tuition_min').isNull() != F.col('tuition_max').isNull()).count())
        check('tuition PARSED/ESTIMATED incomplete', a.filter(
            F.col('tuition_status').isin('PARSED', 'ESTIMATED') &
            (F.col('tuition_min').isNull() | F.col('tuition_max').isNull() |
             F.col('tuition_unit').isNull())).count())
        check('tuition REVIEW/MISSING with numeric amount', a.filter(
            F.col('tuition_status').isin('REVIEW', 'MISSING') &
            (F.col('tuition_min').isNotNull() | F.col('tuition_max').isNotNull())).count())
        check('tuition FREE inconsistent', a.filter(
            (F.col('tuition_status') == 'FREE') &
            (F.col('tuition_is_free').isNull() | (~F.col('tuition_is_free')) |
             F.col('tuition_min').isNull() | F.col('tuition_max').isNull() |
             (F.col('tuition_min') != 0) | (F.col('tuition_max') != 0))).count())
        check('tuition free flag inconsistent with status', a.filter(
            (F.col('tuition_is_free') == True) & (F.col('tuition_status') != 'FREE')).count())

        qsb = u.filter(F.col('university_code') == 'QSB').select('university_id')
        target = g.join(qsb, 'university_id').filter(F.col('admission_year') == 2026)
        check('QSB 2026 quota group count != 1', abs(target.count() - 1))
        check('QSB 2026 group quota != 150', target.filter(F.col('group_quota') != 150).count())
        shared = a.filter(F.col('quota_status') == 'SHARED')
        check('SHARED admission count != 12', abs(shared.count() - 12))
        check('SHARED admissions outside QSB 2026 group',
              shared.join(target.select('quota_group_id'), 'quota_group_id', 'left_anti').count())
        codes = {r['source_major_code'] for r in shared.select('source_major_code').collect()}
        check('SHARED major codes differ from expected 12',
              len(codes.symmetric_difference(EXPECTED_SHARED_CODES)))
        check('SHARED duplicate major codes', shared.count() - len(codes))

        # Informational only: absent admission_code and unresolved tuition are legitimate source limitations.
        print(f'\nINFO: admissions rows={a.count()}, announcements rows={d.count()}', flush=True)
        print(f'INFO: quota REVIEW rows={a.filter(F.col("quota_status") == "REVIEW").count()}', flush=True)
        print(f'INFO: tuition REVIEW rows={a.filter(F.col("tuition_status") == "REVIEW").count()}', flush=True)
    except Exception as exc:
        failures.append((f'DQ execution error: {exc}', 1))
        print(f'[FAIL] DQ execution error: {exc}', flush=True)
    finally:
        for df in locals().get('tables', {}).values():
            df.unpersist()
        spark.stop()

    print(f'\nDQ SUMMARY: {len(failures)} FAIL checks, {len(warnings)} WARN checks', flush=True)
    if failures:
        sys.exit(1)
    print('DQ SILVER EDUCATION: PASS', flush=True)


if __name__ == '__main__':
    main()

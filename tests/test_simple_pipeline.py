import json
from pathlib import Path
import sys
import tempfile
import unittest
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'jobs'))
import simple_pipeline as p


class PipelineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.a, cls.bad_a, _ = p.admissions(list((ROOT / 'data/education').glob('admission_2024.csv')))
        cls.j, cls.bad_j, _ = p.jobs(list((ROOT / 'data/jobs').glob('timviec365*.json')))
        cls.l, cls.bad_l, _ = p.labour(list((ROOT / 'data/economy').glob('6_luc_luong_lao_dong.xlsx')), 'unknown')

    def test_uploaded_data_and_gold_reconcile(self):
        self.assertEqual((len(self.a), len(self.j), len(self.l)), (893, 480, 88))
        self.assertEqual(len(self.bad_a) + len(self.bad_j) + len(self.bad_l), 0)
        gold = p.make_gold(self.a, self.j, self.l)
        p.validate({'admissions': self.a, 'jobs': self.j, 'labour': self.l}, gold)
        self.assertEqual(int(gold['admission_summary'].quota_missing_rows.sum()), 37)

    def test_all_missing_quota_is_not_zero(self):
        a = self.a.iloc[:1].copy()
        a['quota'] = pd.Series([pd.NA], dtype='Int64')
        g = p.make_gold(a, self.j, self.l)['admission_summary']
        self.assertTrue(pd.isna(g.quota_known_sum.iloc[0]))

    def test_year_gap_has_no_yoy(self):
        l = self.l[self.l.year.isin([2015, 2017])]
        g = p.make_gold(self.a, self.j, l)['labour_yearly']
        self.assertTrue(g.yoy_pct.isna().all())

    def test_job_duplicate_keeps_latest_and_bad_date_is_rejected(self):
        rows = [dict(job_id='x', job_title='old', source='test', location='Hà Nội', crawled_at='2026-01-01T00:00:00+07:00'),
                dict(job_id='x', job_title='new', source='test', location='Hà Nội', crawled_at='2026-02-01T00:00:00+07:00'),
                dict(job_id='bad', job_title='bad', source='test', location='Hà Nội', crawled_at='invalid')]
        with tempfile.TemporaryDirectory() as tmp:
            f = Path(tmp) / 'jobs.json'
            f.write_text(json.dumps(rows), encoding='utf-8')
            good, bad, count = p.jobs([f])
        self.assertEqual((count, len(good), len(bad)), (3, 1, 1))
        self.assertEqual(good.job_title.iloc[0], 'new')

    def test_admission_negative_quota_rejected_and_code_preserved(self):
        raw = pd.read_csv(ROOT / 'data/education/admission_2024.csv', dtype=str).head(1)
        raw['Chỉ tiêu'] = '-1'
        raw['Mã ngành'] = '00123TN'
        with tempfile.TemporaryDirectory() as tmp:
            f = Path(tmp) / 'admission.csv'
            raw.to_csv(f, index=False)
            good, bad, _ = p.admissions([f])
        self.assertEqual(len(good), 0)
        self.assertEqual(bad.major_code.iloc[0], '00123TN')

    def test_labour_does_not_sum_national_and_provinces(self):
        g = p.make_gold(self.a, self.j, self.l)['labour_yearly']
        self.assertEqual(len(g[g.geographic_level == 'national']), 11)
        self.assertEqual(len(g), 88)
        self.assertEqual(g[g.location.eq('Cả nước') & g.year.eq(2025)].value.iloc[0], 53500)


if __name__ == '__main__':
    unittest.main()

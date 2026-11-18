"""Cross-table Gold checks shared by file and Delta publication gates."""
from collections import Counter, defaultdict
from datetime import date


def evaluate(tables):
    issues = []
    def fail(code, count=1):
        if count:
            issues.append({'severity': 'error', 'code': code, 'count': count})

    for name, rows in tables.items():
        if not name.startswith('dim_') or not rows:
            continue
        fields = set().union(*(row.keys() for row in rows))
        if not fields.intersection({'valid_from', 'valid_to'}):
            continue
        identity = name[4:] + '_id'
        grouped = defaultdict(list)
        for row in rows:
            try:
                key = row[identity]
                start = date.fromisoformat(str(row['valid_from'])[:10])
                end = date.fromisoformat(str(row['valid_to'])[:10])
                if key is None or start >= end:
                    raise ValueError('invalid interval')
                grouped[key].append((start, end, row.get('is_current')))
            except (KeyError, TypeError, ValueError):
                fail('SCD_INVALID:' + name)
        for intervals in grouped.values():
            intervals.sort(key=lambda value: value[0])
            fail('SCD_OVERLAP:' + name, sum(a[1] > b[0] for a, b in zip(intervals, intervals[1:])))
            fail('SCD_CURRENT:' + name, int(sum(x[2] is True for x in intervals) > 1))

    required = ('bridge_job_location', 'fact_job_posting', 'dim_location', 'job_locations')
    for name in required:
        if name not in tables:
            fail('MISSING_BRIDGE_INPUT:' + name)
    if all(name in tables for name in required):
        bridge, parents, locations, source = (tables[name] for name in required)
        expected_fields = {'job_fact_key', 'job_id', 'location_key', 'location_id', 'is_primary'}
        fail('BRIDGE_MISSING_FIELDS', sum(not expected_fields <= row.keys() for row in bridge))
        if all(expected_fields <= row.keys() for row in bridge):
            counts = Counter((r['job_id'], r['location_id']) for r in bridge)
            fail('BRIDGE_DUPLICATE', sum(n - 1 for n in counts.values()))
            by_job = {r.get('job_fact_key'): r for r in parents}
            keys = {r.get('location_key') for r in locations}
            fail('BRIDGE_ORPHAN_JOB', sum(r['job_fact_key'] not in by_job for r in bridge))
            fail('BRIDGE_ORPHAN_LOCATION', sum(r['location_key'] is None or r['location_key'] not in keys for r in bridge))
            fail('BRIDGE_JOB_MISMATCH', sum(r['job_fact_key'] in by_job and
                 r['job_id'] != by_job[r['job_fact_key']].get('job_id') for r in bridge))
            seen = {r.get('job_id') for r in parents if r.get('seen_in_silver') is True}
            source_pairs = {(r.get('job_id'), r.get('location_id')) for r in source if r.get('job_id') in seen}
            per_job = Counter(job for job, _ in source_pairs)
            expected = {pair for pair in source_pairs if per_job[pair[0]] >= 2}
            actual = {(r['job_id'], r['location_id']) for r in bridge if r['job_id'] in seen}
            fail('BRIDGE_PAIR_MISMATCH', len(expected ^ actual))

    # The legacy aggregate groups active jobs by month, location and industry.
    if 'fact_job_posting' in tables and 'agg_labor_demand' in tables:
        facts, aggregates = tables['fact_job_posting'], tables['agg_labor_demand']
        if all('date_key' in r and 'is_active' in r for r in facts):
            expected = Counter()
            for row in facts:
                if row['is_active']:
                    key = row['date_key']
                    expected[(key // 100 if key else None, row.get('location_id'), row.get('industry_id'))] += 1
            actual = Counter()
            try:
                for row in aggregates:
                    actual[(row['year_month'], row.get('location_id'), row.get('industry_id'))] += int(row['posting_count'])
                fail('LABOR_DEMAND_RECONCILIATION', sum(expected[k] != actual[k] for k in expected.keys() | actual.keys()))
            except (ValueError, TypeError, KeyError):
                fail('LABOR_DEMAND_INVALID_MEASURE')
        else:
            fail('LABOR_DEMAND_UNSUPPORTED_SCHEMA')
    return issues

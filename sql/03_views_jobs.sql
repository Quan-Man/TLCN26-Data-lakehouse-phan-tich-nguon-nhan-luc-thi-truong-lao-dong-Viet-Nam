-- Trino 476. Prerequisite: publication views in delta.analytics, NOT Gold latest.
-- Every join/group includes _publication_id; the caller pins one publication
-- for an entire dashboard render. Rename catalog/schema consistently if needed.
-- Run 03 before 07. These files create views only; they do not publish data.
-- Gold DQ guarantees unique parent/FK/SCD keys. Do not hide invalid facts by
-- arbitrarily picking MAX/ROW_NUMBER. Unknown dimension labels keep the job.

-- Base grain: one row / (_publication_id, job_fact_key).
-- Includes inactive, activity-unknown and retained jobs. Active-only dashboards
-- filter the base BEFORE aggregation. Month is posted month, not active stock.
-- Salary sample = a positive FULL VND/month interval with consistent midpoint.
-- One-sided/negotiable/zero/inconsistent salaries remain jobs, but not samples.
CREATE OR REPLACE VIEW delta.analytics.vw_job_postings
SECURITY INVOKER AS
WITH enriched AS (
    SELECT p.*,
           CAST(date_trunc('month', p.posted_date) AS DATE) AS posted_month,
           coalesce(o.occupation_id, '__UNKNOWN__') AS canonical_occupation_id,
           coalesce(o.occupation_name, 'Không xác định') AS occupation_name,
           o.occupation_group,
           coalesce(i.industry_id, '__UNKNOWN__') AS canonical_industry_id,
           coalesce(i.industry_name, 'Không xác định') AS industry_name,
           coalesce(l.location_name, 'Không xác định') AS primary_location_name,
           coalesce(nullif(trim(l.province_name), ''), '__UNKNOWN__') AS province_name,
           coalesce(c.company_name, 'Không xác định') AS company_name,
           coalesce(
               p.has_salary IS TRUE
               AND coalesce(upper(trim(p.salary_status)), '') NOT IN (
                   'NEGOTIABLE', 'UNDISCLOSED', 'NOT_DISCLOSED',
                   'THOA_THUAN', 'THỎA THUẬN'
               )
               AND p.salary_min_vnd > 0 AND p.salary_max_vnd > 0
               AND p.salary_min_vnd <= p.salary_max_vnd
               AND p.salary_midpoint_vnd IS NOT NULL
               AND p.salary_midpoint_vnd = CAST(
                   (p.salary_min_vnd + p.salary_max_vnd) / DECIMAL '2.00'
                   AS DECIMAL(20, 2)
               ), FALSE
           ) AS salary_is_valid
    FROM delta.analytics.fact_job_posting p
    LEFT JOIN delta.analytics.dim_occupation o
      ON o._publication_id = p._publication_id AND o.occupation_key = p.occupation_key
    LEFT JOIN delta.analytics.dim_industry i
      ON i._publication_id = p._publication_id AND i.industry_key = p.industry_key
    LEFT JOIN delta.analytics.dim_location l
      ON l._publication_id = p._publication_id AND l.location_key = p.location_key
    LEFT JOIN delta.analytics.dim_company c
      ON c._publication_id = p._publication_id AND c.company_key = p.company_key
)
SELECT enriched.*,
       CASE WHEN salary_is_valid THEN salary_midpoint_vnd END AS salary_valid_midpoint_vnd,
       CASE WHEN salary_is_valid THEN BIGINT '1' ELSE BIGINT '0' END AS salary_sample_flag
FROM enriched;

-- Market totals: additive over disjoint posted months; missing date stays NULL.
-- Average/coverage are nonadditive: roll up salary_sum_vnd and salary_valid_count,
-- then divide. Never AVG(salary_avg_vnd), SUM(coverage), or AVG(DISTINCT salary).
CREATE OR REPLACE VIEW delta.analytics.vw_jobs_monthly
SECURITY INVOKER AS
WITH totals AS (
    SELECT _publication_id, posted_month,
           max(_publication_batch_id) AS _publication_batch_id,
           max(_publication_period) AS _publication_period,
           max(_publication_source_states) AS _publication_source_states,
           max(reference_date) AS reference_date,
           count(DISTINCT job_fact_key) AS posting_count,
           count_if(is_active IS TRUE) AS active_posting_count,
           count_if(is_active IS NULL) AS activity_unknown_count,
           count_if(has_salary IS TRUE) AS salary_disclosed_count,
           count(salary_valid_midpoint_vnd) AS salary_valid_count,
           coalesce(sum(salary_valid_midpoint_vnd), DECIMAL '0.00') AS salary_sum_vnd
    FROM delta.analytics.vw_job_postings
    GROUP BY _publication_id, posted_month
)
SELECT totals.*,
       CAST(CAST(salary_sum_vnd AS DECIMAL(38, 6)) / nullif(salary_valid_count, 0) AS DECIMAL(38, 6)) AS salary_avg_vnd,
       CAST(salary_valid_count AS DOUBLE) / nullif(posting_count, 0) AS salary_coverage,
       CAST(salary_disclosed_count AS DOUBLE) / nullif(posting_count, 0) AS salary_disclosure_coverage
FROM totals;

-- Occupation is distinct from economic industry. Key identifies its SCD version.
-- Average/coverage are nonadditive: roll up salary_sum_vnd and salary_valid_count,
-- then divide. Never AVG(salary_avg_vnd), SUM(coverage), or AVG(DISTINCT salary).
CREATE OR REPLACE VIEW delta.analytics.vw_job_demand_occupation_monthly
SECURITY INVOKER AS
WITH totals AS (
    SELECT _publication_id, posted_month, occupation_key, canonical_occupation_id, occupation_name,
           max(_publication_batch_id) AS _publication_batch_id,
           max(_publication_period) AS _publication_period,
           max(_publication_source_states) AS _publication_source_states,
           max(reference_date) AS reference_date,
           count(DISTINCT job_fact_key) AS posting_count,
           count_if(is_active IS TRUE) AS active_posting_count,
           count_if(is_active IS NULL) AS activity_unknown_count,
           count_if(has_salary IS TRUE) AS salary_disclosed_count,
           count(salary_valid_midpoint_vnd) AS salary_valid_count,
           coalesce(sum(salary_valid_midpoint_vnd), DECIMAL '0.00') AS salary_sum_vnd
    FROM delta.analytics.vw_job_postings
    GROUP BY _publication_id, posted_month, occupation_key, canonical_occupation_id, occupation_name
)
SELECT totals.*,
       CAST(CAST(salary_sum_vnd AS DECIMAL(38, 6)) / nullif(salary_valid_count, 0) AS DECIMAL(38, 6)) AS salary_avg_vnd,
       CAST(salary_valid_count AS DOUBLE) / nullif(posting_count, 0) AS salary_coverage,
       CAST(salary_disclosed_count AS DOUBLE) / nullif(posting_count, 0) AS salary_disclosure_coverage
FROM totals;

-- Industry demand, not job occupation. Group by business ID to roll up versions.
-- Average/coverage are nonadditive: roll up salary_sum_vnd and salary_valid_count,
-- then divide. Never AVG(salary_avg_vnd), SUM(coverage), or AVG(DISTINCT salary).
CREATE OR REPLACE VIEW delta.analytics.vw_job_demand_industry_monthly
SECURITY INVOKER AS
WITH totals AS (
    SELECT _publication_id, posted_month, industry_key, canonical_industry_id, industry_name,
           max(_publication_batch_id) AS _publication_batch_id,
           max(_publication_period) AS _publication_period,
           max(_publication_source_states) AS _publication_source_states,
           max(reference_date) AS reference_date,
           count(DISTINCT job_fact_key) AS posting_count,
           count_if(is_active IS TRUE) AS active_posting_count,
           count_if(is_active IS NULL) AS activity_unknown_count,
           count_if(has_salary IS TRUE) AS salary_disclosed_count,
           count(salary_valid_midpoint_vnd) AS salary_valid_count,
           coalesce(sum(salary_valid_midpoint_vnd), DECIMAL '0.00') AS salary_sum_vnd
    FROM delta.analytics.vw_job_postings
    GROUP BY _publication_id, posted_month, industry_key, canonical_industry_id, industry_name
)
SELECT totals.*,
       CAST(CAST(salary_sum_vnd AS DECIMAL(38, 6)) / nullif(salary_valid_count, 0) AS DECIMAL(38, 6)) AS salary_avg_vnd,
       CAST(salary_valid_count AS DOUBLE) / nullif(posting_count, 0) AS salary_coverage,
       CAST(salary_disclosed_count AS DOUBLE) / nullif(posting_count, 0) AS salary_disclosure_coverage
FROM totals;

-- PRIMARY province: each job belongs to one province/unknown group.
-- Average/coverage are nonadditive: roll up salary_sum_vnd and salary_valid_count,
-- then divide. Never AVG(salary_avg_vnd), SUM(coverage), or AVG(DISTINCT salary).
CREATE OR REPLACE VIEW delta.analytics.vw_jobs_primary_province_monthly
SECURITY INVOKER AS
WITH totals AS (
    SELECT _publication_id, posted_month, province_name,
           max(_publication_batch_id) AS _publication_batch_id,
           max(_publication_period) AS _publication_period,
           max(_publication_source_states) AS _publication_source_states,
           max(reference_date) AS reference_date,
           count(DISTINCT job_fact_key) AS posting_count,
           count_if(is_active IS TRUE) AS active_posting_count,
           count_if(is_active IS NULL) AS activity_unknown_count,
           count_if(has_salary IS TRUE) AS salary_disclosed_count,
           count(salary_valid_midpoint_vnd) AS salary_valid_count,
           coalesce(sum(salary_valid_midpoint_vnd), DECIMAL '0.00') AS salary_sum_vnd
    FROM delta.analytics.vw_job_postings
    GROUP BY _publication_id, posted_month, province_name
)
SELECT totals.*,
       CAST(CAST(salary_sum_vnd AS DECIMAL(38, 6)) / nullif(salary_valid_count, 0) AS DECIMAL(38, 6)) AS salary_avg_vnd,
       CAST(salary_valid_count AS DOUBLE) / nullif(posting_count, 0) AS salary_coverage,
       CAST(salary_disclosed_count AS DOUBLE) / nullif(posting_count, 0) AS salary_disclosure_coverage
FROM totals;

-- All work locations. Multi-location jobs come from the bridge INCLUDING their
-- primary place. Other jobs use the primary FK. UNION ALL + anti-join prevents
-- adding the primary place a second time. Unmapped raw IDs are kept separately.
CREATE OR REPLACE VIEW delta.analytics.vw_job_locations
SECURITY INVOKER AS
WITH links AS (
    SELECT DISTINCT _publication_id, job_fact_key,
           location_id AS analysis_location_id, location_key AS analysis_location_key
    FROM delta.analytics.bridge_job_location
    UNION ALL
    SELECT p._publication_id, p.job_fact_key,
           coalesce(p.primary_location_id, '__UNKNOWN__') AS analysis_location_id,
           p.location_key AS analysis_location_key
    FROM delta.analytics.vw_job_postings p
    WHERE NOT EXISTS (
        SELECT 1 FROM delta.analytics.bridge_job_location b
        WHERE b._publication_id = p._publication_id AND b.job_fact_key = p.job_fact_key
    )
)
SELECT p.*, x.analysis_location_id, x.analysis_location_key,
       coalesce(l.location_name, 'Không xác định') AS analysis_location_name,
       coalesce(nullif(trim(l.province_name), ''), '__UNKNOWN__') AS analysis_province_name
FROM links x
JOIN delta.analytics.vw_job_postings p
  ON p._publication_id = x._publication_id AND p.job_fact_key = x.job_fact_key
LEFT JOIN delta.analytics.dim_location l
  ON l._publication_id = x._publication_id AND l.location_key = x.analysis_location_key;

-- A job with two districts in the SAME province is one salary/count sample in
-- that province. Do this dedup BEFORE joining salaries, not SUM(DISTINCT salary).
CREATE OR REPLACE VIEW delta.analytics.vw_job_province_membership
SECURITY INVOKER AS
SELECT DISTINCT _publication_id, job_fact_key, analysis_province_name
FROM delta.analytics.vw_job_locations;

CREATE OR REPLACE VIEW delta.analytics.vw_jobs_all_provinces_monthly
SECURITY INVOKER AS
WITH job_provinces AS (
    SELECT p.*, m.analysis_province_name
    FROM delta.analytics.vw_job_province_membership m
    JOIN delta.analytics.vw_job_postings p
      ON p._publication_id = m._publication_id AND p.job_fact_key = m.job_fact_key
), totals AS (
    SELECT _publication_id, posted_month, analysis_province_name,
           max(_publication_batch_id) AS _publication_batch_id,
           max(_publication_period) AS _publication_period,
           max(_publication_source_states) AS _publication_source_states,
           max(reference_date) AS reference_date,
           count(DISTINCT job_fact_key) AS posting_count,
           count_if(has_salary IS TRUE) AS salary_disclosed_count,
           count(salary_valid_midpoint_vnd) AS salary_valid_count,
           coalesce(sum(salary_valid_midpoint_vnd), DECIMAL '0.00') AS salary_sum_vnd
    FROM job_provinces
    GROUP BY _publication_id, posted_month, analysis_province_name
)
SELECT totals.*,
       CAST(CAST(salary_sum_vnd AS DECIMAL(38, 6)) / nullif(salary_valid_count, 0) AS DECIMAL(38, 6)) AS salary_avg_vnd,
       CAST(salary_valid_count AS DOUBLE) / nullif(posting_count, 0) AS salary_coverage,
       CAST(salary_disclosed_count AS DOUBLE) / nullif(posting_count, 0) AS salary_disclosure_coverage
FROM totals;

-- CAUTION: all-province posting counts/salary sums overlap between provinces.
-- Use vw_jobs_monthly/base for unique-market totals. For a selection of several
-- provinces: SELECT DISTINCT _publication_id, job_fact_key FROM membership
-- WHERE ... first, THEN join the base and aggregate salary once per job.

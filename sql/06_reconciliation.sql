-- Trino 476, PUBLISHED tables/views only, grouped by _publication_id.
-- Run after publisher and sql/03_views_jobs.sql + sql/07_views_job_skills.sql.
-- These diagnostics are evidence, not substitutes for Gold/source DQ gates.
-- Never SUM values across distinct economic indicators or units.

CREATE OR REPLACE VIEW delta.analytics.vw_recon_publication_sources
SECURITY INVOKER AS
SELECT _publication_id, source, source_state, carry_reason, source_batch_id,
       source_period, source_as_of, period_aligned, reference_id, dq_status,
       _publication_batch_id, _publication_period, _publication_source_states
FROM delta.analytics.mart_publication_sources;

CREATE OR REPLACE VIEW delta.analytics.vw_recon_job_fact_counts
SECURITY INVOKER AS
WITH counts AS (
SELECT _publication_id, count(*) AS row_count,
       count(DISTINCT job_fact_key) AS distinct_job_key_count,
       count(DISTINCT job_id) AS distinct_job_id_count,
       count_if(job_fact_key IS NULL) AS null_job_key_count,
       count_if(job_id IS NULL) AS null_job_id_count,
       count_if(posted_date IS NULL) AS missing_posted_date_count,
       count_if(is_active IS NULL) AS unknown_active_count,
       count_if(seen_in_silver IS FALSE) AS retained_job_count
FROM delta.analytics.fact_job_posting
GROUP BY _publication_id
), pubs AS (
    SELECT DISTINCT _publication_id FROM delta.analytics.mart_publication_sources WHERE source='jobs'
)
SELECT p._publication_id, coalesce(c.row_count,0) AS row_count,
       coalesce(c.distinct_job_key_count,0) AS distinct_job_key_count,
       coalesce(c.distinct_job_id_count,0) AS distinct_job_id_count,
       coalesce(c.null_job_key_count,0) AS null_job_key_count,
       coalesce(c.null_job_id_count,0) AS null_job_id_count,
       coalesce(c.missing_posted_date_count,0) AS missing_posted_date_count,
       coalesce(c.unknown_active_count,0) AS unknown_active_count,
       coalesce(c.retained_job_count,0) AS retained_job_count
FROM pubs p LEFT JOIN counts c ON p._publication_id=c._publication_id;

CREATE OR REPLACE VIEW delta.analytics.vw_recon_job_skill_counts
SECURITY INVOKER AS
WITH pairs AS (
    SELECT _publication_id, job_fact_key, skill_id, count(*) AS n
    FROM delta.analytics.fact_job_skill
    GROUP BY _publication_id, job_fact_key, skill_id
), pair_counts AS (
    SELECT _publication_id, count(*) AS distinct_pair_count,
           coalesce(sum(CASE WHEN n > 1 THEN n - 1 ELSE 0 END), 0) AS duplicate_pair_extra_rows
    FROM pairs GROUP BY _publication_id
), raw AS (
    SELECT s._publication_id, count(*) AS row_count,
           count(DISTINCT s.job_fact_key) AS jobs_with_skill_evidence,
           count_if(s.is_required IS TRUE) AS required_pair_count,
           count_if(s.is_required IS FALSE) AS not_required_pair_count,
           count_if(s.is_required IS NULL) AS unknown_requirement_pair_count,
           count_if(s.job_fact_key IS NULL OR s.skill_id IS NULL OR s.skill_key IS NULL) AS null_identity_rows,
           count_if(s.skill_key = '0') AS unknown_skill_key_rows,
           count_if(j.job_fact_key IS NULL) AS orphan_job_rows,
           count_if(k.skill_key IS NULL) AS orphan_skill_rows,
           count_if(k.skill_key IS NOT NULL AND s.skill_id IS DISTINCT FROM k.skill_id) AS skill_id_key_mismatch_rows,
           count_if(j.job_fact_key IS NOT NULL AND (
             s.date_key IS DISTINCT FROM j.posted_date_key OR
             s.location_key IS DISTINCT FROM j.location_key OR
             s.occupation_key IS DISTINCT FROM j.occupation_key OR
             s.industry_key IS DISTINCT FROM j.industry_key)) AS parent_context_mismatch_rows
    FROM delta.analytics.fact_job_skill s
    LEFT JOIN (SELECT DISTINCT _publication_id, job_fact_key, posted_date_key, location_key, occupation_key, industry_key
               FROM delta.analytics.fact_job_posting) j
      ON j._publication_id=s._publication_id AND j.job_fact_key=s.job_fact_key
    LEFT JOIN (SELECT _publication_id, skill_key, min(skill_id) AS skill_id
               FROM delta.analytics.dim_skill GROUP BY _publication_id, skill_key) k
      ON k._publication_id=s._publication_id AND k.skill_key=s.skill_key
    GROUP BY s._publication_id
)
SELECT p._publication_id,
       coalesce(raw.row_count,0) AS row_count,
       coalesce(raw.jobs_with_skill_evidence,0) AS jobs_with_skill_evidence,
       coalesce(raw.required_pair_count,0) AS required_pair_count,
       coalesce(raw.not_required_pair_count,0) AS not_required_pair_count,
       coalesce(raw.unknown_requirement_pair_count,0) AS unknown_requirement_pair_count,
       coalesce(raw.null_identity_rows,0) AS null_identity_rows,
       coalesce(raw.unknown_skill_key_rows,0) AS unknown_skill_key_rows,
       coalesce(raw.orphan_job_rows,0) AS orphan_job_rows,
       coalesce(raw.orphan_skill_rows,0) AS orphan_skill_rows,
       coalesce(raw.skill_id_key_mismatch_rows,0) AS skill_id_key_mismatch_rows,
       coalesce(raw.parent_context_mismatch_rows,0) AS parent_context_mismatch_rows,
       coalesce(pair_counts.distinct_pair_count,0) AS distinct_pair_count,
       coalesce(pair_counts.duplicate_pair_extra_rows,0) AS duplicate_pair_extra_rows
FROM (SELECT DISTINCT _publication_id FROM delta.analytics.mart_publication_sources WHERE source='jobs') p
LEFT JOIN raw ON p._publication_id=raw._publication_id
LEFT JOIN pair_counts ON p._publication_id=pair_counts._publication_id;
-- If either referenced dimension/parent has duplicated keys, check that defect
-- separately. Do not consider a row-count-only report proof of uniqueness.

CREATE OR REPLACE VIEW delta.analytics.vw_recon_job_skill_count_mismatches
SECURITY INVOKER AS
SELECT p._publication_id, p.job_fact_key, p.skill_count,
       count(DISTINCT s.skill_id) FILTER (WHERE s.is_required IS TRUE) AS observed_required_skill_count
FROM delta.analytics.fact_job_posting p
LEFT JOIN delta.analytics.fact_job_skill s
  ON s._publication_id=p._publication_id AND s.job_fact_key=p.job_fact_key
GROUP BY p._publication_id, p.job_fact_key, p.skill_count
HAVING p.skill_count IS NULL OR p.skill_count <>
       count(DISTINCT s.skill_id) FILTER (WHERE s.is_required IS TRUE);
-- Empty result is expected; this check alone cannot prove exact Silver ID sets.

CREATE OR REPLACE VIEW delta.analytics.vw_recon_location_bridge_counts
SECURITY INVOKER AS
WITH pairs AS (
    SELECT _publication_id, job_fact_key, location_id, count(*) AS n
    FROM delta.analytics.bridge_job_location GROUP BY _publication_id, job_fact_key, location_id
), distinct_pairs AS (
    SELECT _publication_id, count(*) AS distinct_pair_count,
           coalesce(sum(CASE WHEN n > 1 THEN n - 1 ELSE 0 END), 0) AS duplicate_pair_extra_rows
    FROM pairs GROUP BY _publication_id
), rows_per_job AS (
    SELECT _publication_id, job_fact_key, count(DISTINCT location_id) AS locations
    FROM pairs GROUP BY _publication_id, job_fact_key
), counts AS (
    SELECT b._publication_id, count(*) AS row_count,
           count(DISTINCT b.job_fact_key) AS jobs_with_bridge,
           count_if(b.job_fact_key IS NULL OR b.location_id IS NULL OR b.location_key IS NULL) AS null_identity_rows,
           count_if(b.location_key = '0') AS unknown_location_rows,
           count_if(p.job_fact_key IS NULL) AS orphan_job_rows,
           count_if(l.location_key IS NULL) AS orphan_location_fk_rows
    FROM delta.analytics.bridge_job_location b
    LEFT JOIN (SELECT DISTINCT _publication_id, job_fact_key FROM delta.analytics.fact_job_posting) p
      ON p._publication_id=b._publication_id AND p.job_fact_key=b.job_fact_key
    LEFT JOIN (SELECT DISTINCT _publication_id, location_key FROM delta.analytics.dim_location) l
      ON l._publication_id=b._publication_id AND l.location_key=b.location_key
    GROUP BY b._publication_id
), cardinality AS (
    SELECT _publication_id, count_if(locations < 2) AS jobs_with_fewer_than_two_ids
    FROM rows_per_job GROUP BY _publication_id
)
SELECT p._publication_id, coalesce(c.row_count,0) AS row_count,
       coalesce(c.jobs_with_bridge,0) AS jobs_with_bridge,
       coalesce(c.null_identity_rows,0) AS null_identity_rows,
       coalesce(c.unknown_location_rows,0) AS unknown_location_rows,
       coalesce(c.orphan_job_rows,0) AS orphan_job_rows,
       coalesce(c.orphan_location_fk_rows,0) AS orphan_location_fk_rows,
       coalesce(d.distinct_pair_count,0) AS distinct_pair_count,
       coalesce(d.duplicate_pair_extra_rows,0) AS duplicate_pair_extra_rows,
       coalesce(x.jobs_with_fewer_than_two_ids,0) AS jobs_with_fewer_than_two_ids
FROM (SELECT DISTINCT _publication_id FROM delta.analytics.mart_publication_sources WHERE source='jobs') p
LEFT JOIN counts c ON p._publication_id=c._publication_id
LEFT JOIN distinct_pairs d ON p._publication_id=d._publication_id
LEFT JOIN cardinality x ON p._publication_id=x._publication_id;

-- Resolved excludes missing, unknown '0' and orphan FK. Coverage is per fact
-- role; NULL/unknown can also mean a dimension is not applicable, so raw
-- diagnostics must be interpreted using the source's own business rules.
CREATE OR REPLACE VIEW delta.analytics.vw_recon_job_mapping_coverage
SECURITY INVOKER AS
WITH mapping AS (
    SELECT f._publication_id, 'posted_date' AS dimension_role,
           count(*) AS total_jobs,
           count_if(f.posted_date_key IS NULL) AS null_fk_count,
           count_if(CAST(f.posted_date_key AS VARCHAR)='0') AS unknown_fk_count,
           count_if(f.posted_date_key IS NOT NULL AND CAST(f.posted_date_key AS VARCHAR)<>'0' AND d.key_value IS NULL) AS orphan_fk_count,
           count_if(f.posted_date_key IS NOT NULL AND CAST(f.posted_date_key AS VARCHAR)<>'0' AND d.key_value IS NOT NULL) AS resolved_count
    FROM delta.analytics.fact_job_posting f
    LEFT JOIN (SELECT DISTINCT _publication_id, CAST(date_key AS VARCHAR) AS key_value FROM delta.analytics.dim_date) d
      ON d._publication_id=f._publication_id AND d.key_value=CAST(f.posted_date_key AS VARCHAR)
    GROUP BY f._publication_id
    UNION ALL
    SELECT f._publication_id, 'expiration_date' AS dimension_role,
           count(*) AS total_jobs,
           count_if(f.expiration_date_key IS NULL) AS null_fk_count,
           count_if(CAST(f.expiration_date_key AS VARCHAR)='0') AS unknown_fk_count,
           count_if(f.expiration_date_key IS NOT NULL AND CAST(f.expiration_date_key AS VARCHAR)<>'0' AND d.key_value IS NULL) AS orphan_fk_count,
           count_if(f.expiration_date_key IS NOT NULL AND CAST(f.expiration_date_key AS VARCHAR)<>'0' AND d.key_value IS NOT NULL) AS resolved_count
    FROM delta.analytics.fact_job_posting f
    LEFT JOIN (SELECT DISTINCT _publication_id, CAST(date_key AS VARCHAR) AS key_value FROM delta.analytics.dim_date) d
      ON d._publication_id=f._publication_id AND d.key_value=CAST(f.expiration_date_key AS VARCHAR)
    GROUP BY f._publication_id
    UNION ALL
    SELECT f._publication_id, 'primary_location' AS dimension_role,
           count(*) AS total_jobs,
           count_if(f.location_key IS NULL) AS null_fk_count,
           count_if(CAST(f.location_key AS VARCHAR)='0') AS unknown_fk_count,
           count_if(f.location_key IS NOT NULL AND CAST(f.location_key AS VARCHAR)<>'0' AND d.key_value IS NULL) AS orphan_fk_count,
           count_if(f.location_key IS NOT NULL AND CAST(f.location_key AS VARCHAR)<>'0' AND d.key_value IS NOT NULL) AS resolved_count
    FROM delta.analytics.fact_job_posting f
    LEFT JOIN (SELECT DISTINCT _publication_id, CAST(location_key AS VARCHAR) AS key_value FROM delta.analytics.dim_location) d
      ON d._publication_id=f._publication_id AND d.key_value=CAST(f.location_key AS VARCHAR)
    GROUP BY f._publication_id
    UNION ALL
    SELECT f._publication_id, 'occupation' AS dimension_role,
           count(*) AS total_jobs,
           count_if(f.occupation_key IS NULL) AS null_fk_count,
           count_if(CAST(f.occupation_key AS VARCHAR)='0') AS unknown_fk_count,
           count_if(f.occupation_key IS NOT NULL AND CAST(f.occupation_key AS VARCHAR)<>'0' AND d.key_value IS NULL) AS orphan_fk_count,
           count_if(f.occupation_key IS NOT NULL AND CAST(f.occupation_key AS VARCHAR)<>'0' AND d.key_value IS NOT NULL) AS resolved_count
    FROM delta.analytics.fact_job_posting f
    LEFT JOIN (SELECT DISTINCT _publication_id, CAST(occupation_key AS VARCHAR) AS key_value FROM delta.analytics.dim_occupation) d
      ON d._publication_id=f._publication_id AND d.key_value=CAST(f.occupation_key AS VARCHAR)
    GROUP BY f._publication_id
    UNION ALL
    SELECT f._publication_id, 'industry' AS dimension_role,
           count(*) AS total_jobs,
           count_if(f.industry_key IS NULL) AS null_fk_count,
           count_if(CAST(f.industry_key AS VARCHAR)='0') AS unknown_fk_count,
           count_if(f.industry_key IS NOT NULL AND CAST(f.industry_key AS VARCHAR)<>'0' AND d.key_value IS NULL) AS orphan_fk_count,
           count_if(f.industry_key IS NOT NULL AND CAST(f.industry_key AS VARCHAR)<>'0' AND d.key_value IS NOT NULL) AS resolved_count
    FROM delta.analytics.fact_job_posting f
    LEFT JOIN (SELECT DISTINCT _publication_id, CAST(industry_key AS VARCHAR) AS key_value FROM delta.analytics.dim_industry) d
      ON d._publication_id=f._publication_id AND d.key_value=CAST(f.industry_key AS VARCHAR)
    GROUP BY f._publication_id
    UNION ALL
    SELECT f._publication_id, 'company' AS dimension_role,
           count(*) AS total_jobs,
           count_if(f.company_key IS NULL) AS null_fk_count,
           count_if(CAST(f.company_key AS VARCHAR)='0') AS unknown_fk_count,
           count_if(f.company_key IS NOT NULL AND CAST(f.company_key AS VARCHAR)<>'0' AND d.key_value IS NULL) AS orphan_fk_count,
           count_if(f.company_key IS NOT NULL AND CAST(f.company_key AS VARCHAR)<>'0' AND d.key_value IS NOT NULL) AS resolved_count
    FROM delta.analytics.fact_job_posting f
    LEFT JOIN (SELECT DISTINCT _publication_id, CAST(company_key AS VARCHAR) AS key_value FROM delta.analytics.dim_company) d
      ON d._publication_id=f._publication_id AND d.key_value=CAST(f.company_key AS VARCHAR)
    GROUP BY f._publication_id
)
SELECT mapping.*, CAST(resolved_count AS DOUBLE)/nullif(total_jobs,0) AS mapping_coverage
FROM mapping;

CREATE OR REPLACE VIEW delta.analytics.vw_recon_job_salary
SECURITY INVOKER AS
WITH totals AS (
    SELECT _publication_id, count(*) AS row_count, count(DISTINCT job_fact_key) AS distinct_job_count,
           count_if(has_salary IS TRUE) AS disclosed_salary_count,
           count(salary_valid_midpoint_vnd) AS salary_valid_count,
           count_if(has_salary IS TRUE AND salary_valid_midpoint_vnd IS NULL) AS disclosed_but_excluded_count,
           count_if(salary_is_valid IS FALSE AND salary_valid_midpoint_vnd IS NOT NULL) AS invalid_value_leaked_count,
           coalesce(sum(salary_valid_midpoint_vnd),DECIMAL '0.00') AS salary_sum_vnd
    FROM delta.analytics.vw_job_postings GROUP BY _publication_id
)
SELECT p._publication_id, coalesce(t.row_count,0) AS row_count,
       coalesce(t.distinct_job_count,0) AS distinct_job_count,
       coalesce(t.disclosed_salary_count,0) AS disclosed_salary_count,
       coalesce(t.salary_valid_count,0) AS salary_valid_count,
       coalesce(t.disclosed_but_excluded_count,0) AS disclosed_but_excluded_count,
       coalesce(t.invalid_value_leaked_count,0) AS invalid_value_leaked_count,
       coalesce(t.salary_sum_vnd, DECIMAL '0.00') AS salary_sum_vnd,
       CAST(CAST(t.salary_sum_vnd AS DECIMAL(38,6))/nullif(t.salary_valid_count,0) AS DECIMAL(38,6)) AS salary_avg_vnd,
       CAST(t.salary_valid_count AS DOUBLE)/nullif(t.distinct_job_count,0) AS salary_coverage
FROM (SELECT DISTINCT _publication_id FROM delta.analytics.mart_publication_sources WHERE source='jobs') p
LEFT JOIN totals t ON p._publication_id=t._publication_id;

-- Compare monthly dataset against the parent population. Expected: 0 mismatch
-- rows. NULL posted month is joined null-safely, not silently discarded.
CREATE OR REPLACE VIEW delta.analytics.vw_recon_job_monthly_mismatches
SECURITY INVOKER AS
WITH base AS (
    SELECT _publication_id, posted_month, count(DISTINCT job_fact_key) AS job_count,
           count(salary_valid_midpoint_vnd) AS salary_valid_count,
           coalesce(sum(salary_valid_midpoint_vnd),DECIMAL '0.00') AS salary_sum_vnd
    FROM delta.analytics.vw_job_postings GROUP BY _publication_id, posted_month
), comparison AS (
    SELECT coalesce(b._publication_id,a._publication_id) AS _publication_id,
           coalesce(b.posted_month,a.posted_month) AS posted_month,
           b.job_count AS fact_job_count, a.posting_count AS dataset_job_count,
           b.salary_valid_count AS fact_salary_valid_count, a.salary_valid_count AS dataset_salary_valid_count,
           b.salary_sum_vnd AS fact_salary_sum_vnd, a.salary_sum_vnd AS dataset_salary_sum_vnd
    FROM base b FULL OUTER JOIN delta.analytics.vw_jobs_monthly a
      ON b._publication_id=a._publication_id AND b.posted_month IS NOT DISTINCT FROM a.posted_month
)
SELECT * FROM comparison
WHERE fact_job_count IS DISTINCT FROM dataset_job_count
   OR fact_salary_valid_count IS DISTINCT FROM dataset_salary_valid_count
   OR fact_salary_sum_vnd IS DISTINCT FROM dataset_salary_sum_vnd;

-- Published economy model has exactly these two facts; do not invent a third.
-- NULL indicator values and unknown dimensions are counts for review, not
-- proof of DQ failure; their meanings depend on indicator/scope/source rules.
CREATE OR REPLACE VIEW delta.analytics.vw_recon_economy_fact_counts
SECURITY INVOKER AS
WITH facts AS (
SELECT _publication_id, 'fact_economic_indicator' AS fact_name,
       count(*) AS row_count, count(DISTINCT economic_fact_key) AS distinct_fact_key_count,
       count_if(economic_fact_key IS NULL) AS null_fact_key_count,
       count_if(indicator_key IS NULL OR CAST(indicator_key AS VARCHAR)='0') AS unknown_indicator_count,
       count_if(indicator_value IS NULL) AS null_indicator_value_count,
       count_if(location_key IS NULL OR CAST(location_key AS VARCHAR)='0') AS unknown_or_na_location_count,
       count_if(industry_key IS NULL OR CAST(industry_key AS VARCHAR)='0') AS unknown_or_na_industry_count,
       CAST(NULL AS BIGINT) AS unknown_or_na_occupation_count,
       CAST(NULL AS BIGINT) AS unknown_or_na_residence_count
FROM delta.analytics.fact_economic_indicator GROUP BY _publication_id
UNION ALL
SELECT _publication_id, 'fact_labor_market' AS fact_name,
       count(*) AS row_count, count(DISTINCT labor_fact_key) AS distinct_fact_key_count,
       count_if(labor_fact_key IS NULL) AS null_fact_key_count,
       count_if(indicator_key IS NULL OR CAST(indicator_key AS VARCHAR)='0') AS unknown_indicator_count,
       count_if(indicator_value IS NULL) AS null_indicator_value_count,
       count_if(location_key IS NULL OR CAST(location_key AS VARCHAR)='0') AS unknown_or_na_location_count,
       count_if(industry_key IS NULL OR CAST(industry_key AS VARCHAR)='0') AS unknown_or_na_industry_count,
       count_if(occupation_key IS NULL OR CAST(occupation_key AS VARCHAR)='0') AS unknown_or_na_occupation_count,
       count_if(residence_type_key IS NULL OR CAST(residence_type_key AS VARCHAR)='0') AS unknown_or_na_residence_count
FROM delta.analytics.fact_labor_market GROUP BY _publication_id
), scopes AS (
    SELECT p._publication_id, f.fact_name
    FROM (SELECT DISTINCT _publication_id FROM delta.analytics.mart_publication_sources WHERE source='economy') p
    CROSS JOIN (VALUES ('fact_economic_indicator'), ('fact_labor_market')) AS f(fact_name)
)
SELECT s._publication_id, s.fact_name,
       coalesce(f.row_count,0) AS row_count,
       coalesce(f.distinct_fact_key_count,0) AS distinct_fact_key_count,
       coalesce(f.null_fact_key_count,0) AS null_fact_key_count,
       coalesce(f.unknown_indicator_count,0) AS unknown_indicator_count,
       coalesce(f.null_indicator_value_count,0) AS null_indicator_value_count,
       coalesce(f.unknown_or_na_location_count,0) AS unknown_or_na_location_count,
       coalesce(f.unknown_or_na_industry_count,0) AS unknown_or_na_industry_count,
       CASE WHEN s.fact_name='fact_economic_indicator' THEN CAST(NULL AS BIGINT)
            ELSE coalesce(f.unknown_or_na_occupation_count,0) END AS unknown_or_na_occupation_count,
       CASE WHEN s.fact_name='fact_economic_indicator' THEN CAST(NULL AS BIGINT)
            ELSE coalesce(f.unknown_or_na_residence_count,0) END AS unknown_or_na_residence_count
FROM scopes s LEFT JOIN facts f
  ON s._publication_id=f._publication_id AND s.fact_name=f.fact_name;

-- Only row count is generic: a legacy Education builder uses admission_fact_key
-- while the proposed publication.json used admission_key. Use ONE explicitly
-- chosen adapter after inspecting the actual source, never guess or add both
-- shared-quota models. The legacy adapter is sql/adapters/06_education_quota_legacy.sql.
CREATE OR REPLACE VIEW delta.analytics.vw_recon_education_row_counts
SECURITY INVOKER AS
WITH counts AS (
    SELECT _publication_id, count(*) AS row_count
    FROM delta.analytics.fact_admission GROUP BY _publication_id
)
SELECT p._publication_id, coalesce(c.row_count,0) AS row_count,
       'ADMISSION_KEY_AND_QUOTA_ADAPTER_REQUIRED' AS reconciliation_status
FROM (SELECT DISTINCT _publication_id FROM delta.analytics.mart_publication_sources WHERE source='education') p
LEFT JOIN counts c ON p._publication_id=c._publication_id;

-- Duplicate dimension keys violate the joins in other views. A healthy release
-- returns no rows. Include every dimension referenced by jobs dashboards.
CREATE OR REPLACE VIEW delta.analytics.vw_recon_jobs_dimension_duplicate_keys
SECURITY INVOKER AS
SELECT _publication_id, 'dim_location' AS dimension_name, CAST(location_key AS VARCHAR) AS key_value, count(*) AS row_count
FROM delta.analytics.dim_location GROUP BY _publication_id, location_key HAVING count(*)>1
UNION ALL
SELECT _publication_id, 'dim_occupation', CAST(occupation_key AS VARCHAR), count(*)
FROM delta.analytics.dim_occupation GROUP BY _publication_id, occupation_key HAVING count(*)>1
UNION ALL
SELECT _publication_id, 'dim_industry', CAST(industry_key AS VARCHAR), count(*)
FROM delta.analytics.dim_industry GROUP BY _publication_id, industry_key HAVING count(*)>1
UNION ALL
SELECT _publication_id, 'dim_skill', CAST(skill_key AS VARCHAR), count(*)
FROM delta.analytics.dim_skill GROUP BY _publication_id, skill_key HAVING count(*)>1
UNION ALL
SELECT _publication_id, 'dim_company', CAST(company_key AS VARCHAR), count(*)
FROM delta.analytics.dim_company GROUP BY _publication_id, company_key HAVING count(*)>1
UNION ALL
SELECT _publication_id, 'dim_date', CAST(date_key AS VARCHAR), count(*)
FROM delta.analytics.dim_date GROUP BY _publication_id, date_key HAVING count(*)>1;

-- NEW MODEL ADAPTER CONTRACT (not executed until actual schemas are agreed):
-- fact_admission: admission_fact_key, independent_quota_value (no group quota).
-- fact_admission_quota_group: quota_group_key, group_quota_value (one row/group).
-- Aggregate each fact INDEPENDENTLY by publication and compatible scope, then
-- join the two aggregates by that scope. Check unique keys/null/negative values;
-- compare grouped quota to validated Silver quota_groups at the SAME grain.
-- Never join group fact through bridge members before summing group_quota_value.
-- Use real project column names; the contract names above are NOT asserted to
-- exist. A group quota cannot be allocated to programs/majors without a valid
-- documented allocation rule. Legacy and new models are mutually exclusive.

-- Trino 476. Prerequisite: publication views in delta.analytics, NOT Gold latest.
-- Every join/group includes _publication_id; the caller pins one publication
-- for an entire dashboard render. Rename catalog/schema consistently if needed.
-- Run 03 before 07. These files create views only; they do not publish data.
-- Gold DQ guarantees unique parent/FK/SCD keys. Do not hide invalid facts by
-- arbitrarily picking MAX/ROW_NUMBER. Unknown dimension labels keep the job.

-- is_required IS TRUE = explicit required evidence. FALSE means not required;
-- it is not necessarily proof of a "preferred" skill. NULL remains unspecified.
-- Detail grain: one job/skill BUSINESS ID per publication (Gold DQ contract).
CREATE OR REPLACE VIEW delta.analytics.vw_job_skill_detail
SECURITY INVOKER AS
SELECT j.*, s.skill_key, s.skill_id,
       s.is_required, s.required_level, s.years_required, s.confidence_score,
       s.skill_source_as_of, s.reference_date AS skill_reference_date,
       s.loaded_at AS skill_loaded_at,
       CASE WHEN s.is_required IS TRUE THEN 'REQUIRED'
            WHEN s.is_required IS FALSE THEN 'NOT_REQUIRED'
            ELSE 'UNSPECIFIED' END AS requirement_status,
       coalesce(k.canonical_name, 'Không xác định') AS skill_canonical_name,
       k.normalized_name AS skill_normalized_name, k.skill_type, k.skill_category,
       k.cluster_id, k.validation_status,
       k.valid_from AS skill_valid_from, k.valid_to AS skill_valid_to
FROM delta.analytics.fact_job_skill s
JOIN delta.analytics.vw_job_postings j
  ON j._publication_id = s._publication_id AND j.job_fact_key = s.job_fact_key
LEFT JOIN delta.analytics.dim_skill k
  ON k._publication_id = s._publication_id AND k.skill_key = s.skill_key;
-- Use the persisted SCD key, never filter k.is_current=true or join name/alias.
-- Location here is PRIMARY parent context. Never expand the detail by location
-- before calculating a per-skill salary or unique-market posting count.

CREATE OR REPLACE VIEW delta.analytics.vw_job_skills_required
SECURITY INVOKER AS
SELECT * FROM delta.analytics.vw_job_skill_detail WHERE is_required IS TRUE;

-- Denominator coverage includes jobs with NO extracted skills. A NULL required
-- flag is never counted as true. These are evidence counts, not proof that a job
-- requires no skills when nothing has been extracted.
CREATE OR REPLACE VIEW delta.analytics.vw_jobs_skill_evidence_monthly
SECURITY INVOKER AS
WITH per_job AS (
    SELECT _publication_id, job_fact_key, count(*) AS skill_pair_count,
           count_if(is_required IS TRUE) AS required_pair_count
    FROM delta.analytics.vw_job_skill_detail
    GROUP BY _publication_id, job_fact_key
), totals AS (
    SELECT j._publication_id, j.posted_month,
           count(DISTINCT j.job_fact_key) AS market_job_count,
           count_if(coalesce(s.skill_pair_count, 0) > 0) AS jobs_with_any_skill_evidence,
           count_if(coalesce(s.required_pair_count, 0) > 0) AS jobs_with_required_skill_evidence,
           coalesce(sum(s.skill_pair_count), BIGINT '0') AS job_skill_pair_count,
           coalesce(sum(s.required_pair_count), BIGINT '0') AS required_pair_count
    FROM delta.analytics.vw_job_postings j
    LEFT JOIN per_job s
      ON s._publication_id = j._publication_id AND s.job_fact_key = j.job_fact_key
    GROUP BY j._publication_id, j.posted_month
)
SELECT totals.*,
       CAST(jobs_with_any_skill_evidence AS DOUBLE) / nullif(market_job_count, 0) AS skill_evidence_coverage,
       CAST(jobs_with_required_skill_evidence AS DOUBLE) / nullif(market_job_count, 0) AS required_evidence_coverage
FROM totals;

-- Per-skill counts are unique jobs WITHIN that skill; SUM across skills is
-- job-skill pairs, not unique market jobs. Denominator: all parent jobs in the
-- SAME publication, including jobs with no skills.
CREATE OR REPLACE VIEW delta.analytics.vw_job_skill_summary
SECURITY INVOKER AS
WITH denominators AS (
    SELECT _publication_id, count(DISTINCT job_fact_key) AS market_job_count,
           count(salary_valid_midpoint_vnd) AS market_salary_valid_count,
           coalesce(sum(salary_valid_midpoint_vnd), DECIMAL '0.00') AS market_salary_sum_vnd
    FROM delta.analytics.vw_job_postings
    GROUP BY _publication_id
), skills AS (
    SELECT _publication_id, skill_id,
           array_join(array_sort(array_agg(DISTINCT skill_canonical_name)), ' | ') AS skill_names,
           count(DISTINCT job_fact_key) AS mentioned_job_count,
           count(DISTINCT job_fact_key) FILTER (WHERE is_required IS TRUE) AS required_job_count,
           count(DISTINCT job_fact_key) FILTER (WHERE is_required IS FALSE) AS not_required_job_count,
           count(DISTINCT job_fact_key) FILTER (WHERE is_required IS NULL) AS requirement_unknown_job_count,
           count(salary_valid_midpoint_vnd) AS mentioned_salary_valid_count,
           coalesce(sum(salary_valid_midpoint_vnd), DECIMAL '0.00') AS mentioned_salary_sum_vnd,
           count(salary_valid_midpoint_vnd) FILTER (WHERE is_required IS TRUE) AS required_salary_valid_count,
           coalesce(sum(salary_valid_midpoint_vnd) FILTER (WHERE is_required IS TRUE), DECIMAL '0.00') AS required_salary_sum_vnd
    FROM delta.analytics.vw_job_skill_detail
    GROUP BY _publication_id, skill_id
)
SELECT s.*, d.market_job_count, d.market_salary_valid_count, d.market_salary_sum_vnd,
       CAST(s.required_job_count AS DOUBLE) / nullif(d.market_job_count, 0) AS required_job_share,
       CAST(s.mentioned_job_count AS DOUBLE) / nullif(d.market_job_count, 0) AS mentioned_job_share,
       CAST(CAST(s.required_salary_sum_vnd AS DECIMAL(38, 6)) / nullif(s.required_salary_valid_count, 0) AS DECIMAL(38, 6)) AS required_salary_avg_vnd,
       CAST(s.required_salary_valid_count AS DOUBLE) / nullif(s.required_job_count, 0) AS required_salary_coverage,
       CAST(CAST(s.mentioned_salary_sum_vnd AS DECIMAL(38, 6)) / nullif(s.mentioned_salary_valid_count, 0) AS DECIMAL(38, 6)) AS mentioned_salary_avg_vnd,
       CAST(s.mentioned_salary_valid_count AS DOUBLE) / nullif(s.mentioned_job_count, 0) AS mentioned_salary_coverage
FROM skills s
JOIN denominators d ON s._publication_id = d._publication_id;

-- No hardcoded LIMIT: filter required_demand_rank <= N, retaining ties.
CREATE OR REPLACE VIEW delta.analytics.vw_top_required_skills
SECURITY INVOKER AS
SELECT s.*, dense_rank() OVER (
           PARTITION BY _publication_id ORDER BY required_job_count DESC
       ) AS required_demand_rank
FROM delta.analytics.vw_job_skill_summary s
WHERE required_job_count > 0;

-- Per-skill counts are unique jobs WITHIN that skill; SUM across skills is
-- job-skill pairs, not unique market jobs. Denominator: all parent jobs in the
-- SAME publication / posted_month, occupation_key, canonical_occupation_id, occupation_name, province_name, including jobs with no skills.
CREATE OR REPLACE VIEW delta.analytics.vw_job_skill_demand_monthly
SECURITY INVOKER AS
WITH denominators AS (
    SELECT _publication_id, posted_month, occupation_key, canonical_occupation_id, occupation_name, province_name, count(DISTINCT job_fact_key) AS market_job_count,
           count(salary_valid_midpoint_vnd) AS market_salary_valid_count,
           coalesce(sum(salary_valid_midpoint_vnd), DECIMAL '0.00') AS market_salary_sum_vnd
    FROM delta.analytics.vw_job_postings
    GROUP BY _publication_id, posted_month, occupation_key, canonical_occupation_id, occupation_name, province_name
), skills AS (
    SELECT _publication_id, posted_month, occupation_key, canonical_occupation_id, occupation_name, province_name, skill_id,
           array_join(array_sort(array_agg(DISTINCT skill_canonical_name)), ' | ') AS skill_names,
           count(DISTINCT job_fact_key) AS mentioned_job_count,
           count(DISTINCT job_fact_key) FILTER (WHERE is_required IS TRUE) AS required_job_count,
           count(DISTINCT job_fact_key) FILTER (WHERE is_required IS FALSE) AS not_required_job_count,
           count(DISTINCT job_fact_key) FILTER (WHERE is_required IS NULL) AS requirement_unknown_job_count,
           count(salary_valid_midpoint_vnd) AS mentioned_salary_valid_count,
           coalesce(sum(salary_valid_midpoint_vnd), DECIMAL '0.00') AS mentioned_salary_sum_vnd,
           count(salary_valid_midpoint_vnd) FILTER (WHERE is_required IS TRUE) AS required_salary_valid_count,
           coalesce(sum(salary_valid_midpoint_vnd) FILTER (WHERE is_required IS TRUE), DECIMAL '0.00') AS required_salary_sum_vnd
    FROM delta.analytics.vw_job_skill_detail
    GROUP BY _publication_id, posted_month, occupation_key, canonical_occupation_id, occupation_name, province_name, skill_id
)
SELECT s.*, d.market_job_count, d.market_salary_valid_count, d.market_salary_sum_vnd,
       CAST(s.required_job_count AS DOUBLE) / nullif(d.market_job_count, 0) AS required_job_share,
       CAST(s.mentioned_job_count AS DOUBLE) / nullif(d.market_job_count, 0) AS mentioned_job_share,
       CAST(CAST(s.required_salary_sum_vnd AS DECIMAL(38, 6)) / nullif(s.required_salary_valid_count, 0) AS DECIMAL(38, 6)) AS required_salary_avg_vnd,
       CAST(s.required_salary_valid_count AS DOUBLE) / nullif(s.required_job_count, 0) AS required_salary_coverage,
       CAST(CAST(s.mentioned_salary_sum_vnd AS DECIMAL(38, 6)) / nullif(s.mentioned_salary_valid_count, 0) AS DECIMAL(38, 6)) AS mentioned_salary_avg_vnd,
       CAST(s.mentioned_salary_valid_count AS DOUBLE) / nullif(s.mentioned_job_count, 0) AS mentioned_salary_coverage
FROM skills s
JOIN denominators d ON s._publication_id = d._publication_id AND s.posted_month IS NOT DISTINCT FROM d.posted_month AND s.occupation_key IS NOT DISTINCT FROM d.occupation_key AND s.canonical_occupation_id IS NOT DISTINCT FROM d.canonical_occupation_id AND s.occupation_name IS NOT DISTINCT FROM d.occupation_name AND s.province_name IS NOT DISTINCT FROM d.province_name;

-- Compare within SAME posted month / occupation version / primary province.
-- "Without required evidence" includes FALSE, NULL and no extracted mention;
-- it is NOT proof that the job does not use the skill. No causal inference.
CREATE OR REPLACE VIEW delta.analytics.vw_job_skill_salary_comparison
SECURITY INVOKER AS
WITH samples AS (
    SELECT d.*,
           market_job_count - required_job_count AS without_required_evidence_job_count,
           market_salary_valid_count - required_salary_valid_count AS without_required_evidence_salary_count,
           market_salary_sum_vnd - required_salary_sum_vnd AS without_required_evidence_salary_sum_vnd
    FROM delta.analytics.vw_job_skill_demand_monthly d
    WHERE required_job_count > 0
), compared AS (
    SELECT samples.*,
           CAST(CAST(without_required_evidence_salary_sum_vnd AS DECIMAL(38, 6)) /
                nullif(without_required_evidence_salary_count, 0) AS DECIMAL(38, 6)) AS without_required_evidence_salary_avg_vnd,
           CAST(without_required_evidence_salary_count AS DOUBLE) /
                nullif(without_required_evidence_job_count, 0) AS without_required_evidence_salary_coverage
    FROM samples
)
SELECT compared.*,
       required_salary_avg_vnd - without_required_evidence_salary_avg_vnd AS salary_difference_vnd
FROM compared;

-- SAFE multi-skill salary dataset: one row/job, array of DISTINCT required IDs.
-- Filtering arrays_overlap(required_skill_ids, ARRAY[...]) gives OR semantics
-- WITHOUT exploding jobs. For AND use array_intersect/cardinality; see guide.
CREATE OR REPLACE VIEW delta.analytics.vw_jobs_with_required_skills
SECURITY INVOKER AS
WITH per_job AS (
    SELECT _publication_id, job_fact_key,
           array_sort(array_agg(DISTINCT skill_id)) AS required_skill_ids
    FROM delta.analytics.vw_job_skills_required
    GROUP BY _publication_id, job_fact_key
)
SELECT j.*, coalesce(s.required_skill_ids, CAST(ARRAY[] AS ARRAY(VARCHAR))) AS required_skill_ids
FROM delta.analytics.vw_job_postings j
LEFT JOIN per_job s ON s._publication_id = j._publication_id AND s.job_fact_key = j.job_fact_key;

-- Optional all-province detail. Grain = job-skill-province, NOT job-skill.
-- Membership is deduped BEFORE the join: two districts in one province do not
-- multiply a salary sample. Cross-province totals still overlap intentionally.
CREATE OR REPLACE VIEW delta.analytics.vw_job_skill_all_provinces
SECURITY INVOKER AS
SELECT s.*, m.analysis_province_name
FROM delta.analytics.vw_job_skill_detail s
JOIN delta.analytics.vw_job_province_membership m
  ON m._publication_id = s._publication_id AND m.job_fact_key = s.job_fact_key;

-- Multi-skill OR query pattern (replace IDs with actual catalog IDs):
-- WITH requested AS (SELECT DISTINCT skill_id FROM (VALUES ('ID_A'), ('ID_B')) v(skill_id)),
-- selected_jobs AS (
--     SELECT DISTINCT s._publication_id, s.job_fact_key
--     FROM delta.analytics.vw_job_skills_required s
--     JOIN requested r ON r.skill_id = s.skill_id
--     WHERE s._publication_id = 'PUBLICATION_ID'
-- )
-- SELECT count(*) AS posting_count, count(j.salary_valid_midpoint_vnd) AS salary_valid_count,
--        CAST(sum(j.salary_valid_midpoint_vnd) AS DECIMAL(38,6)) / nullif(count(j.salary_valid_midpoint_vnd), 0) AS salary_avg_vnd,
--        CAST(count(j.salary_valid_midpoint_vnd) AS DOUBLE) / nullif(count(*), 0) AS salary_coverage
-- FROM selected_jobs s JOIN delta.analytics.vw_job_postings j
--   ON j._publication_id=s._publication_id AND j.job_fact_key=s.job_fact_key;
-- For a new month/occupation/province/active filter, apply the SAME parent filter
-- BEFORE both numerator and denominator. Filtering an aggregated skill view
-- alone does not recompute its market denominator for arbitrary dashboard scopes.

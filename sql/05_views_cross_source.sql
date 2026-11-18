-- Trino 476; run after publish_analytics and sql/03_views_jobs.sql.
-- P5 advanced is NOT established by this file. No cross-source comparison is
-- exposed just because three source receipts exist. This view shows readiness
-- and explicitly leaves the mapping/grain gate NOT_EVALUATED.
CREATE OR REPLACE VIEW delta.analytics.vw_cross_source_readiness
SECURITY INVOKER AS
WITH source_status AS (
    SELECT _publication_id,
           count(*) AS source_rows,
           count(DISTINCT source) AS distinct_sources,
           count(DISTINCT CASE WHEN source IN ('education','jobs','economy')
               AND dq_status = 'PASSED' THEN source END) AS passed_main_sources,
           count_if(source_state = 'CARRIED_FORWARD') AS carried_sources,
           count_if(period_aligned IS FALSE) AS differently_labeled_periods,
           count_if(source_state = 'CARRIED_FORWARD'
                    AND (carry_reason IS NULL OR trim(carry_reason) = '')) AS missing_carry_reasons,
           max(_publication_batch_id) AS _publication_batch_id,
           max(_publication_period) AS _publication_period,
           max(_publication_source_states) AS _publication_source_states
    FROM delta.analytics.mart_publication_sources
    GROUP BY _publication_id
)
SELECT source_status.*,
       CASE WHEN passed_main_sources = 3 AND source_rows = distinct_sources
                      AND missing_carry_reasons = 0
            THEN 'SOURCES_READY_P5_NOT_CERTIFIED'
            ELSE 'BLOCKED_SOURCE_STATUS' END AS readiness_status,
       'NOT_EVALUATED' AS p5_mapping_and_grain_status,
       FALSE AS comparison_enabled,
       'Cần nghiệm thu mapping, kỳ, địa giới, grain và coverage trước khi so sánh.' AS limitation
FROM source_status;

-- OPTIONAL P5 TEMPLATE: leave disabled until the following PUBLISHED inputs
-- have been built, independently reconciled and approved in the SAME release.
-- grain of e/j/l: publication_id, calendar_year, province_id, occupation_id.
-- e: mapped_quota_weighted, education_mapping_coverage, mapping_version.
-- j: unique_job_count, job_mapping_coverage, mapping_version.
-- l: indicator_value, labour_mapping_coverage, mapping_version, indicator_id,
--    unit; exactly one approved indicator/unit per gate/year.
-- gate: publication_id, calendar_year, mapping_version, status, indicator_id,
--       unit, approved_period_definition, and dq_evidence_id.
-- No GROUP quota is assigned to majors/occupations without approved weights.
-- Each per-source dataset was aggregated to the SAME grain before this join.
-- The gate must prove no duplicate keys, reviewed one-to-many allocation whose
-- weights sum to 1, coverage thresholds and compatible period/geography/source.
-- NULL = absent/unmapped observation, NEVER replaced with zero.
--
-- CREATE OR REPLACE VIEW delta.analytics.vw_cross_source_annual_comparison
-- SECURITY INVOKER AS
-- WITH keys AS (
--     SELECT _publication_id, calendar_year, province_id, occupation_id
--     FROM delta.analytics.p5_education_year_province_occupation
--     UNION
--     SELECT _publication_id, calendar_year, province_id, occupation_id
--     FROM delta.analytics.p5_jobs_year_province_occupation
--     UNION
--     SELECT _publication_id, calendar_year, province_id, occupation_id
--     FROM delta.analytics.p5_labour_year_province_occupation
-- )
-- SELECT k._publication_id, k.calendar_year, k.province_id, k.occupation_id,
--        g.mapping_version, g.indicator_id, g.unit, g.dq_evidence_id,
--        e.mapped_quota_weighted, e.education_mapping_coverage,
--        j.unique_job_count, j.job_mapping_coverage,
--        l.indicator_value, l.labour_mapping_coverage
-- FROM keys k
-- JOIN delta.analytics.p5_validated_gate g
--   ON g._publication_id=k._publication_id AND g.calendar_year=k.calendar_year
--  AND g.status='PASSED' AND g.approved_period_definition='CALENDAR_YEAR'
--  AND g.dq_evidence_id IS NOT NULL
-- LEFT JOIN delta.analytics.p5_education_year_province_occupation e
--   ON e._publication_id=k._publication_id AND e.calendar_year=k.calendar_year
--  AND e.province_id=k.province_id AND e.occupation_id=k.occupation_id
--  AND e.mapping_version=g.mapping_version
-- LEFT JOIN delta.analytics.p5_jobs_year_province_occupation j
--   ON j._publication_id=k._publication_id AND j.calendar_year=k.calendar_year
--  AND j.province_id=k.province_id AND j.occupation_id=k.occupation_id
--  AND j.mapping_version=g.mapping_version
-- LEFT JOIN delta.analytics.p5_labour_year_province_occupation l
--   ON l._publication_id=k._publication_id AND l.calendar_year=k.calendar_year
--  AND l.province_id=k.province_id AND l.occupation_id=k.occupation_id
--  AND l.mapping_version=g.mapping_version
--  AND l.indicator_id=g.indicator_id AND l.unit=g.unit;
--
-- This template is deliberately NOT executed. Do not interpret enrolment
-- quota as graduates or job postings as all vacancies/labour demand. Coverage,
-- source periods, release and allocation method must accompany comparisons.

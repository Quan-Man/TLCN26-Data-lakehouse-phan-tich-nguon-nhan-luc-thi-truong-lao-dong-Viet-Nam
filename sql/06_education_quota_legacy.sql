-- OPTIONAL: use ONLY after confirming the deployed fact_admission follows the
-- older build_education_gold.py using quota_total_contribution and group ID.
-- Do NOT run alongside/aggregate with a separate quota-group fact. Group quota
-- is allocated to one representative admission for totals; it is NOT the quota
-- of that program/major. Compare with validated Silver quota_groups separately.
-- A group contribution of zero is not proof that its original quota was zero.

CREATE OR REPLACE VIEW delta.analytics.vw_recon_education_legacy_group_detail
SECURITY INVOKER AS
SELECT _publication_id, quota_group_id,
       count(*) AS member_rows,
       count(DISTINCT admission_fact_key) AS distinct_admission_keys,
       count(DISTINCT university_key) AS universities,
       count(DISTINCT admission_year) AS admission_years,
       count_if(quota IS NOT NULL) AS group_member_with_individual_quota_rows,
       count_if(quota_total_contribution IS NULL) AS null_contribution_rows,
       count_if(quota_total_contribution < 0) AS negative_contribution_rows,
       count_if(quota_total_contribution > 0) AS positive_contribution_rows,
       sum(quota_total_contribution) AS recorded_group_quota_contribution
FROM delta.analytics.fact_admission
WHERE quota_group_id IS NOT NULL
GROUP BY _publication_id, quota_group_id;

CREATE OR REPLACE VIEW delta.analytics.vw_recon_education_legacy_quota
SECURITY INVOKER AS
WITH facts AS (
    SELECT _publication_id, count(*) AS row_count,
           count(DISTINCT admission_fact_key) AS distinct_admission_key_count,
           count_if(admission_fact_key IS NULL) AS null_admission_key_count,
           count_if(quota_group_id IS NULL AND quota IS NULL) AS missing_individual_quota_rows,
           count_if(quota_group_id IS NULL AND quota IS DISTINCT FROM quota_total_contribution) AS individual_contribution_mismatch_rows,
           count_if(quota_total_contribution IS NULL) AS null_contribution_rows,
           count_if(quota < 0 OR quota_total_contribution < 0) AS negative_quota_rows,
           sum(quota) FILTER (WHERE quota_group_id IS NULL) AS individual_quota_observed,
           sum(quota_total_contribution) FILTER (WHERE quota_group_id IS NOT NULL) AS group_quota_contribution_observed,
           sum(quota_total_contribution) AS recorded_total_quota
    FROM delta.analytics.fact_admission GROUP BY _publication_id
), groups AS (
    SELECT _publication_id, count(*) AS group_count,
           count_if(positive_contribution_rows > 1) AS groups_with_repeated_positive_contributions,
           count_if(universities > 1 OR admission_years > 1) AS groups_with_scope_conflicts,
           count_if(group_member_with_individual_quota_rows > 0) AS groups_with_member_quota_values,
           count_if(positive_contribution_rows = 0) AS groups_without_positive_contribution
    FROM delta.analytics.vw_recon_education_legacy_group_detail GROUP BY _publication_id
)
SELECT f.*, coalesce(g.group_count,0) AS group_count,
       coalesce(g.groups_with_repeated_positive_contributions,0) AS groups_with_repeated_positive_contributions,
       coalesce(g.groups_with_scope_conflicts,0) AS groups_with_scope_conflicts,
       coalesce(g.groups_with_member_quota_values,0) AS groups_with_member_quota_values,
       coalesce(g.groups_without_positive_contribution,0) AS groups_without_positive_contribution,
       'LEGACY_CONTRIBUTION_REQUIRES_SILVER_GROUP_COMPARISON' AS reconciliation_status
FROM facts f LEFT JOIN groups g ON f._publication_id=g._publication_id;

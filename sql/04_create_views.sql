-- Job demand by posting month, location, and industry.
CREATE OR REPLACE VIEW gold.v_job_demand_monthly AS
SELECT d.year_number,
       d.month_number,
       d.month_name,
       l.location_id,
       l.location_name,
       i.industry_id,
       i.industry_name,
       COUNT(*)::bigint AS posting_count,
       COUNT(DISTINCT f.company_key)::bigint AS hiring_company_count,
       COUNT(*) FILTER (WHERE f.is_salary_disclosed)::bigint AS salary_disclosed_count,
       AVG(f.salary_min) AS average_salary_min,
       AVG(f.salary_max) AS average_salary_max
FROM gold.fact_job_posting f
LEFT JOIN gold.dim_date d ON d.date_key = f.date_key
LEFT JOIN gold.dim_location l ON l.location_key = f.location_key
LEFT JOIN gold.dim_industry i ON i.industry_key = f.industry_key
WHERE f.is_active
GROUP BY d.year_number, d.month_number, d.month_name,
         l.location_id, l.location_name, i.industry_id, i.industry_name;

-- Frequency of each skill in active job postings.
CREATE OR REPLACE VIEW gold.v_skill_demand AS
SELECT s.skill_id,
       s.skill_name,
       s.skill_category,
       COUNT(DISTINCT j.job_posting_key)::bigint AS posting_count,
       COUNT(DISTINCT j.company_key)::bigint AS hiring_company_count,
       COUNT(*) FILTER (WHERE js.is_required IS TRUE)::bigint AS required_mention_count
FROM gold.fact_job_skill js
JOIN gold.fact_job_posting j ON j.job_posting_key = js.job_posting_key
JOIN gold.dim_skill s ON s.skill_key = js.skill_key
WHERE j.is_active AND s.is_active
GROUP BY s.skill_id, s.skill_name, s.skill_category;

-- Admission supply by year and study major.
CREATE OR REPLACE VIEW gold.v_admission_supply AS
SELECT a.admission_year,
       m.major_id,
       m.major_name,
       SUM(a.target_count)::bigint AS target_count,
       SUM(a.applicant_count)::bigint AS applicant_count,
       SUM(a.admitted_count)::bigint AS admitted_count,
       SUM(a.enrolled_count)::bigint AS enrolled_count,
       COUNT(DISTINCT a.university_key)::bigint AS university_count
FROM gold.fact_admission a
JOIN gold.dim_major m ON m.major_key = a.major_key
GROUP BY a.admission_year, m.major_id, m.major_name;

-- Labour-market measures by period and location.
CREATE OR REPLACE VIEW gold.v_labour_market AS
SELECT f.period_start,
       f.period_end,
       f.period_label,
       l.location_id,
       l.location_name,
       f.population_count,
       f.labor_force_count,
       f.employed_count,
       f.unemployed_count,
       f.underemployed_count,
       f.labor_force_rate,
       f.employment_rate,
       f.unemployment_rate,
       f.underemployment_rate,
       f.average_income,
       f.income_currency
FROM gold.fact_labour_market f
LEFT JOIN gold.dim_location l ON l.location_key = f.location_key;

-- Enterprise activity by period, location, and industry.
CREATE OR REPLACE VIEW gold.v_enterprise_activity AS
SELECT f.period_start,
       f.period_end,
       f.period_label,
       l.location_id,
       l.location_name,
       i.industry_id,
       i.industry_name,
       f.enterprise_count,
       f.newly_registered_count,
       f.suspended_count,
       f.dissolved_count,
       f.revenue,
       f.revenue_currency
FROM gold.fact_enterprise f
LEFT JOIN gold.dim_location l ON l.location_key = f.location_key
LEFT JOIN gold.dim_industry i ON i.industry_key = f.industry_key;

-- Economic indicators with readable dimension labels.
CREATE OR REPLACE VIEW gold.v_economic_indicators AS
SELECT f.period_start,
       f.period_end,
       f.period_label,
       l.location_id,
       l.location_name,
       e.indicator_id,
       e.indicator_name,
       e.indicator_category,
       f.value,
       COALESCE(f.unit, e.unit) AS unit,
       f.source_name
FROM gold.fact_economic_indicator f
JOIN gold.dim_economic_indicator e
  ON e.economic_indicator_key = f.economic_indicator_key
LEFT JOIN gold.dim_location l ON l.location_key = f.location_key;

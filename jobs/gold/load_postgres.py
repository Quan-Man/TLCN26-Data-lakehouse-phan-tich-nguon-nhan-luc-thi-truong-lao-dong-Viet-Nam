"""Load the generated Gold tables and aggregates into PostgreSQL."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from _common import ROOT, TABLE_DIR, read_table

DIM_COLUMNS = {
    "dim_date": ["date_key", "full_date", "day_of_month", "day_of_week", "day_name", "week_of_year", "month_number", "month_name", "quarter_number", "year_number", "is_weekend", "is_holiday"],
    "dim_location": ["location_id", "location_name", "location_type", "province_name", "region_name", "country_code", "latitude", "longitude", "valid_from", "valid_to", "is_current"],
    "dim_company": ["company_id", "company_name", "company_name_normalized", "website", "company_size", "industry_id", "is_active", "source_name"],
    "dim_occupation": ["occupation_id", "occupation_name", "occupation_group", "occupation_family", "is_active"],
    "dim_industry": ["industry_id", "industry_name", "parent_industry_id", "industry_level", "is_active"],
    "dim_major": ["major_id", "major_name", "major_group", "is_active"],
    "dim_skill": ["skill_id", "skill_name", "skill_category", "skill_type", "is_active"],
    "dim_university": ["university_id", "university_name", "university_name_normalized", "location_id", "institution_type", "is_active"],
    "dim_economic_indicator": ["indicator_id", "indicator_name", "indicator_category", "unit", "frequency", "source_name", "is_active"],
}
AGG_DDL = """
CREATE TABLE IF NOT EXISTS gold.agg_labor_demand (
    year_month integer, location_id text, industry_id text, posting_count bigint NOT NULL,
    hiring_company_count bigint NOT NULL, average_salary_min numeric(18,2)
);
CREATE TABLE IF NOT EXISTS gold.agg_skill_demand (
    skill_id text PRIMARY KEY, skill_name text NOT NULL, skill_category text,
    posting_count bigint NOT NULL, hiring_company_count bigint NOT NULL,
    required_mention_count bigint NOT NULL
);
CREATE TABLE IF NOT EXISTS gold.agg_labor_supply (
    year_month integer, period_start date, period_end date, period_label text, location_id text,
    location_name text, labor_force_count bigint, employed_count bigint,
    unemployed_count bigint, unemployment_rate numeric(9,4)
);
CREATE TABLE IF NOT EXISTS gold.agg_supply_demand_gap (
    year_month integer, location_id text, industry_id text, posting_count bigint NOT NULL,
    unemployed_count bigint, posting_minus_unemployed bigint
);
"""
AGG_COLUMNS = {
    "agg_labor_demand": ["year_month", "location_id", "industry_id", "posting_count", "hiring_company_count", "average_salary_min"],
    "agg_skill_demand": ["skill_id", "skill_name", "skill_category", "posting_count", "hiring_company_count", "required_mention_count"],
    "agg_labor_supply": ["year_month", "period_start", "period_end", "period_label", "location_id", "location_name", "labor_force_count", "employed_count", "unemployed_count", "unemployment_rate"],
    "agg_supply_demand_gap": ["year_month", "location_id", "industry_id", "posting_count", "unemployed_count", "posting_minus_unemployed"],
}


def insert_rows(cursor, table: str, rows: list[dict], columns: list[str], returning: str | None = None):
    if not rows:
        return []
    names = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    suffix = f" RETURNING {returning}" if returning else ""
    query = f"INSERT INTO gold.{table} ({names}) VALUES ({placeholders}){suffix}"
    returned = []
    for row in rows:
        cursor.execute(query, [row.get(column) for column in columns])
        if returning:
            returned.append(cursor.fetchone()[0])
    return returned


def load(input_dir: Path) -> None:
    try:
        import psycopg2
    except ImportError as exc:
        raise RuntimeError("psycopg2 is required in the runtime image to load PostgreSQL") from exc

    connection = psycopg2.connect(
        host=os.getenv("PGHOST", "postgres"), port=int(os.getenv("PGPORT", "5432")),
        dbname=os.getenv("PGDATABASE", "lakehouse"), user=os.getenv("PGUSER", "lakehouse"),
        password=os.environ["PGPASSWORD"],
    )
    try:
        with connection:
            with connection.cursor() as cursor:
                for filename in ("01_create_schemas.sql", "02_create_dimensions.sql",
                                 "03_create_facts.sql", "04_create_views.sql"):
                    sql = (ROOT / "sql" / filename).read_text(encoding="utf-8-sig")
                    cursor.execute(sql)
                cursor.execute(AGG_DDL)
                cursor.execute("TRUNCATE TABLE " + ", ".join(
                    "gold." + table for table in [
                        "agg_supply_demand_gap", "agg_labor_supply", "agg_skill_demand",
                        "agg_labor_demand", "fact_job_skill", "fact_job_posting",
                        "fact_admission", "fact_economic_indicator", "fact_labour_market",
                        "fact_enterprise", "dim_date", "dim_location", "dim_company",
                        "dim_occupation", "dim_industry", "dim_major", "dim_skill",
                        "dim_university", "dim_economic_indicator",
                    ]
                ) + " RESTART IDENTITY CASCADE")

                for table, columns in DIM_COLUMNS.items():
                    insert_rows(cursor, table, read_table(table, input_dir), columns)

                cursor.execute("SELECT company_id, company_key FROM gold.dim_company")
                company_keys = dict(cursor.fetchall())
                cursor.execute("SELECT location_id, location_key FROM gold.dim_location")
                location_keys = dict(cursor.fetchall())
                cursor.execute("SELECT skill_id, skill_key FROM gold.dim_skill")
                skill_keys = dict(cursor.fetchall())

                posting_rows = []
                for row in read_table("fact_job_posting", input_dir):
                    posting_rows.append({**row,
                        "company_key": company_keys.get(row.get("company_id")),
                        "location_key": location_keys.get(row.get("location_id")),
                        "occupation_key": None, "industry_key": None})
                insert_rows(cursor, "fact_job_posting", posting_rows,
                    ["job_id", "date_key", "company_key", "occupation_key", "industry_key", "location_key", "source_name", "job_title", "job_type", "experience_level", "education_level", "salary_min", "salary_max", "salary_currency", "salary_period", "is_salary_disclosed", "posting_count", "is_active", "crawled_at", "valid_through"])
                cursor.execute("SELECT job_id, job_posting_key FROM gold.fact_job_posting")
                posting_keys = dict(cursor.fetchall())

                skill_rows = []
                for row in read_table("fact_job_skill", input_dir):
                    skill_rows.append({**row, "job_posting_key": posting_keys[row["job_id"]],
                                       "skill_key": skill_keys[row["skill_id"]]})
                insert_rows(cursor, "fact_job_skill", skill_rows,
                    ["job_posting_key", "skill_key", "evidence_field", "matched_text", "raw_skill_text", "is_required", "required_level", "years_required", "extraction_method", "confidence_score", "validation_status", "extractor_version"])

                fact_columns = {
                    "fact_admission": ["admission_year", "university_key", "major_key", "location_key", "program_code", "degree_level", "admission_method", "target_count", "applicant_count", "admitted_count", "enrolled_count", "tuition_amount", "tuition_currency", "source_name"],
                    "fact_economic_indicator": ["economic_indicator_key", "location_key", "date_key", "period_start", "period_end", "period_label", "value", "unit", "source_name"],
                    "fact_labour_market": ["location_key", "date_key", "period_start", "period_end", "period_label", "population_count", "labor_force_count", "employed_count", "unemployed_count", "underemployed_count", "labor_force_rate", "employment_rate", "unemployment_rate", "underemployment_rate", "average_income", "income_currency", "source_name"],
                    "fact_enterprise": ["location_key", "industry_key", "date_key", "period_start", "period_end", "period_label", "enterprise_count", "newly_registered_count", "suspended_count", "dissolved_count", "revenue", "revenue_currency", "source_name"],
                }
                for table, columns in fact_columns.items():
                    insert_rows(cursor, table, read_table(table, input_dir), columns)
                for table, columns in AGG_COLUMNS.items():
                    insert_rows(cursor, table, read_table(table, input_dir), columns)
    finally:
        connection.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, default=TABLE_DIR)
    args = parser.parse_args()
    load(args.input_dir)
    print("Gold dimensions, facts, aggregates, and views loaded into PostgreSQL.")


if __name__ == "__main__":
    main()







-- PostgreSQL analytical schemas. Safe to run repeatedly.
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS demo;

COMMENT ON SCHEMA gold IS 'Curated dimensions, facts, and analytical views';

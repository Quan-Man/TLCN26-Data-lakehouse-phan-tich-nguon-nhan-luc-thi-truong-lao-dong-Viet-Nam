#!/usr/bin/env bash
set -euo pipefail
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres \
  --set=airflow_password="$AIRFLOW_DB_PASSWORD" \
  --set=superset_password="$SUPERSET_DB_PASSWORD" \
  --set=lakehouse_password="$LAKEHOUSE_DB_PASSWORD" \
  --set=bi_password="$BI_DB_PASSWORD" <<'SQL'
CREATE ROLE airflow LOGIN PASSWORD :'airflow_password';
CREATE ROLE superset LOGIN PASSWORD :'superset_password';
CREATE ROLE lakehouse LOGIN PASSWORD :'lakehouse_password';
CREATE ROLE bi_reader LOGIN PASSWORD :'bi_password';
CREATE DATABASE airflow OWNER airflow;
CREATE DATABASE superset OWNER superset;
CREATE DATABASE lakehouse OWNER lakehouse;
REVOKE CONNECT ON DATABASE airflow FROM PUBLIC;
REVOKE CONNECT ON DATABASE superset FROM PUBLIC;
REVOKE CONNECT ON DATABASE lakehouse FROM PUBLIC;
GRANT CONNECT ON DATABASE lakehouse TO bi_reader;
\connect lakehouse
CREATE SCHEMA demo AUTHORIZATION lakehouse;
CREATE SCHEMA gold AUTHORIZATION lakehouse;
GRANT USAGE ON SCHEMA demo, gold TO bi_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE lakehouse IN SCHEMA demo
  GRANT SELECT ON TABLES TO bi_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE lakehouse IN SCHEMA gold
  GRANT SELECT ON TABLES TO bi_reader;
SET ROLE lakehouse;
CREATE TABLE demo.job_counts (
  location text PRIMARY KEY,
  posting_count bigint NOT NULL CHECK (posting_count >= 0)
);
RESET ROLE;
SQL

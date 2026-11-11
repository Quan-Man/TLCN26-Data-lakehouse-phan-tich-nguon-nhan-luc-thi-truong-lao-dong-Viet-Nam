#!/usr/bin/env bash
set -Eeuo pipefail
set +x

log() { printf '[lakehouse-postgres] %s\n' "$*" >&2; }
die() { log "ERROR: $*"; exit 1; }

vendor_entrypoint="${LAKEHOUSE_POSTGRES_ENTRYPOINT:-/usr/local/bin/docker-entrypoint.sh}"
[[ -x "$vendor_entrypoint" ]] || die "Official PostgreSQL entrypoint not found."

if (( $# == 0 )); then set -- postgres; fi
if [[ "$1" == -* ]]; then set -- postgres "$@"; fi
# Preserve the official image's handling of administrative commands and help.
if [[ "$1" != postgres ]]; then exec "$vendor_entrypoint" "$@"; fi
for arg in "$@"; do
    case "$arg" in
        --help|--version|-V|--describe-config) exec "$vendor_entrypoint" "$@" ;;
    esac
done

export POSTGRES_USER="${POSTGRES_USER:-postgres}"
export POSTGRES_DB="${POSTGRES_DB:-postgres}"
export PGPORT="${PGPORT:-5432}"
ready_file="${POSTGRES_READY_FILE:-/tmp/lakehouse-postgres-ready}"
startup_timeout="${POSTGRES_STARTUP_TIMEOUT:-180}"
[[ "$startup_timeout" =~ ^[1-9][0-9]*$ ]] || die "POSTGRES_STARTUP_TIMEOUT must be positive seconds."
[[ "$PGPORT" =~ ^[0-9]+$ ]] || die "PGPORT must be numeric."

postgres_pid=''
cleanup() {
    local status=$?
    trap - EXIT TERM INT
    rm -f -- "$ready_file"
    if [[ -n "$postgres_pid" ]] && kill -0 "$postgres_pid" 2>/dev/null; then
        # SIGINT requests PostgreSQL fast shutdown, closing client sessions.
        log "Stopping PostgreSQL."
        kill -INT "$postgres_pid" 2>/dev/null || true
        wait "$postgres_pid" 2>/dev/null || true
    fi
    exit "$status"
}
trap cleanup EXIT
trap 'exit 0' TERM INT
rm -f -- "$ready_file"

for variable in POSTGRES_PASSWORD AIRFLOW_DB_PASSWORD SUPERSET_DB_PASSWORD LAKEHOUSE_DB_PASSWORD BI_DB_PASSWORD; do
    [[ -n "${!variable:-}" ]] || die "Required environment variable is missing: $variable"
    export "$variable"
done

"$vendor_entrypoint" "$@" &
postgres_pid=$!

export PGCONNECT_TIMEOUT=3
export PGPASSWORD="$POSTGRES_PASSWORD"
psql_command=(psql -X --no-password --host=127.0.0.1 --port="$PGPORT"
    --username="$POSTGRES_USER" --dbname=postgres --set=ON_ERROR_STOP=1)

log "Waiting for PostgreSQL (timeout: ${startup_timeout}s)."
deadline=$(( SECONDS + startup_timeout ))
until "${psql_command[@]}" --quiet --tuples-only --command='SELECT 1' >/dev/null 2>&1; do
    if ! kill -0 "$postgres_pid" 2>/dev/null; then
        die "PostgreSQL exited before bootstrap; check preceding server logs."
    fi
    if (( SECONDS >= deadline )); then
        die "Cannot authenticate to PostgreSQL. Check server logs, POSTGRES_USER, POSTGRES_PASSWORD and PGPORT. On an existing volume, POSTGRES_PASSWORD must match the stored administrator password."
    fi
    sleep 1
done

log "Ensuring application roles, databases, schemas and privileges."

PGOPTIONS='-c statement_timeout=60000 -c lock_timeout=10000' \
"${psql_command[@]}" <<'SQL'
\getenv airflow_password AIRFLOW_DB_PASSWORD
\getenv superset_password SUPERSET_DB_PASSWORD
\getenv lakehouse_password LAKEHOUSE_DB_PASSWORD
\getenv bi_password BI_DB_PASSWORD
\getenv admin_user POSTGRES_USER

SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', v.role_name, v.role_password)
FROM (VALUES
    ('airflow', :'airflow_password'),
    ('superset', :'superset_password'),
    ('lakehouse', :'lakehouse_password'),
    ('bi_reader', :'bi_password')
) AS v(role_name, role_password)
WHERE NOT EXISTS (
    SELECT 1 FROM pg_roles r WHERE r.rolname = v.role_name
)
\gexec

-- Synchronize application passwords from environment variables.
SELECT format('ALTER ROLE %I WITH LOGIN PASSWORD %L',
    v.role_name, v.role_password)
FROM (VALUES
    ('airflow', :'airflow_password'),
    ('superset', :'superset_password'),
    ('lakehouse', :'lakehouse_password'),
    ('bi_reader', :'bi_password')
) AS v(role_name, role_password)
\gexec

SELECT format('CREATE DATABASE %I OWNER %I', v.database_name, v.owner_name)
FROM (VALUES
    ('airflow', 'airflow'),
    ('superset', 'superset'),
    ('lakehouse', 'lakehouse'),
    ('hive_metastore', :'admin_user')
) AS v(database_name, owner_name)
WHERE NOT EXISTS (SELECT 1 FROM pg_database d WHERE d.datname = v.database_name)
\gexec

-- Preserve existing database ownership, role attributes and passwords.
-- Existing application roles must already have LOGIN, as in the original init.
REVOKE CONNECT ON DATABASE airflow FROM PUBLIC;
REVOKE CONNECT ON DATABASE superset FROM PUBLIC;
REVOKE CONNECT ON DATABASE lakehouse FROM PUBLIC;
GRANT CONNECT ON DATABASE airflow TO airflow;
GRANT CONNECT ON DATABASE superset TO superset;
GRANT CONNECT ON DATABASE lakehouse TO lakehouse, bi_reader;

\connect lakehouse
CREATE SCHEMA IF NOT EXISTS demo AUTHORIZATION lakehouse;
CREATE SCHEMA IF NOT EXISTS gold AUTHORIZATION lakehouse;
GRANT USAGE ON SCHEMA demo, gold TO bi_reader;

-- Applies to future tables created by lakehouse (not by other owners).
ALTER DEFAULT PRIVILEGES FOR ROLE lakehouse IN SCHEMA demo
    GRANT SELECT ON TABLES TO bi_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE lakehouse IN SCHEMA gold
    GRANT SELECT ON TABLES TO bi_reader;
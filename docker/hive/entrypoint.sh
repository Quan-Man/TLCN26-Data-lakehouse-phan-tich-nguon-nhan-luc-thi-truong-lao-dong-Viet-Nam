#!/usr/bin/env bash
set -Eeuo pipefail
set +x

log() { printf '[lakehouse-hive] %s\n' "$*" >&2; }
die() { log "ERROR: $*"; exit 1; }

export HIVE_HOME="${HIVE_HOME:-/opt/hive}"
export HIVE_CONF_DIR="${HIVE_CONF_DIR:-$HIVE_HOME/conf}"
export HIVE_DB_HOST="${HIVE_DB_HOST:-postgres}"
export HIVE_DB_PORT="${HIVE_DB_PORT:-5432}"
export HIVE_DB_NAME="${HIVE_DB_NAME:-hive_metastore}"
export HIVE_DB_USER="${HIVE_DB_USER:-postgres}"
export HIVE_DB_PASSWORD="${HIVE_DB_PASSWORD:-${POSTGRES_PASSWORD:-}}"
export HIVE_METASTORE_PORT="${HIVE_METASTORE_PORT:-9083}"
wait_timeout="${HIVE_DB_WAIT_TIMEOUT:-180}"
schema_timeout="${HIVE_SCHEMA_TIMEOUT:-180}"
ready_file="${HIVE_READY_FILE:-/tmp/lakehouse-hive-ready}"

rm -f -- "$ready_file"
trap 'rm -f -- "$ready_file"' EXIT
trap 'exit 143' TERM
trap 'exit 130' INT

[[ -n "$HIVE_DB_PASSWORD" ]] || die "Set HIVE_DB_PASSWORD or POSTGRES_PASSWORD."
[[ "$HIVE_DB_HOST" =~ ^[a-zA-Z0-9_.-]+$ ]] || die "HIVE_DB_HOST must be a hostname or IPv4 address."
[[ "$HIVE_DB_NAME" =~ ^[a-zA-Z0-9_]+$ ]] || die "HIVE_DB_NAME must use letters, digits or underscores."
for variable in HIVE_DB_PORT HIVE_METASTORE_PORT; do
    value="${!variable}"
    [[ "$value" =~ ^[0-9]{1,5}$ ]] || die "$variable must be a port number."
    (( 10#$value >= 1 && 10#$value <= 65535 )) || die "$variable must be between 1 and 65535."
done
for value in "$wait_timeout" "$schema_timeout"; do
    [[ "$value" =~ ^[1-9][0-9]*$ ]] || die "Timeouts must be positive seconds."
done

[[ -r "$HIVE_CONF_DIR/hive-site.xml" ]] || die "Missing readable HIVE_CONF_DIR/hive-site.xml."
[[ -x "$HIVE_HOME/bin/schematool" ]] || die "schematool is not executable."
[[ -x "$HIVE_HOME/bin/hive" ]] || die "Hive executable is missing."
[[ -r "$HIVE_HOME/lib/postgresql-42.7.5.jar" ]] || die "Missing PostgreSQL JDBC driver."
command -v psql >/dev/null || die "Install postgresql-client in the Dockerfile."
command -v timeout >/dev/null || die "The timeout command is required."

export HIVE_METASTORE_JDBC_URL="jdbc:postgresql://${HIVE_DB_HOST}:${HIVE_DB_PORT}/${HIVE_DB_NAME}?currentSchema=public&connectTimeout=5"
export PGPASSWORD="$HIVE_DB_PASSWORD"
export PGCONNECT_TIMEOUT=3
export PGOPTIONS='-c statement_timeout=5000 -c lock_timeout=3000'
psql_command=(psql -X --no-password --no-align --tuples-only --quiet
    --host="$HIVE_DB_HOST" --port="$HIVE_DB_PORT"
    --username="$HIVE_DB_USER" --dbname="$HIVE_DB_NAME" --set=ON_ERROR_STOP=1)

log "Waiting for PostgreSQL database ${HIVE_DB_NAME} (timeout: ${wait_timeout}s)."
deadline=$(( SECONDS + wait_timeout ))
until "${psql_command[@]}" --command='SELECT 1' >/dev/null 2>&1; do
    if (( SECONDS >= deadline )); then
        die "Database is unreachable or credentials are invalid. Ensure PostgreSQL created hive_metastore; schema initialization was not attempted."
    fi
    sleep 1
done

object_count=$("${psql_command[@]}" --command="
SELECT count(*) FROM pg_catalog.pg_class AS c
JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p', 'v', 'm', 'S', 'f');")
[[ "$object_count" =~ ^[0-9]+$ ]] || die "Unexpected database inspection result."

run_schema_tool() {
    timeout --signal=TERM --kill-after=10s "$schema_timeout" \
        "$HIVE_HOME/bin/schematool" -dbType postgres "$@"
}

if [[ "$object_count" == 0 ]]; then
    log "Empty public schema: initializing Hive metastore."
    if ! run_schema_tool -initSchema; then
        die "Schema initialization failed. Inspect the error above; no automatic retry or destructive reset is performed."
    fi
else
    log "Existing database objects found: skipping initSchema."
fi

if ! run_schema_tool -info; then
    die "Metastore schema is incompatible or incomplete. Review schema state/version; do not reinitialize it automatically."
fi

touch -- "$ready_file"
log "Schema is ready; starting Hive Metastore on port ${HIVE_METASTORE_PORT}."
exec "$HIVE_HOME/bin/hive" --service metastore -p "$HIVE_METASTORE_PORT"

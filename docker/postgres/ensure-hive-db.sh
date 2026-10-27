#!/bin/sh
set -eu
if ! psql -h postgres -U postgres -d postgres -Atqc "SELECT 1 FROM pg_database WHERE datname='hive_metastore'" | grep -qx 1; then
  psql -h postgres -U postgres -d postgres -c 'CREATE DATABASE hive_metastore'
fi

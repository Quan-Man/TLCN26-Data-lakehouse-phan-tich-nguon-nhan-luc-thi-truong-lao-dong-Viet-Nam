#!/usr/bin/env bash
set -euo pipefail
superset db upgrade
python /app/register_database.py
superset init

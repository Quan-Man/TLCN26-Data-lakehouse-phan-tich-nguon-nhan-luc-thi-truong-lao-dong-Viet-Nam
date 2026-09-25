#!/usr/bin/env bash
set -euo pipefail
airflow db migrate
python - <<'PY'
import os
import subprocess
from airflow.www.app import create_app

username = os.environ['AIRFLOW_ADMIN_USERNAME']
app = create_app()
with app.app_context():
    exists = app.appbuilder.sm.find_user(username=username) is not None
if not exists:
    subprocess.run([
        'airflow', 'users', 'create', '--username', username,
        '--password', os.environ['AIRFLOW_ADMIN_PASSWORD'],
        '--firstname', 'Lakehouse', '--lastname', 'Admin',
        '--role', 'Admin', '--email', 'admin@example.local'
    ], check=True)
print('Airflow metadata and admin account are ready.')
PY

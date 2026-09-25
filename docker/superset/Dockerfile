FROM apache/superset:4.1.2
USER root
RUN pip install --no-cache-dir psycopg2-binary==2.9.10
COPY --chown=superset:superset docker/superset/superset_config.py /app/pythonpath/superset_config.py
COPY --chown=superset:superset docker/superset/bootstrap.sh /app/bootstrap.sh
COPY --chown=superset:superset docker/superset/register_database.py /app/register_database.py
USER superset

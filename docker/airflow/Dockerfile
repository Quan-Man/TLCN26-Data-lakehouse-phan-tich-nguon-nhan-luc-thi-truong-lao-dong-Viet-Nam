FROM apache/airflow:2.10.5-python3.11

USER root

# Java 17 và curl
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        curl \
    && mkdir -p /opt/java \
    && ln -s "$(dirname "$(dirname "$(readlink -f /usr/bin/java)")")" /opt/java/current \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/opt/java/current

USER airflow

# Spark and Delta Lake
RUN python -m pip install --no-cache-dir --no-deps \
        pyspark==3.5.5 \
        delta-spark==3.3.0 \
        py4j==0.10.9.7

# MinIO Python SDK
RUN python -m pip install --no-cache-dir \
        minio==7.2.20

# Download Spark and Delta Lake JARs
COPY --chown=airflow:root \
    docker/airflow/download_jars.py \
    /tmp/download_jars.py

RUN python /tmp/download_jars.py

# Airflow bootstrap
COPY --chown=airflow:root \
    docker/airflow/bootstrap.sh \
    /opt/airflow/bootstrap.sh

# MinIO bucket initialization script
COPY --chown=airflow:root \
    scripts/ensure_buckets.py \
    /opt/airflow/scripts/ensure_buckets.py

COPY --chown=airflow:root \
    scripts/register_delta_tables.py \
    /opt/airflow/scripts/register_delta_tables.py

USER root

RUN chmod 755 /opt/airflow/bootstrap.sh

USER airflow

FROM apache/airflow:2.10.5-python3.11

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        curl \
    && mkdir -p /opt/java \
    && ln -s "$(dirname "$(dirname "$(readlink -f /usr/bin/java)")")" /opt/java/current \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/opt/java/current

USER airflow

# Spark và Delta Lake
RUN python -m pip install --no-cache-dir --no-deps \
        pyspark==3.5.5 \
        delta-spark==3.3.0 \
        py4j==0.10.9.7 \
    && python -m pip check

RUN python -m pip freeze > /tmp/installed-constraints.txt \
    && python -m pip install --no-cache-dir \
        --constraint /tmp/installed-constraints.txt \
        "apache-airflow==2.10.5" \
        pandas \
        openpyxl \
        pyarrow \
        boto3 \
    && python -m pip check \
    && python -c "import pandas, openpyxl, pyarrow, boto3; print('Pipeline dependencies OK')"

COPY --chown=airflow:root \
    docker/airflow/download_jars.py \
    /tmp/download_jars.py

RUN python /tmp/download_jars.py

COPY --chown=airflow:root \
    docker/airflow/bootstrap.sh \
    /opt/airflow/bootstrap.sh

# Stage 1: Download PostgreSQL JDBC Driver
FROM alpine:3.20 AS jdbc-downloader

ARG POSTGRES_JDBC_VERSION=42.7.5

RUN apk add --no-cache ca-certificates curl \
    && curl -fSL --retry 3 \
       "https://jdbc.postgresql.org/download/postgresql-${POSTGRES_JDBC_VERSION}.jar" \
       -o /tmp/postgresql-jdbc.jar

# Stage 2: Build Hive Metastore
FROM apache/hive:3.1.3

USER root

# Remove old PostgreSQL JDBC drivers
RUN find /opt/hive/lib -maxdepth 1 -type f \
    -name 'postgresql*.jar' -delete \
    && find /opt/hadoop/share/hadoop -type f \
    -name 'postgresql*.jar' -delete

# Install new PostgreSQL JDBC Driver
COPY --from=jdbc-downloader \
    /tmp/postgresql-jdbc.jar \
    /opt/hive/lib/postgresql-42.7.5.jar

RUN chmod 644 /opt/hive/lib/postgresql-42.7.5.jar

USER hive

EXPOSE 9083

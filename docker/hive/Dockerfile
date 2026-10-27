
FROM apache/hive:3.1.3

USER root

# Xoa PostgreSQL JDBC Driver cu
RUN find /opt/hive/lib -maxdepth 1 -type f \
    -name 'postgresql*.jar' -delete \
    && find /opt/hadoop/share/hadoop -type f \
    -name 'postgresql*.jar' -delete

# Copy JDBC Driver tu thu muc du an
COPY docker/hive/jars/postgresql-42.7.5.jar \
    /opt/hive/lib/postgresql-42.7.5.jar

RUN chmod 644 /opt/hive/lib/postgresql-42.7.5.jar

USER hive

EXPOSE 9083

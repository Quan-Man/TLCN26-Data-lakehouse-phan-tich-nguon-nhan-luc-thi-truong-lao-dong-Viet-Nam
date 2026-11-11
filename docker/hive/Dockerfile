FROM apache/hive:3.1.3

USER root

RUN set -eux; \
    find /etc/apt -type f \( -name '*.list' -o -name '*.sources' \) \
        -exec sed -i \
            -e 's|deb.debian.org/debian-security|archive.debian.org/debian-security|g' \
            -e 's|security.debian.org/debian-security|archive.debian.org/debian-security|g' \
            {} +; \
    printf 'Acquire::Check-Valid-Until "false";\n' \
        > /etc/apt/apt.conf.d/99archive; \
    rm -rf /var/lib/apt/lists/*; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        postgresql-client; \
    rm -rf /var/lib/apt/lists/*

COPY docker/hive/jars/postgresql-42.7.5.jar \
    /opt/hive/lib/postgresql-42.7.5.jar

COPY docker/hive/hive-site.xml \
    /opt/hive/conf/hive-site.xml

COPY docker/hive/entrypoint.sh \
    /usr/local/bin/lakehouse-hive-entrypoint.sh

RUN find /opt/hive/scripts/metastore/upgrade/postgres \
      -type f -name '*.sql' \
      -exec sed -i -E \
      '/^[[:space:]]*SET[[:space:]]+default_with_oids[[:space:]]*=/Id' {} + \
    && chmod 644 \
        /opt/hive/lib/postgresql-42.7.5.jar \
        /opt/hive/conf/hive-site.xml \
    && chmod 755 /usr/local/bin/lakehouse-hive-entrypoint.sh \
    && mkdir -p /opt/hive/data/warehouse \
    && chown -R hive /opt/hive/data/warehouse

ENV HIVE_CONF_DIR=/opt/hive/conf

USER hive

EXPOSE 9083

ENTRYPOINT ["/bin/bash", "/usr/local/bin/lakehouse-hive-entrypoint.sh"]
CMD []
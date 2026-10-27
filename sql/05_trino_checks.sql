-- Run after Trino and Hive Metastore are healthy.
SHOW CATALOGS;
SHOW SCHEMAS FROM delta;
-- Tables only appear after Spark registers them in Hive Metastore.

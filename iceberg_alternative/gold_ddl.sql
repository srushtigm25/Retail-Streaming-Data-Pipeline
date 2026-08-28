-- Equivalent gold-layer DDL if the platform were standardized on Iceberg
-- instead of Delta (e.g. because Trino + Flink also need native read/write
-- access to these tables). Run via spark-sql with the Iceberg Spark runtime
-- and a configured catalog (Hive Metastore / Glue / REST catalog):
--
--   spark-sql --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2 \
--     --conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog \
--     --conf spark.sql.catalog.lakehouse.type=hive

CREATE TABLE IF NOT EXISTS lakehouse.gold.dim_customer (
    customer_key   STRING,
    customer_id    STRING,
    customer_name  STRING,
    customer_region STRING,
    start_date     DATE,
    end_date       DATE,
    is_current     BOOLEAN
)
USING iceberg
TBLPROPERTIES (
    'format-version' = '2',                 -- v2 = row-level deletes, needed for MERGE
    'write.upsert.enabled' = 'true'
);

CREATE TABLE IF NOT EXISTS lakehouse.gold.dim_product (
    product_key      STRING,
    product_id       STRING,
    product_name     STRING,
    product_category STRING
)
USING iceberg;

CREATE TABLE IF NOT EXISTS lakehouse.gold.dim_date (
    date_key     INT,
    full_date    DATE,
    year         INT,
    month        INT,
    day          INT,
    day_of_week  STRING,
    is_weekend   BOOLEAN,
    week_of_year INT
)
USING iceberg
PARTITIONED BY (years(full_date));   -- hidden partitioning: no partition column
                                       -- to manage or get wrong in queries

CREATE TABLE IF NOT EXISTS lakehouse.gold.fact_orders (
    order_id     STRING,
    event_id     STRING,
    customer_key STRING,
    product_key  STRING,
    date_key     INT,
    quantity     INT,
    unit_price   DOUBLE,
    order_total  DOUBLE,
    status       STRING,
    event_ts     TIMESTAMP
)
USING iceberg
PARTITIONED BY (status, days(event_ts));

-- SCD2 upsert into dim_customer -- same idea as the Delta MERGE in
-- 02_silver_gold_star_schema.py, Iceberg's syntax is close to identical:
--
-- MERGE INTO lakehouse.gold.dim_customer t
-- USING changed_customers s
-- ON t.customer_id = s.customer_id AND t.is_current = true
-- WHEN MATCHED AND (s.customer_region <> t.customer_region) THEN
--   UPDATE SET t.is_current = false, t.end_date = s.effective_date
-- WHEN NOT MATCHED THEN
--   INSERT (customer_key, customer_id, customer_name, customer_region,
--           start_date, end_date, is_current)
--   VALUES (s.customer_key, s.customer_id, s.customer_name, s.customer_region,
--           s.effective_date, NULL, true);

"""
02_silver_gold_star_schema.py
------------------------------
SILVER: dedupe + clean bronze into a conformed `orders_clean` table.
GOLD:   star schema for BI/analytics consumption --
          fact_orders          (grain: one row per order line item)
          dim_customer         (Type-2 SCD -- tracks region changes over time)
          dim_product
          dim_date

Design notes (why star schema here, not Data Vault):
- Consumers are BI dashboards and a handful of analysts running ad-hoc SQL.
  Star schema optimizes for that: few large joins, denormalized dimensions,
  intuitive to a non-engineer, and query engines (Spark SQL, Trino) plan
  star joins well.
- Data Vault would be the better call if this were a compliance-heavy
  enterprise warehouse ingesting from 10+ disparate source systems where I
  needed full historical traceability of every raw attribute change and
  parallel, decoupled loading by many teams -- Hub/Link/Satellite's insert-only
  model shines there, but it pushes query-time complexity (lots of joins)
  onto every consumer, which isn't worth it for a single-source retail feed
  with a handful of known dashboards. I'd reach for Data Vault as the silver
  layer if this lake had to merge order data from 3 different acquired
  companies' systems with divergent schemas -- not the case here.
- dim_customer is Type-2 (SCD2): region is a slowly changing attribute and
  the business wants "what region was this customer in when they ordered"
  preserved for historical reporting accuracy, not overwritten.
"""
import os

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql import functions as F
from pyspark.sql.window import Window

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LAKE_ROOT = os.environ.get("LAKE_ROOT", os.path.join(BASE_DIR, "lake"))
BRONZE_PATH = os.path.join(LAKE_ROOT, "bronze", "orders")
SILVER_PATH = os.path.join(LAKE_ROOT, "silver", "orders_clean")
DIM_CUSTOMER_PATH = os.path.join(LAKE_ROOT, "gold", "dim_customer")
DIM_PRODUCT_PATH = os.path.join(LAKE_ROOT, "gold", "dim_product")
DIM_DATE_PATH = os.path.join(LAKE_ROOT, "gold", "dim_date")
FACT_ORDERS_PATH = os.path.join(LAKE_ROOT, "gold", "fact_orders")


def get_spark():
    builder = (
        SparkSession.builder.appName("SilverGoldStarSchema")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "4")
    )

    return configure_spark_with_delta_pip(builder).getOrCreate()


def build_silver(spark):
    """Dedup on event_id (Kafka at-least-once can redeliver), drop test/garbage rows."""
    bronze = spark.read.format("delta").load(BRONZE_PATH)

    w = Window.partitionBy("event_id").orderBy(F.col("_ingested_at").desc())
    silver = (
        bronze.withColumn("_rn", F.row_number().over(w))
        .filter("_rn = 1")
        .drop("_rn")
        .filter(F.col("order_total") > 0)
        .filter(F.col("status").isin("PLACED", "PAID", "SHIPPED", "CANCELLED", "REFUNDED"))
    )

    silver.write.format("delta").mode("overwrite").save(SILVER_PATH)
    print(f"[silver] {silver.count()} clean rows -> {SILVER_PATH}")
    return silver


def build_dim_product(spark, silver):
    """Product dimension: Type-1 (overwrite) -- catalog attributes aren't
    something the business needs history for; current price is what matters."""
    dim = (
        silver.select("product_id", "product_name", "product_category")
        .distinct()
        .withColumn("product_key", F.md5(F.col("product_id")))
    )
    dim.write.format("delta").mode("overwrite").save(DIM_PRODUCT_PATH)
    print(f"[gold] dim_product: {dim.count()} rows")


def build_dim_date(spark, silver):
    """Standard date dimension generated from the min/max order dates seen."""
    bounds = silver.agg(
        F.min(F.to_date("event_ts")).alias("min_d"), F.max(F.to_date("event_ts")).alias("max_d")
    ).collect()[0]

    dim = (
        spark.sql(f"SELECT explode(sequence(to_date('{bounds['min_d']}'), "
                  f"to_date('{bounds['max_d']}'), interval 1 day)) as full_date")
        .withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast("int"))
        .withColumn("year", F.year("full_date"))
        .withColumn("month", F.month("full_date"))
        .withColumn("day", F.dayofmonth("full_date"))
        .withColumn("day_of_week", F.date_format("full_date", "EEEE"))
        .withColumn("is_weekend", F.dayofweek("full_date").isin(1, 7))
        .withColumn("week_of_year", F.weekofyear("full_date"))
    )
    dim.write.format("delta").mode("overwrite").save(DIM_DATE_PATH)
    print(f"[gold] dim_date: {dim.count()} rows")


def build_dim_customer_scd2(spark, silver):
    """
    Type-2 SCD via Delta MERGE: when a customer's region changes, close out
    the old row (set is_current=false, end_date=now) and insert a new
    current row -- this is the textbook Delta MERGE pattern for SCD2 and is
    exactly the kind of thing worth walking through live, since it's the
    part plain Parquet genuinely can't do atomically (no MERGE/UPSERT).
    """
    from delta.tables import DeltaTable

    latest_per_customer = (
        silver.withColumn(
            "_rn",
            F.row_number().over(
                Window.partitionBy("customer_id").orderBy(F.col("event_ts").desc())
            ),
        )
        .filter("_rn = 1")
        .select(
            "customer_id",
            "customer_name",
            "customer_region",
            F.to_date("event_ts").alias("effective_date"),
        )
    )
    first_seen = (
    silver.groupBy("customer_id")
    .agg(F.min(F.to_date("event_ts")).alias("first_seen_date"))
    )

    latest_per_customer = (
        latest_per_customer
        .join(first_seen, "customer_id")
    )
    if not DeltaTable.isDeltaTable(spark, DIM_CUSTOMER_PATH):
        init = (
            latest_per_customer.withColumn("customer_key", F.md5(F.col("customer_id")))
            .withColumn("start_date", F.col("first_seen_date"))
            .withColumn("end_date", F.lit(None).cast("date"))
            .withColumn("is_current", F.lit(True))
            .drop("effective_date", "first_seen_date")
        )
        init.write.format("delta").mode("overwrite").save(DIM_CUSTOMER_PATH)
        print(f"[gold] dim_customer initialized: {init.count()} rows")
        return

    dim_customer = DeltaTable.forPath(spark, DIM_CUSTOMER_PATH)
    current = dim_customer.toDF().filter("is_current = true")

    changed = (
    latest_per_customer.alias("src")
    .join(current.alias("cur"), "customer_id")
    .where(
        "src.customer_region <> cur.customer_region "
        "OR src.customer_name <> cur.customer_name"
    )
    .select("src.*")
    .cache()
    )
  
    changed_count = changed.count()

    if changed_count == 0:
        print("[gold] dim_customer: no SCD2 changes detected")
        return

    # Step 1: close out changed current rows
    dim_customer.alias("t").merge(
        changed.alias("s"), "t.customer_id = s.customer_id AND t.is_current = true"
    ).whenMatchedUpdate(
        set={"is_current": "false", "end_date": "s.effective_date"}
    ).execute()

    # Step 2: insert new current rows
    new_rows = (
        changed.withColumn("customer_key", F.md5(F.concat("customer_id", F.current_timestamp().cast("string"))))
        .withColumn("start_date", F.col("effective_date"))
        .withColumn("end_date", F.lit(None).cast("date"))
        .withColumn("is_current", F.lit(True))
        .drop("effective_date", "first_seen_date")
    )
    new_rows.write.format("delta").mode("append").save(DIM_CUSTOMER_PATH)
    print(f"[gold] dim_customer: closed + inserted {changed_count} SCD2 changes")
    changed.unpersist()


def build_fact_orders(spark, silver):
    dim_customer = spark.read.format("delta").load(DIM_CUSTOMER_PATH)
    dim_product = spark.read.format("delta").load(DIM_PRODUCT_PATH)

    fact = (
        silver.alias("s")
        .join(
            dim_customer.alias("dc"),
            (F.col("s.customer_id") == F.col("dc.customer_id"))
            & (F.to_date(F.col("s.event_ts")) >= F.col("dc.start_date"))
            & (
                F.col("dc.end_date").isNull()
                | (F.to_date(F.col("s.event_ts")) < F.col("dc.end_date"))
            ),
        )
        .join(dim_product.alias("dp"), "product_id")
        .select(
            "s.order_id",
            "s.event_id",
            F.col("dc.customer_key"),
            F.col("dp.product_key"),
            F.date_format("s.event_ts", "yyyyMMdd").cast("int").alias("date_key"),
            "s.quantity",
            "s.unit_price",
            "s.order_total",
            "s.status",
            "s.event_ts",
        )
    )
    fact.write.format("delta").mode("overwrite").partitionBy("status").save(FACT_ORDERS_PATH)
    print(f"[gold] fact_orders: {fact.count()} rows")


def run_all():
    spark = get_spark()
    silver = build_silver(spark)
    build_dim_product(spark, silver)
    build_dim_date(spark, silver)
    build_dim_customer_scd2(spark, silver)
    build_fact_orders(spark, silver)

    fact = spark.read.format("delta").load(FACT_ORDERS_PATH)
    dim_customer = spark.read.format("delta").load(DIM_CUSTOMER_PATH)
    dim_product = spark.read.format("delta").load(DIM_PRODUCT_PATH)

    fact.createOrReplaceTempView("fact_orders")
    dim_customer.createOrReplaceTempView("dim_customer")
    dim_product.createOrReplaceTempView("dim_product")

    print("\n[gold] sample business question -- revenue by region & category:")

    spark.sql("""
        SELECT
            dc.customer_region,
            dp.product_category,
            ROUND(SUM(f.order_total), 2) AS revenue,
            COUNT(*) AS orders
        FROM fact_orders f
        JOIN dim_customer dc
            ON f.customer_key = dc.customer_key
            AND dc.is_current = true
        JOIN dim_product dp
            ON f.product_key = dp.product_key
        WHERE f.status NOT IN ('CANCELLED', 'REFUNDED')
        GROUP BY dc.customer_region, dp.product_category
        ORDER BY revenue DESC
        LIMIT 10
    """).show()

    spark.stop()


def run_stage(stage: str):
    """Entry point used by the Airflow DAG, which calls each stage as a
    separate task so failures/retries are isolated per stage instead of
    re-running the whole pipeline on any single failure."""
    spark = get_spark()
    silver = spark.read.format("delta").load(SILVER_PATH) if stage != "silver" else None

    if stage == "silver":
        build_silver(spark)
    elif stage == "dim_customer":
        build_dim_customer_scd2(spark, silver)
    elif stage == "dim_product":
        build_dim_product(spark, silver)
    elif stage == "dim_date":
        build_dim_date(spark, silver)
    elif stage == "fact":
        build_fact_orders(spark, silver)
    else:
        raise ValueError(f"Unknown stage: {stage}")
    spark.stop()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--stage",
        choices=["silver", "dim_customer", "dim_product", "dim_date", "fact", "all"],
        default="all",
    )
    args = parser.parse_args()

    if args.stage == "all":
        run_all()
    else:
        run_stage(args.stage)

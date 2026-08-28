"""
01_bronze_ingest.py
--------------------
BRONZE layer: raw, append-only, schema-enforced landing zone.

Reads order events (from local JSON files here; from the `orders.raw` Kafka
topic in prod -- see the commented block below for that version) and writes
them into a Delta Lake table with ACID transactions, so a job that crashes
mid-write never leaves the table half-updated, and concurrent readers never
see partial data.

Why Delta Lake for bronze (vs. plain Parquet):
- Delta's transaction log (_delta_log) gives ACID + time travel with a small
  operational footprint -- good fit for a single-writer streaming ingest path
  where I mostly need append-safety and the ability to replay/rollback.
- See docs/table_format_tradeoffs.md for the full Delta vs. Iceberg comparison
  and iceberg_alternative/ for the equivalent gold-layer DDL in Iceberg -- I
  used Delta end-to-end here since it doesn't need a Maven-resolved catalog
  jar to run, but the trade-off write-up covers why a multi-engine shop
  (Spark + Trino + Flink all reading the same table on OpenShift) would lean
  Iceberg for the layers other teams query directly.
"""
import os

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import col, current_timestamp, input_file_name, to_timestamp, from_json
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    DoubleType,
    IntegerType,
)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LAKE_ROOT = os.environ.get("LAKE_ROOT", os.path.join(BASE_DIR, "lake"))
RAW_STREAM_DIR = os.path.join(LAKE_ROOT, "raw_stream")
BRONZE_PATH = os.path.join(LAKE_ROOT, "bronze", "orders")
CHECKPOINT_PATH = os.path.join(LAKE_ROOT, "_checkpoints", "bronze_orders")

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("customer_name", StringType()),
    StructField("customer_region", StringType()),
    StructField("product_id", StringType()),
    StructField("product_name", StringType()),
    StructField("product_category", StringType()),
    StructField("unit_price", DoubleType()),
    StructField("quantity", IntegerType()),
    StructField("order_total", DoubleType()),
    StructField("status", StringType()),
    StructField("event_ts", StringType()),
    StructField("ingest_source", StringType()),
])


def get_spark():
    builder = (
        SparkSession.builder.appName("BronzeIngest")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        
        .config("spark.sql.shuffle.partitions", "4")
    )
        
    return configure_spark_with_delta_pip(
    builder,
    extra_packages=["org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"],
).getOrCreate()


def run_batch_ingest():
    """
    Batch mode: reads whatever JSON files currently sit in raw_stream/ and
    appends them to the bronze Delta table in one ACID commit. This is the
    mode used for the historical backfill and for the demo (deterministic,
    finishes immediately). run_streaming_ingest() below is the always-on
    version for a real deployment.
    """
    spark = get_spark()
    df = (
        spark.read.schema(EVENT_SCHEMA)
        .json(RAW_STREAM_DIR)
        .withColumn("event_ts", to_timestamp("event_ts"))
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", input_file_name())
    )

    (
        df.write.format("delta")
        .mode("append")
        .partitionBy("customer_region")
        .save(BRONZE_PATH)
    )

    count = spark.read.format("delta").load(BRONZE_PATH).count()
    print(f"[bronze] table now has {count} rows at {BRONZE_PATH}")

    # --- ACID / time travel proof, the thing worth demoing live ---
    from delta.tables import DeltaTable

    dt = DeltaTable.forPath(spark, BRONZE_PATH)
    print("\n[bronze] transaction history (this IS the ACID log):")
    dt.history(5).select("version", "timestamp", "operation", "operationMetrics").show(
        truncate=False
    )

    spark.stop()


def run_streaming_ingest():
    """
    Real always-on version: Structured Streaming with a file source here,
    or a Kafka source in prod (commented). Exactly-once semantics come from
    the checkpoint (tracks which Kafka offsets / files have been committed)
    combined with Delta's atomic commits -- reprocessing after a crash never
    double-writes.
    """
    spark = get_spark()

    # --- Kafka version (prod) ---
    # raw = (
    #     spark.readStream.format("kafka")
    #     .option("kafka.bootstrap.servers", "kafka:9092")
    #     .option("subscribe", "orders.raw")
    #     .option("startingOffsets", "latest")
    #     .option("maxOffsetsPerTrigger", 1000)
    #     .load()
    # )
    # df = raw.select(from_json(col("value").cast("string"), EVENT_SCHEMA).alias("e")).select("e.*")

    # --- File-source version (local demo, same downstream code) ---
    # df = (
    #     spark.readStream.schema(EVENT_SCHEMA)
    #     .json(RAW_STREAM_DIR)
    #     .withColumn("event_ts", to_timestamp("event_ts"))
    #     .withColumn("_ingested_at", current_timestamp())
    # )

    raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "orders.raw")
    .option("startingOffsets", "latest")
    .option("maxOffsetsPerTrigger", 1000)
    .load()
    )

    df = (
        raw.select(
            from_json(col("value").cast("string"), EVENT_SCHEMA).alias("e")
        )
        .select("e.*")
        .withColumn("event_ts", to_timestamp("event_ts"))
        .withColumn("_ingested_at", current_timestamp())
    )

    query = (
        df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .partitionBy("customer_region")
        .trigger(processingTime="5 seconds")
        .start(BRONZE_PATH)
    )
    return query, spark


if __name__ == "__main__":
    import sys

    mode = sys.argv[1] if len(sys.argv) > 1 else "batch"
    if mode == "batch":
        run_batch_ingest()
    else:
        q, spark = run_streaming_ingest()
        q.awaitTermination(timeout=60)
        spark.stop()

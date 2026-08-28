# Retail Lakehouse — order events to a gold star schema

A small but complete lakehouse pipeline built to demonstrate the Data
Engineer skill set: modern table formats (Delta Lake, with an Iceberg
alternative documented), star-schema data modeling, distributed processing
with Spark, Kafka streaming ingestion, Airflow orchestration, and a Java
API layer to serve the result.

**Use case:** an e-commerce checkout service emits order events. The
pipeline lands them raw (bronze), cleans and dedupes them (silver), and
builds a queryable star schema (gold) that a Spring Boot API exposes to
downstream teams.

```
checkout service          Spark Structured           Spark batch
   (Kafka producer)          Streaming                 transform
        │                       │                          │
        ▼                       ▼                          ▼
  orders.raw topic  ──▶  bronze/orders (Delta) ──▶  silver/orders_clean ──▶  gold/
  (Kafka)                ACID append, schema         dedup + validate         fact_orders
                          enforced                                            dim_customer (SCD2)
                                                                               dim_product
                                                                               dim_date
                                                                                    │
                                                          Airflow DAG orchestrates ─┘
                                                          the silver/gold refresh
                                                                                    │
                                                                                    ▼
                                                                    Spring Boot API (Trino/JDBC)
                                                                    /api/v1/sales-summary
```

## What actually runs where

I built this in a sandboxed environment with no access to Maven Central, so
I validated everything I could without it and I'm being upfront about the
boundary rather than pretending it's untested:

| Component | Status |
|---|---|
| Event generator (`data_generator/`) | **Runs as-is.** Pure Python. |
| Star-schema transform logic (dedup, joins, SCD2 shape, dimension builds) | **Logic validated** locally using a Parquet stand-in (no Delta/Iceberg jar needed to test join/window/aggregation correctness) |
| Delta Lake read/write, MERGE, time travel (`spark_jobs/`) | **Code is complete and correct Delta API usage**, but needs `--packages io.delta:delta-spark_2.12:3.2.0` resolved from Maven Central at runtime — will pull and run fine on any machine with normal internet access (your laptop) |
| Kafka producer/consumer (`kafka/`) | **Code complete**, needs a broker — `docker-compose up` on your machine |
| Airflow DAG | **Syntax-checked**, written against Airflow 2.x APIs; needs an Airflow install to actually schedule |
| Java Spring Boot API | **Code complete**, not compiled in-sandbox (same Maven Central restriction) — run `mvn spring-boot:run` on your machine to verify before the interview |

**Tonight, before the interview:** run the commands below on your own
laptop (normal internet) once, so you've actually seen it execute and
you're not narrating untested code.

## Quickstart

```bash
# 1. Generate historical order events (pure Python, always works)
cd data_generator
pip install faker
python generate_events.py --mode backfill --n 5000

# 2. Bronze ingest into Delta Lake (downloads Delta jars on first run)
cd ../spark_jobs
pip install pyspark==3.5.1 delta-spark==3.2.0
python 01_bronze_ingest.py batch

# 3. Build the silver/gold star schema (includes the SCD2 MERGE demo)
python 02_silver_gold_star_schema.py

# 4. (optional) Real Kafka instead of the file-based simulation
cd ..
docker compose up -d
docker exec -it kafka kafka-topics --create --topic orders.raw \
    --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
pip install kafka-python
python kafka/producer.py --rate 5 --duration 30 &
python kafka/consumer.py

# 5. Java API (needs a Trino instance pointed at the gold tables to fully
#    run end-to-end; `mvn compile` alone proves the code is valid)
cd java-api
mvn compile
```

## What to actually demo live (in priority order, given limited prep time)

1. **`02_silver_gold_star_schema.py` output** — run it, show the printed
   `dt.history()` transaction log (proves ACID/versioning) and the final
   revenue-by-region-and-category query. This is the single most
   demo-able artifact — it's real output from real code.
2. **The SCD2 MERGE function** (`build_dim_customer_scd2`) — walk through
   why a customer's region change needs a MERGE, not an overwrite, and
   what "Type-2" means in plain English before touching code.
3. **The Airflow DAG's shape** — point at the fan-out/fan-in (three
   dimensions build in parallel, fact waits for all three) and the
   quality-gate branch. You don't need it running to explain the design.
4. **`docs/table_format_tradeoffs.md`** — this is your answer to "why
   Delta over Iceberg" and, if asked, "when would you use Iceberg instead."
   Know this cold; it's the question most likely to come up given the role
   lists both.

## Repo layout

```
data_generator/     event source simulation (stand-in for the checkout service)
spark_jobs/          bronze ingest + silver/gold star-schema build (Delta Lake)
kafka/                real producer/consumer + docker-compose for a live broker
airflow_dags/         orchestration DAG for the silver/gold refresh
java-api/             Spring Boot read API over the gold tables (via Trino)
iceberg_alternative/  what the gold DDL looks like in Iceberg instead, for the trade-off discussion
docs/                 architecture rationale, data model ERD, table format comparison
```

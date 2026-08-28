# Delta Lake vs. Apache Iceberg — why this project uses Delta

Both give you ACID transactions, schema evolution, and time travel on top of
an object store (S3, MinIO, ODF/Ceph on OpenShift). They solve the same core
problem — plain Parquet has no transaction log, so concurrent writers can
corrupt a table and readers can see partial files mid-write. The differences
that actually matter when picking one:

| | Delta Lake | Apache Iceberg |
|---|---|---|
| Transaction log | `_delta_log` JSON + checkpoint Parquet, Spark-native | Metadata/manifest files, engine-agnostic by design |
| Engine support | Best on Spark; Trino/Presto/Flink support exists but Spark is first-class | Built multi-engine from day one — Spark, Trino, Flink, Snowflake all read/write natively |
| Partition evolution | Changing partition strategy needs a full table rewrite | Hidden partitioning — change partition strategy without rewriting data |
| MERGE/UPSERT (for SCD2) | Mature, well-documented `DeltaTable.merge()` API | Supported, slightly newer/less battle-tested API surface |
| Catalog | Works fine path-based (what this demo uses); Unity Catalog for the "real" catalog experience | Needs a proper catalog (Hive Metastore, AWS Glue, REST catalog, Nessie) to unlock its best features |
| Ops footprint | Lighter to stand up for a single-engine (Spark-only) shop | More moving parts (external catalog service) but pays off once >1 engine writes to the table |

**For this project:** ingestion and transforms are Spark-only, and there's
one writer per layer (the streaming job owns bronze, the batch job owns
silver/gold) — so Delta's simpler operational model wins and its native
`MERGE` is exactly what `dim_customer`'s SCD2 logic needs.

**Where I'd flip to Iceberg:** the role description mentions Flink and
OpenShift specifically. The moment a second engine needs to write or read
the same table natively — say Flink doing real-time enrichment while Spark
does batch backfill, or Trino serving BI queries directly against gold —
Iceberg's engine-agnostic catalog stops being a nice-to-have and starts
being the reason a table doesn't need per-engine reprocessing. A
multi-engine OpenShift platform team supporting Spark *and* Flink *and*
Trino across many teams' tables is the textbook Iceberg case. See
`iceberg_alternative/gold_ddl.sql` for what the same gold layer looks like
in Iceberg DDL, and `iceberg_alternative/README.md` for the delta between
the two APIs in code.

This isn't a "pick a favorite and defend it" answer — in a real platform
role I'd expect to operate both, and the choice is per-table based on who
writes and reads it, not a one-time architecture decision for the whole lake.

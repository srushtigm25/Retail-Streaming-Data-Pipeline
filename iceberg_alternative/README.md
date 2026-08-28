# Iceberg alternative

`gold_ddl.sql` shows what the gold layer's table definitions look like in
Iceberg instead of Delta. Two differences worth being able to speak to:

1. **Hidden partitioning** (`PARTITIONED BY (years(full_date))`) — Iceberg
   partitions are transforms of a column, not a separate physical column
   you have to remember to filter on. In Delta, `dim_date` would need an
   explicit `year` column and every query has to know to filter on it for
   partition pruning to kick in. Iceberg figures it out from the query
   predicate on `full_date` directly.

2. **`format-version = '2'`** — Iceberg v1 tables are append-only at the
   file level; v2 adds row-level deletes, which is what makes `MERGE`
   (needed for the `dim_customer` SCD2 upsert) possible. This is a detail
   worth knowing if asked "does Iceberg support MERGE" — yes, but only on
   v2 tables, and it's an explicit table property, not a default.

The `MERGE INTO` syntax itself is nearly identical between Delta and
Iceberg (both are close to standard SQL MERGE) — that similarity is
itself worth mentioning if asked "how hard would it be to migrate" —
the harder part of a Delta→Iceberg migration is usually the catalog
(moving from path-based/Hive Metastore to whatever REST/Glue catalog
you're centralizing on), not the DML.

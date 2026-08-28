# Interview walkthrough script

## 30-second framing (say this first)
"I built a small retail order-events lakehouse to mirror this role's scope:
Kafka ingestion, a bronze/silver/gold Delta Lake pipeline with a star
schema, Airflow orchestration, and a Java API to serve it. I'll walk
through the design decisions and trade-offs, not just the code."

## Walking through each skill area

**1. Modern table formats (Delta Lake)**
- What it gives you that Parquet alone doesn't: a transaction log, so
  concurrent writers can't corrupt the table and readers never see a
  half-committed batch. Point at `dt.history()` output if you ran it.
- The SCD2 `MERGE` in `dim_customer` is your best concrete example — it's
  literally the operation plain Parquet can't do atomically.
- If asked "why not Iceberg": give the multi-engine answer from
  `table_format_tradeoffs.md` — Delta for single-engine-Spark ownership,
  Iceberg once Flink/Trino need native write access to the same table.
  Don't just say "Delta is simpler" — say *why* that trade-off was correct
  for *this* pipeline's writer pattern.

**2. Data modeling (star schema)**
- State the grain first, unprompted: "fact_orders is one row per order
  line item." Interviewers listen for whether you actually thought about
  grain or just built tables.
- Explain the Type-2 vs Type-1 choice (`dim_customer` vs `dim_product`) as
  a business-requirements decision, not a technical default — "region
  history matters for revenue-by-region trend accuracy; product category
  doesn't need historical versioning for this use case."
- Have the Data Vault answer ready even though you didn't build one: single
  source system → star schema is the right call; multiple divergent
  source systems merging → Data Vault's insert-only Hub/Link/Satellite
  model would be worth the extra query-time joins.

**3. Distributed processing (Spark)**
- Talk about the bronze/silver/gold layering itself as the answer to
  "how do you handle large-scale transforms" — bronze is append-only
  (cheap, fast writes), silver dedupes/validates (the expensive shuffle
  step happens once, not on every downstream read), gold is
  pre-joined/aggregated for consumption (BI tools never join raw data).
- If asked about Flink instead of Spark for streaming: be honest that this
  demo uses Spark Structured Streaming for bronze ingestion, and explain
  when you'd reach for Flink instead — genuinely low-latency, event-at-a-
  time processing (sub-second) or complex event-time/session windowing
  where Spark's micro-batch model adds latency you can't afford. This
  pipeline's freshness requirement (minutes, not milliseconds) is why
  Spark was the reasonable choice here.

**4. Streaming / Kafka**
- Explain the partition key choice: `customer_id` as the Kafka message key
  guarantees per-customer event ordering (important if any downstream
  logic assumes a customer's events arrive in order), at the cost of
  potential hot partitions if one customer generates disproportionate
  volume — a trade-off worth naming even though it's not the bottleneck
  in a demo-scale system.
- `acks=all` in the producer: durability over raw throughput, appropriate
  for order events (losing an order is worse than a slower producer).

**5. Orchestration (Airflow)**
- The DAG's structure *is* the answer to "why Airflow": three dimension
  builds run in parallel and fan into the fact table build, with a
  branching data-quality gate between bronze and silver. Point at the
  `>>` dependency graph and explain you modeled the *real* dependency
  shape rather than a linear script.
- If asked about Argo Workflows (it's in the JD): Airflow owns the batch
  refresh DAG here; a long-lived streaming job (Flink, or Spark Structured
  Streaming in always-on mode) doesn't fit Airflow's "tasks complete"
  model — that's deployed as a standing Kubernetes/OpenShift resource and
  managed with Argo CD for the deployment lifecycle, not scheduled as a DAG
  task. Naming this distinction is the kind of answer that shows real
  platform experience rather than "Airflow does everything."

**6. Java**
- The Spring Boot API queries the gold tables via Trino/JDBC rather than
  embedding a Spark session — explain *why*: Spark's startup latency is
  wrong for a request/response API; Trino is built for interactive SQL
  latency over the same lake files, no separate serving copy of the data.
- The `/internal/cache/refresh` endpoint the Airflow DAG calls after a
  successful gold rebuild — ties orchestration and the API together and
  shows you thought about cache invalidation, not just "add @Cacheable and
  move on."

## Questions to expect, and honest answers

- **"Did you actually run this?"** — Be straight: the transform logic
  (joins, dedup, window functions, SCD2 shape) was validated locally; the
  Delta-specific and Java pieces need normal internet access to pull
  dependencies from Maven Central, which I ran on my own machine before
  this interview. Don't oversell it as a fully deployed production system
  — it's a design-and-implementation exercise, and treating it as anything
  more invites a question you can't back up.
- **"What would you change for real production scale?"** — Partitioning
  strategy would need real data-volume analysis (this demo's partitioning
  choices are reasonable defaults, not tuned against actual skew); I'd add
  proper data quality tooling (Great Expectations / dbt tests) instead of
  the inline threshold check in the Airflow DAG; the Java API's Trino
  connection would need connection pooling and circuit-breaking for real
  traffic.
- **"Why this use case (retail orders) and not something else?"** — It's a
  domain simple enough to reason about in an interview setting but with
  genuine data-modeling decisions (SCD2, grain, partition-by-status) baked
  in — didn't want a toy example that dodges the interesting trade-offs.

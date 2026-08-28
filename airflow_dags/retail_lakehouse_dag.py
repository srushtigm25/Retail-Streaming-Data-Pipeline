"""
retail_lakehouse_dag.py
------------------------
Orchestrates the daily lakehouse refresh: bronze ingest -> data quality gate
-> silver clean -> gold star schema build -> notify.

Why Airflow here (over raw cron or a Flink-only pipeline):
- The dependency graph is genuinely a DAG, not a straight line: gold's fact
  table depends on ALL THREE dimensions being rebuilt first, and I want that
  fan-in modeled explicitly rather than hoping a shell script's ordering
  happens to be right.
- Built-in retry/backoff + alerting on failure matters for a pipeline a
  business team depends on for daily numbers -- I don't want to find out a
  job silently died three days ago from a Slack ping asking why the
  dashboard looks stale.
- On OpenShift specifically, this would run via the Airflow K8s executor (or
  the KubernetesPodOperator per task) so each task gets its own pod --
  cleaner resource isolation than a shared worker, and it's the same
  operator API this DAG uses, so the code doesn't change moving from a local
  Celery/local executor to that.

If this were pure event-driven/continuous streaming (no daily batch
boundary, e.g. a Flink job that never "finishes"), I'd manage that
deployment lifecycle with Argo Workflows / Argo CD on OpenShift instead --
Airflow's execution model assumes tasks complete, which doesn't fit an
always-on streaming job. This DAG is specifically for the batch
silver/gold refresh; the bronze streaming ingest itself runs as a
long-lived Spark Structured Streaming job (or Flink job) outside Airflow,
deployed via a K8s/OpenShift Deployment, not a DAG task.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator  # DummyOperator is deprecated as of Airflow 2.4+

SPARK_JOBS_DIR = "/opt/lakehouse/spark_jobs"
SPARK_SUBMIT = "python"

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email": ["data-eng-alerts@example.com"],
}

with DAG(
    dag_id="retail_lakehouse_daily_refresh",
    description="Bronze -> Silver -> Gold star schema refresh for the retail lakehouse",
    default_args=default_args,
    schedule=None,#schedule="0 3 * * *",  # 3am daily, after upstream OLTP nightly close
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,  # never let two refreshes race on the same Delta table
    tags=["lakehouse", "retail", "star-schema"],
) as dag:

    start = EmptyOperator(task_id="start")

    bronze_ingest = BashOperator(
        task_id="bronze_ingest_batch",
        bash_command=f"{SPARK_SUBMIT} {SPARK_JOBS_DIR}/01_bronze_ingest.py batch",
    )

    def _quality_gate(**context):
        """
        Data quality gate between bronze and silver. In prod this would be a
        Great Expectations or dbt-test suite; kept inline here to show the
        DECISION POINT in the DAG (branch to quarantine vs. proceed) rather
        than the specific validation framework.
        """
        from pyspark.sql import SparkSession
        from delta import configure_spark_with_delta_pip

        builder = (
            SparkSession.builder
            .appName("dq_check")
            .config(
                "spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension",
            )
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
        )

        spark = configure_spark_with_delta_pip(builder).getOrCreate()

        bronze = spark.read.format("delta").load(
            "/opt/lakehouse/lake/bronze/orders"
        )

        total = bronze.count()
        nulls = bronze.filter("order_id IS NULL OR customer_id IS NULL").count()
        bad_totals = bronze.filter("order_total <= 0").count()
        null_rate = nulls / total if total else 1
        spark.stop()

        if null_rate > 0.02 or bad_totals > total * 0.02:
            return "quarantine_and_alert"
        return "build_silver"

    quality_gate = BranchPythonOperator(
        task_id="quality_gate",
        python_callable=_quality_gate,
    )

    quarantine_and_alert = BashOperator(
        task_id="quarantine_and_alert",
        bash_command="echo 'Data quality threshold breached -- routing to quarantine, paging on-call' && exit 1",
    )

    build_silver = BashOperator(
        task_id="build_silver",
        bash_command=(
            f"{SPARK_SUBMIT} {SPARK_JOBS_DIR}/02_silver_gold_star_schema.py --stage silver"
        ),
    )

    # Dimensions can build in parallel -- they don't depend on each other,
    # only on silver. Fact table fans back in and waits for all three.
    build_dim_customer = BashOperator(
        task_id="build_dim_customer_scd2",
        bash_command=f"{SPARK_SUBMIT} {SPARK_JOBS_DIR}/02_silver_gold_star_schema.py --stage dim_customer",
    )
    build_dim_product = BashOperator(
        task_id="build_dim_product",
        bash_command=f"{SPARK_SUBMIT} {SPARK_JOBS_DIR}/02_silver_gold_star_schema.py --stage dim_product",
    )
    build_dim_date = BashOperator(
        task_id="build_dim_date",
        bash_command=f"{SPARK_SUBMIT} {SPARK_JOBS_DIR}/02_silver_gold_star_schema.py --stage dim_date",
    )

    build_fact_orders = BashOperator(
        task_id="build_fact_orders",
        bash_command=f"{SPARK_SUBMIT} {SPARK_JOBS_DIR}/02_silver_gold_star_schema.py --stage fact",
    )

    refresh_java_api_cache = BashOperator(
    task_id="warm_api_cache",
    bash_command=(
        "echo 'Local demo: API cache refresh skipped. "
        "Production target is the cluster-local Spring Boot service.'"
    ),
)

    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")

    (
        start
        >> bronze_ingest
        >> quality_gate
        >> [build_silver, quarantine_and_alert]
    )
    build_silver >> [build_dim_customer, build_dim_product, build_dim_date] >> build_fact_orders
    build_fact_orders >> refresh_java_api_cache >> end
    quarantine_and_alert >> end

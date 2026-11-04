"""Airflow DAG for manual on-demand triggering of the e-commerce streaming pipeline.

This DAG is NOT responsible for keeping the pipeline running — the producer
and Spark containers do that continuously on their own. This DAG is used when
you want to trigger an ad-hoc run, backfill data, or inspect task logs via the UI.

Two tasks run sequentially:
    1. produce_order_to_kafka     — fetch carts from API and publish to Kafka
    2. transform_and_write_to_postgres — run Spark to consume Kafka and write to PostgreSQL
"""

from datetime import datetime
import os

from airflow import DAG
from airflow.operators.bash import BashOperator

# ── Spark JAR paths ────────────────────────────────────────────────────────────
# These JARs are bundled in the repo under jars/ instead of downloaded at runtime.
# This avoids Maven HTTPS failures and makes the job work on any network.
SPARK_KAFKA_JARS = ",".join(
    [
        "/opt/airflow/project/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar",
        "/opt/airflow/project/jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar",
        "/opt/airflow/project/jars/kafka-clients-3.4.1.jar",
        "/opt/airflow/project/jars/commons-pool2-2.11.1.jar",
        "/opt/airflow/project/jars/lz4-java-1.8.0.jar",
        "/opt/airflow/project/jars/snappy-java-1.1.10.1.jar",
        "/opt/airflow/project/jars/zstd-jni-1.5.2-1.jar",
    ]
)

# ── Environment variables passed to both tasks ─────────────────────────────────
# append_env=True on the BashOperator merges these with the existing container
# environment instead of replacing it — this preserves PATH so spark-submit
# can be found without specifying its full path.
PIPELINE_ENV = {
    "API_URL": os.getenv("API_URL", "https://dummyjson.com/carts"),
    "API_VERIFY_SSL": os.getenv("API_VERIFY_SSL", "true"),
    "ORDER_LIMIT": os.getenv("ORDER_LIMIT", "10"),
    "PRODUCER_SLEEP_SECONDS": os.getenv("PRODUCER_SLEEP_SECONDS", "5"),
    "KAFKA_BOOTSTRAP_SERVERS": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092"),
    "KAFKA_TOPIC": os.getenv("KAFKA_TOPIC", "ecommerce_orders"),
    "POSTGRES_HOST": os.getenv("POSTGRES_HOST", "postgres"),
    "POSTGRES_PORT": os.getenv("POSTGRES_PORT", "5432"),
    "POSTGRES_DB": os.getenv("POSTGRES_DB", "ecommerce"),
    "POSTGRES_USER": os.getenv("POSTGRES_USER", "ecommerce_user"),
    "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD", "ecommerce_password"),
    "SPARK_CHECKPOINT_LOCATION": os.getenv(
        "SPARK_CHECKPOINT_LOCATION",
        "/tmp/spark-checkpoints/ecommerce_orders",
    ),
    "PYSPARK_PYTHON": "python",
    "PYSPARK_DRIVER_PYTHON": "python",
}

# ── DAG Definition ─────────────────────────────────────────────────────────────
# schedule=None means this DAG only runs when manually triggered (no cron schedule).
# catchup=False prevents Airflow from backfilling missed runs since start_date.
with DAG(
    dag_id="ecommerce_streaming_pipeline",
    description="Manually trigger: fetch DummyJSON carts → Kafka → Spark → PostgreSQL",
    start_date=datetime(2026, 1, 1),
    schedule=None,   # manual trigger only — no automatic scheduling
    catchup=False,
    tags=["ecommerce", "kafka", "spark", "postgres"],
) as dag:

    # Task 1: Run the producer script — fetches one batch from the API and publishes to Kafka.
    # If this task fails (e.g. API is down), the DAG stops here and task 2 never runs.
    produce_order_to_kafka = BashOperator(
        task_id="produce_order_to_kafka",
        bash_command="cd /opt/airflow/project && python producer/producer.py",
        env=PIPELINE_ENV,
        append_env=True,  # merge with existing env — do not replace PATH
    )

    # Task 2: Run the Spark job — reads from Kafka, transforms, writes to PostgreSQL.
    # Uses local JARs via --jars instead of downloading from Maven at runtime.
    transform_and_write_to_postgres = BashOperator(
        task_id="transform_and_write_to_postgres",
        bash_command=(
            "cd /opt/airflow/project && "
            f"spark-submit --jars {SPARK_KAFKA_JARS} "
            "spark/streaming_job.py"
        ),
        env=PIPELINE_ENV,
        append_env=True,
    )

    # Task dependency: task 2 only runs if task 1 succeeds
    produce_order_to_kafka >> transform_and_write_to_postgres

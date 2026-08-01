"""Airflow DAG that orchestrates the e-commerce streaming pipeline."""

from datetime import datetime
import os

from airflow import DAG
from airflow.operators.bash import BashOperator

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

PIPELINE_ENV = {
    "API_URL": os.getenv("API_URL", "https://dummyjson.com/carts"),
    "API_VERIFY_SSL": os.getenv("API_VERIFY_SSL", "true"),
    "ALLOW_SAMPLE_FALLBACK": os.getenv("ALLOW_SAMPLE_FALLBACK", "false"),
    "SAMPLE_ORDERS_PATH": os.getenv("SAMPLE_ORDERS_PATH", "data/sample_orders.json"),
    "ORDER_LIMIT": os.getenv("ORDER_LIMIT", "10"),
    "PRODUCER_RUNS": os.getenv("PRODUCER_RUNS", "1"),
    "PRODUCER_SLEEP_SECONDS": os.getenv("PRODUCER_SLEEP_SECONDS", "0"),
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

with DAG(
    dag_id="ecommerce_streaming_pipeline",
    description="Fetch DummyJSON carts, publish them to Kafka, transform them with Spark, and write PostgreSQL.",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["ecommerce", "kafka", "spark", "postgres"],
) as dag:
    produce_order_to_kafka = BashOperator(
        task_id="produce_order_to_kafka",
        bash_command="cd /opt/airflow/project && python producer/producer.py",
        env=PIPELINE_ENV,
        append_env=True,
    )

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

    produce_order_to_kafka >> transform_and_write_to_postgres

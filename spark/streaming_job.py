import os

import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, from_json
from pyspark.sql.types import ArrayType, DoubleType, IntegerType, StringType, StructField, StructType

# ── Configuration ─────────────────────────────────────────────────────────────
# All values injected via environment variables so the same script works
# locally and inside Docker without any code changes.

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
TOPIC_NAME = os.getenv("KAFKA_TOPIC", "ecommerce_orders")

# Checkpoint directory stores the last committed Kafka offset.
# On restart, Spark reads this and continues from where it left off —
# no messages are reprocessed or skipped.
CHECKPOINT_LOCATION = os.getenv("SPARK_CHECKPOINT_LOCATION", "/tmp/spark-checkpoints/ecommerce_orders")

POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "ecommerce"),
    "user": os.getenv("POSTGRES_USER", "ecommerce_user"),
    "password": os.getenv("POSTGRES_PASSWORD", "ecommerce_password"),
}

# Exact column order used in the INSERT statement — must match the table schema
POSTGRES_COLUMNS = [
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "kafka_timestamp",
    "order_id",
    "user_id",
    "order_total",
    "order_discounted_total",
    "total_products",
    "total_quantity",
    "product_id",
    "product_title",
    "product_price",
    "product_quantity",
    "product_total",
    "product_discount_percentage",
    "product_discounted_total",
]

# ── Schema Definition ──────────────────────────────────────────────────────────
# Spark needs an explicit schema to parse the JSON payload from Kafka.
# Without this, it would treat the entire message as a plain string.

PRODUCT_SCHEMA = StructType([
    StructField("id", IntegerType()),
    StructField("title", StringType()),
    StructField("price", DoubleType()),
    StructField("quantity", IntegerType()),
    StructField("total", DoubleType()),
    StructField("discountPercentage", DoubleType()),
    StructField("discountedTotal", DoubleType()),
    StructField("thumbnail", StringType()),
])

# Each order contains a "products" array — this nested schema reflects that structure
ORDER_SCHEMA = StructType([
    StructField("id", IntegerType()),
    StructField("products", ArrayType(PRODUCT_SCHEMA)),  # array of product objects
    StructField("total", DoubleType()),
    StructField("discountedTotal", DoubleType()),
    StructField("userId", IntegerType()),
    StructField("totalProducts", IntegerType()),
    StructField("totalQuantity", IntegerType()),
])


# ── Spark Session ──────────────────────────────────────────────────────────────

def create_spark_session():
    """Create the Spark session.

    shuffle.partitions=1 avoids spawning 200 shuffle partitions for
    a single-node local setup — keeps memory usage low.
    """
    return (
        SparkSession.builder.appName("EcommerceKafkaStreaming")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )


# ── Kafka Reader ───────────────────────────────────────────────────────────────

def read_orders_from_kafka(spark):
    """Create a streaming DataFrame connected to the Kafka topic.

    startingOffsets=latest means Spark only reads NEW messages —
    messages produced before the job started are ignored.
    The Kafka connector handles offset tracking automatically.
    """
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC_NAME)
        .option("startingOffsets", "latest")
        .load()
    )


# ── Transformations ────────────────────────────────────────────────────────────

def transform_orders(kafka_df):
    """Parse and flatten the raw Kafka messages into individual product rows.

    The pipeline has three steps:

    Step 1 — Cast Kafka bytes to string and keep metadata columns
    Step 2 — Parse the JSON string into typed columns using the ORDER_SCHEMA
             from_json returns null for fields that don't match the schema
    Step 3 — explode(order.products): one order with N products becomes N rows
             This is the key transformation — it normalizes nested data into
             a flat relational structure suitable for PostgreSQL
    """
    # Step 1: Kafka delivers messages as raw bytes — cast value to readable string
    readable = kafka_df.select(
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp"),
        col("value").cast("string").alias("message"),
    )

    # Step 2: Parse the JSON message using the defined schema
    # .where(isNotNull) filters out any malformed or non-DummyJSON messages
    parsed = readable.select(
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp").alias("kafka_timestamp"),
        from_json(col("message"), ORDER_SCHEMA).alias("order"),
    ).where(col("order.id").isNotNull())

    # Step 3: Flatten — explode the products array and select all final columns
    return parsed.select(
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("kafka_timestamp"),
        col("order.id").alias("order_id"),
        col("order.userId").alias("user_id"),
        col("order.total").alias("order_total"),
        col("order.discountedTotal").alias("order_discounted_total"),
        col("order.totalProducts").alias("total_products"),
        col("order.totalQuantity").alias("total_quantity"),
        explode(col("order.products")).alias("product"),  # one row per product
    ).select(
        col("kafka_topic"),
        col("kafka_partition"),
        col("kafka_offset"),
        col("kafka_timestamp"),
        col("order_id"),
        col("user_id"),
        col("order_total"),
        col("order_discounted_total"),
        col("total_products"),
        col("total_quantity"),
        col("product.id").alias("product_id"),
        col("product.title").alias("product_title"),
        col("product.price").alias("product_price"),
        col("product.quantity").alias("product_quantity"),
        col("product.total").alias("product_total"),
        col("product.discountPercentage").alias("product_discount_percentage"),
        col("product.discountedTotal").alias("product_discounted_total"),
    )


# ── PostgreSQL Writer ──────────────────────────────────────────────────────────

def write_batch_to_postgres(batch_df, batch_id):
    """Write one Spark micro-batch to PostgreSQL.

    Called by foreachBatch every trigger interval (10 seconds).

    Key design: ON CONFLICT DO NOTHING
    The primary key (kafka_partition, kafka_offset, product_id) is unique
    per message. If Spark restarts and replays offsets, duplicate inserts
    are silently ignored — this gives us exactly-once semantics without
    needing Kafka transactions.

    execute_values is used instead of cursor.execute() because it sends
    all rows in a single round-trip to PostgreSQL — much faster than
    inserting row by row.
    """
    rows = [tuple(row[col] for col in POSTGRES_COLUMNS) for row in batch_df.toLocalIterator()]

    if not rows:
        return  # nothing to write in this batch

    columns_sql = ", ".join(POSTGRES_COLUMNS)
    insert_sql = f"""
        INSERT INTO order_items ({columns_sql})
        VALUES %s
        ON CONFLICT (kafka_partition, kafka_offset, product_id) DO NOTHING
    """

    with psycopg2.connect(**POSTGRES_CONFIG) as conn:
        with conn.cursor() as cur:
            execute_values(cur, insert_sql, rows)

    print(f"Batch {batch_id}: wrote {len(rows)} rows to PostgreSQL")


# ── Entry Point ────────────────────────────────────────────────────────────────

def main():
    """Start the Spark Structured Streaming job and keep it running indefinitely.

    trigger(processingTime="10 seconds"):
        Spark wakes up every 10 seconds, reads all new Kafka messages
        since the last offset, processes them, and writes to PostgreSQL.
        This is different from availableNow (which exits after one pass) —
        here the job stays alive and runs continuously.

    query.awaitTermination():
        Blocks the main thread so the process doesn't exit.
        The streaming query runs in background threads managed by Spark.
    """
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")  # suppress verbose INFO logs

    kafka_df = read_orders_from_kafka(spark)
    transformed = transform_orders(kafka_df)

    query = (
        transformed.writeStream
        .foreachBatch(write_batch_to_postgres)  # custom writer called per micro-batch
        .outputMode("append")                   # only new rows, never update existing ones
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .trigger(processingTime="10 seconds")   # process every 10 seconds continuously
        .start()
    )

    query.awaitTermination()  # keep the process alive


if __name__ == "__main__":
    main()

"""Clean Kafka order messages and write them to PostgreSQL.

Mini-projet 08: Kafka topic -> Spark transformation -> PostgreSQL
"""

import os

import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, from_json
from pyspark.sql.types import ArrayType, DoubleType, IntegerType, StringType, StructField, StructType

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
TOPIC_NAME = os.getenv("KAFKA_TOPIC", "ecommerce_orders")
CHECKPOINT_LOCATION = os.getenv("SPARK_CHECKPOINT_LOCATION", "/tmp/spark-checkpoints/ecommerce_orders")
POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5433")),
    "dbname": os.getenv("POSTGRES_DB", "ecommerce"),
    "user": os.getenv("POSTGRES_USER", "ecommerce_user"),
    "password": os.getenv("POSTGRES_PASSWORD", "ecommerce_password"),
}

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

PRODUCT_SCHEMA = StructType(
    [
        StructField("id", IntegerType()),
        StructField("title", StringType()),
        StructField("price", DoubleType()),
        StructField("quantity", IntegerType()),
        StructField("total", DoubleType()),
        StructField("discountPercentage", DoubleType()),
        StructField("discountedTotal", DoubleType()),
        StructField("thumbnail", StringType()),
    ]
)

ORDER_SCHEMA = StructType(
    [
        StructField("id", IntegerType()),
        StructField("products", ArrayType(PRODUCT_SCHEMA)),
        StructField("total", DoubleType()),
        StructField("discountedTotal", DoubleType()),
        StructField("userId", IntegerType()),
        StructField("totalProducts", IntegerType()),
        StructField("totalQuantity", IntegerType()),
    ]
)


def create_spark_session():
    """Create the Spark session used by the streaming job."""
    return (
        SparkSession.builder.appName("EcommerceKafkaStreaming")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )


def read_orders_from_kafka(spark):
    """Create a streaming DataFrame that reads raw messages from Kafka."""
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC_NAME)
        .option("startingOffsets", "earliest")
        .load()
    )


def select_readable_columns(kafka_dataframe):
    """Convert Kafka bytes into readable string columns."""
    return kafka_dataframe.select(
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp"),
        col("value").cast("string").alias("message"),
    )


def transform_orders(readable_orders):
    """Parse JSON orders and create one clean row per product."""
    parsed_orders = readable_orders.select(
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp").alias("kafka_timestamp"),
        from_json(col("message"), ORDER_SCHEMA).alias("order"),
    )

    valid_orders = parsed_orders.where(col("order.id").isNotNull())

    return valid_orders.select(
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
        explode(col("order.products")).alias("product"),
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


def write_batch_to_postgres(batch_dataframe, batch_id):
    """Insert one Spark micro-batch into PostgreSQL."""
    rows = [tuple(row[column] for column in POSTGRES_COLUMNS) for row in batch_dataframe.toLocalIterator()]

    if not rows:
        print(f"Batch {batch_id}: no rows to write")
        return

    columns_sql = ", ".join(POSTGRES_COLUMNS)
    insert_sql = f"""
        INSERT INTO order_items ({columns_sql})
        VALUES %s
        ON CONFLICT (kafka_partition, kafka_offset, product_id) DO NOTHING
    """

    with psycopg2.connect(**POSTGRES_CONFIG) as connection:
        with connection.cursor() as cursor:
            execute_values(cursor, insert_sql, rows)

    print(f"Batch {batch_id}: wrote {len(rows)} rows to PostgreSQL")


def main():
    """Start the Spark streaming query and write transformed orders to PostgreSQL."""
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    kafka_dataframe = read_orders_from_kafka(spark)
    readable_orders = select_readable_columns(kafka_dataframe)
    transformed_orders = transform_orders(readable_orders)

    query = (
        transformed_orders.writeStream.foreachBatch(write_batch_to_postgres)
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .trigger(availableNow=True)
        .start()
    )

    query.awaitTermination()
    spark.stop()


if __name__ == "__main__":
    main()

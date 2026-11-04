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
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
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

ORDER_SCHEMA = StructType([
    StructField("id", IntegerType()),
    StructField("products", ArrayType(PRODUCT_SCHEMA)),
    StructField("total", DoubleType()),
    StructField("discountedTotal", DoubleType()),
    StructField("userId", IntegerType()),
    StructField("totalProducts", IntegerType()),
    StructField("totalQuantity", IntegerType()),
])


def create_spark_session():
    return (
        SparkSession.builder.appName("EcommerceKafkaStreaming")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )


def read_orders_from_kafka(spark):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC_NAME)
        .option("startingOffsets", "latest")
        .load()
    )


def transform_orders(kafka_df):
    readable = kafka_df.select(
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp"),
        col("value").cast("string").alias("message"),
    )

    parsed = readable.select(
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp").alias("kafka_timestamp"),
        from_json(col("message"), ORDER_SCHEMA).alias("order"),
    ).where(col("order.id").isNotNull())

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


def write_batch_to_postgres(batch_df, batch_id):
    rows = [tuple(row[col] for col in POSTGRES_COLUMNS) for row in batch_df.toLocalIterator()]

    if not rows:
        return

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


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    kafka_df = read_orders_from_kafka(spark)
    transformed = transform_orders(kafka_df)

    query = (
        transformed.writeStream
        .foreachBatch(write_batch_to_postgres)
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .trigger(processingTime="10 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()

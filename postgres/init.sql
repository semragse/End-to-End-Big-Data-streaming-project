-- This script runs automatically when the PostgreSQL container starts for the first time.
-- It creates the table that Spark writes transformed order data into.

CREATE TABLE IF NOT EXISTS order_items (
    -- Kafka metadata columns: track exactly where this row came from in the topic.
    -- These are critical for idempotency — they form part of the primary key.
    kafka_topic              VARCHAR(100)   NOT NULL,
    kafka_partition          INTEGER        NOT NULL,
    kafka_offset             BIGINT         NOT NULL,  -- unique message position within a partition
    kafka_timestamp          TIMESTAMP      NOT NULL,

    -- Order-level columns: one order = one Kafka message = multiple rows (one per product)
    order_id                 INTEGER        NOT NULL,
    user_id                  INTEGER        NOT NULL,
    order_total              NUMERIC(12,2)  NOT NULL,
    order_discounted_total   NUMERIC(12,2)  NOT NULL,
    total_products           INTEGER        NOT NULL,
    total_quantity           INTEGER        NOT NULL,

    -- Product-level columns: each row represents one product within the order
    product_id               INTEGER        NOT NULL,
    product_title            TEXT           NOT NULL,
    product_price            NUMERIC(12,2)  NOT NULL,
    product_quantity         INTEGER        NOT NULL,
    product_total            NUMERIC(12,2)  NOT NULL,
    product_discount_percentage NUMERIC(5,2) NOT NULL,
    product_discounted_total NUMERIC(12,2)  NOT NULL,

    -- Automatically set to the current time when Spark inserts the row
    inserted_at              TIMESTAMP      DEFAULT CURRENT_TIMESTAMP,

    -- Composite primary key — guarantees idempotency.
    -- (partition, offset) uniquely identifies a Kafka message.
    -- Adding product_id handles the case where one message has multiple products.
    -- If Spark restarts and replays the same offsets, ON CONFLICT DO NOTHING
    -- silently skips duplicates — no data is corrupted or doubled.
    PRIMARY KEY (kafka_partition, kafka_offset, product_id)
);

# Architecture

## Pipeline Overview

```
DummyJSON API
    → Python Producer (infinite loop)
    → Apache Kafka 4.0 — topic: ecommerce_orders
    → Spark Structured Streaming (continuous)
    → PostgreSQL 16 — table: order_items
    → Grafana 11.1 — live KPI dashboard
```

Airflow is available for manual pipeline runs and ad-hoc triggering, but the core pipeline runs continuously as long-lived Docker services.

---

## Services

### Producer (`producer/`)

Fetches e-commerce cart data from the DummyJSON REST API every `PRODUCER_SLEEP_SECONDS` seconds and publishes each cart as a JSON message to the Kafka topic.

- Uses `confluent-kafka` (C-backed, production-grade)
- Runs as a Docker service with `restart: unless-stopped`
- Retries on API or Kafka errors without crashing

### Kafka (Apache Kafka 4.0 KRaft)

Single-broker cluster in KRaft mode — no ZooKeeper required. Two listeners:
- `INTERNAL://kafka:19092` — used by containers
- `EXTERNAL://localhost:19092` — used by local tooling

### Spark Structured Streaming (`spark/`)

Reads from Kafka continuously (`trigger(processingTime="10 seconds")`), parses nested JSON, explodes the product array into individual rows, and writes to PostgreSQL via `foreachBatch`.

Key transformations:
- `from_json(ORDER_SCHEMA)` — deserializes Kafka messages into typed columns
- `explode(order.products)` — one order → N rows (one per product)
- `ON CONFLICT DO NOTHING` — idempotent writes keyed on `(kafka_partition, kafka_offset, product_id)`
- Checkpoint directory persists Kafka offsets across restarts

### PostgreSQL 16

Stores one row per product per order. Schema in `postgres/init.sql`.

Primary key: `(kafka_partition, kafka_offset, product_id)` — guarantees exactly-once semantics without Kafka transactions.

### Grafana 11.1

Auto-provisioned at startup from config files in `dashboard/grafana/provisioning/`. No manual setup required. Connects directly to PostgreSQL.

### Airflow 2.9.3

Custom image (`airflow/Dockerfile`) with Java 17 + PySpark baked in. Used for manual pipeline triggering and monitoring. The DAG `ecommerce_streaming_pipeline` runs the producer then the Spark job sequentially.

---

## Startup Order

```
kafka + postgres
    → adminer, grafana, producer, spark (after kafka/postgres healthy)
    → airflow-init (db migrate + user create)
    → airflow-webserver
    → airflow-scheduler
```

---

## Key Design Decisions

**KRaft mode** — Kafka 4.0 dropped ZooKeeper. Simpler single-container setup.

**`trigger(processingTime="10 seconds")`** — Spark processes new Kafka messages every 10 seconds and stays running, unlike `availableNow` which exits after one pass.

**Composite primary key as idempotency key** — Re-running or restarting Spark never creates duplicate rows.

**Local Spark Kafka JARs** — Copied from `jars/` into the Spark container image at build time, avoiding Maven downloads at runtime.

**`restart: unless-stopped`** — Producer and Spark recover automatically from transient failures.

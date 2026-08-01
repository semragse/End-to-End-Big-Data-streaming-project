# Architecture Details

## Pipeline Layers

### 1. Ingestion Layer — Python Producer

**File:** `producer/producer.py`

The producer fetches configurable batches of e-commerce cart data from the [DummyJSON](https://dummyjson.com) public REST API.
Each cart contains nested product objects. The producer serializes each full cart as a single JSON message
and publishes it to the Kafka topic configured by `KAFKA_TOPIC`.

```
DummyJSON /carts?limit=10
    → fetch_orders()
    → send_orders_to_kafka()
    → Kafka topic: ecommerce_orders
```

Key design choices:
- Uses `confluent-kafka` (high-performance C-backed client) instead of `kafka-python` (pure Python, not maintained)
- Uses normal TLS certificate verification for API calls
- Supports repeated runs through `PRODUCER_RUNS` and `PRODUCER_SLEEP_SECONDS` for local stream simulation
- `producer.flush()` ensures delivery before exit — important when run from Airflow
- Exits with `sys.exit(1)` on API failure so Airflow marks the task as FAILED and stops the DAG

---

### 2. Messaging Layer — Apache Kafka

**Image:** `apache/kafka:4.0.0`

Kafka 4.0 runs in **KRaft mode** (no ZooKeeper). A single broker handles both the broker and controller roles.

```
KAFKA_PROCESS_ROLES: broker,controller
KAFKA_LISTENERS: INTERNAL://:19092, EXTERNAL://:29092, CONTROLLER://:9093
KAFKA_ADVERTISED_LISTENERS:
    INTERNAL://kafka:19092   ← used by Spark/Airflow containers
    EXTERNAL://localhost:19092  ← used by local Python scripts
```

Why two listeners?
- Containers use the internal Docker/Podman network (`kafka:19092`)
- Local scripts on the host use `localhost:19092`

---

### 3. Processing Layer — Spark Structured Streaming

**File:** `spark/streaming_job.py`

Spark reads all available Kafka messages, transforms nested JSON, and writes to PostgreSQL.

```
Kafka topic
    → readStream.format("kafka")
    → cast value to string
    → from_json(ORDER_SCHEMA)
    → explode(order.products)
    → foreachBatch(write_batch_to_postgres)
```

Key transformations:
1. `from_json` parses the raw Kafka `value` string into a structured `StructType`
2. `explode(order.products)` turns one order row into N product rows (one per product)
3. `where(col("order.id").isNotNull())` filters non-DummyJSON messages
4. `checkpointLocation` stores progress metadata for scheduled Spark runs
5. `trigger(availableNow=True)` processes all current messages and exits (not a continuous stream)

---

### 4. Storage Layer — PostgreSQL

**Image:** `postgres:16`

The `order_items` table stores one row per product per Kafka message.

Primary key: `(kafka_partition, kafka_offset, product_id)`

This composite key guarantees **idempotency**:
- Same Kafka offset + same product → duplicate is silently ignored
- Safe to re-run the Spark job multiple times without creating duplicates

---

### 5. Visualization Layer — Grafana

**Image:** `grafana/grafana:11.1.0`

Grafana is **fully auto-provisioned** at startup via volume mounts:

```
dashboard/grafana/provisioning/datasources/postgres.yml   → declares PostgreSQL datasource
dashboard/grafana/provisioning/dashboards/dashboards.yml  → tells Grafana where to load JSON
dashboard/grafana/dashboards/ecommerce-streaming.json     → the dashboard definition
```

No manual setup needed — the dashboard is available immediately after `podman-compose up`.

---

### 6. Orchestration Layer — Apache Airflow

**Image:** Custom `ecommerce-airflow:latest` (see `airflow/Dockerfile`)

The custom image extends `apache/airflow:2.9.3-python3.11` with:
- `openjdk-17-jre-headless` — required by PySpark / `spark-submit`
- All Python dependencies from `requirements.txt` (PySpark, psycopg2, confluent-kafka, requests)

The DAG `ecommerce_streaming_pipeline` runs two tasks:

```
produce_order_to_kafka
    BashOperator: python producer/producer.py
    env: KAFKA_BOOTSTRAP_SERVERS=kafka:19092

    ↓ (only if success)

transform_and_write_to_postgres
    BashOperator: spark-submit --packages ... spark/streaming_job.py
    env: KAFKA_BOOTSTRAP_SERVERS=kafka:19092, POSTGRES_HOST=postgres
```

`append_env=True` is required on both operators so that the container PATH
(including `/home/airflow/.local/bin` where `spark-submit` lives) is preserved.

---

### 7. Infrastructure — Podman Compose

All services are defined in `docker-compose.yml` (compatible with both Docker and Podman Compose).

Startup order:
```
kafka + postgres (start immediately)
    → adminer, grafana (depend on postgres)
    → airflow-init (depends on kafka + postgres, runs db migrate + user create)
    → airflow-webserver (depends on airflow-init)
    → airflow-scheduler (depends on airflow-webserver)
```

The `airflow-init` service solves the SQLite concurrent-migration race condition:
it runs `airflow db migrate` once before webserver and scheduler start,
preventing both from trying to initialize the metadata DB simultaneously.

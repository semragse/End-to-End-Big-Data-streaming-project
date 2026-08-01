.PHONY: up down build airflow-init trigger logs-spark logs-scheduler psql count clean restart-airflow test validate

# ─── Infrastructure ───────────────────────────────────────────────────────────

up:
	podman-compose up -d

down:
	podman-compose down

build:
	podman build -t ecommerce-airflow:latest ./airflow

build-no-cache:
	podman build --no-cache -t ecommerce-airflow:latest ./airflow

restart:
	podman-compose down && podman-compose up -d

# ─── Airflow ──────────────────────────────────────────────────────────────────

airflow-init:
	podman-compose up airflow-init

restart-airflow:
	podman rm -f ecommerce-airflow-webserver ecommerce-airflow-scheduler || true
	podman-compose up -d airflow-webserver airflow-scheduler

trigger:
	podman exec ecommerce-airflow-webserver airflow dags trigger ecommerce_streaming_pipeline

dag-status:
	podman exec ecommerce-airflow-webserver airflow dags list-runs -d ecommerce_streaming_pipeline

dag-errors:
	podman exec ecommerce-airflow-webserver airflow dags list-import-errors

# ─── Logs ─────────────────────────────────────────────────────────────────────

logs-webserver:
	podman logs ecommerce-airflow-webserver --tail 80

logs-scheduler:
	podman logs ecommerce-airflow-scheduler --tail 80

logs-kafka:
	podman logs ecommerce-kafka --tail 50

logs-spark:
	podman exec ecommerce-airflow-webserver bash -c \
		"find /opt/airflow/logs -name '*.log' | sort | tail -1 | xargs tail -80"

# ─── Kafka ────────────────────────────────────────────────────────────────────

kafka-topics:
	podman exec ecommerce-kafka /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server localhost:19092 --list

kafka-describe:
	podman exec ecommerce-kafka /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server localhost:19092 --describe --topic ecommerce_orders

# ─── PostgreSQL ───────────────────────────────────────────────────────────────

psql:
	podman exec -it ecommerce-postgres psql -U ecommerce_user -d ecommerce

count:
	podman exec ecommerce-postgres psql -U ecommerce_user -d ecommerce \
		-c "SELECT COUNT(*) AS total_order_items FROM order_items;"

query:
	podman exec ecommerce-postgres psql -U ecommerce_user -d ecommerce \
		-c "SELECT order_id, product_title, product_quantity, product_total FROM order_items ORDER BY inserted_at DESC LIMIT 10;"

validate:
	python scripts/validate_pipeline.py

test:
	python -m unittest discover -s tests

# ─── Producer (local) ─────────────────────────────────────────────────────────

produce:
	python producer/producer.py

# ─── Cleanup ──────────────────────────────────────────────────────────────────

clean:
	podman-compose down -v

status:
	podman ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

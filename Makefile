.PHONY: up down build restart logs-producer logs-spark logs-kafka psql count clean status airflow-init trigger

# ─── Infrastructure ───────────────────────────────────────────────────────────

up:
	docker compose up -d

down:
	docker compose down

build:
	docker compose build

build-no-cache:
	docker compose build --no-cache

restart:
	docker compose down && docker compose up -d

# ─── Logs ─────────────────────────────────────────────────────────────────────

logs-producer:
	docker logs ecommerce-producer -f

logs-spark:
	docker logs ecommerce-spark -f

logs-kafka:
	docker logs ecommerce-kafka --tail 50

# ─── Kafka ────────────────────────────────────────────────────────────────────

kafka-topics:
	docker exec ecommerce-kafka /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server localhost:19092 --list

kafka-describe:
	docker exec ecommerce-kafka /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server localhost:19092 --describe --topic ecommerce_orders

# ─── PostgreSQL ───────────────────────────────────────────────────────────────

psql:
	docker exec -it ecommerce-postgres psql -U ecommerce_user -d ecommerce

count:
	docker exec ecommerce-postgres psql -U ecommerce_user -d ecommerce \
		-c "SELECT COUNT(*) AS total_order_items FROM order_items;"

query:
	docker exec ecommerce-postgres psql -U ecommerce_user -d ecommerce \
		-c "SELECT order_id, product_title, product_quantity, product_total FROM order_items ORDER BY inserted_at DESC LIMIT 10;"

# ─── Airflow (manual trigger) ─────────────────────────────────────────────────

airflow-init:
	docker compose up airflow-init

trigger:
	docker exec ecommerce-airflow-webserver airflow dags trigger ecommerce_streaming_pipeline

# ─── Cleanup ──────────────────────────────────────────────────────────────────

clean:
	docker compose down -v

status:
	docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

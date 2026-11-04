import json
import logging
import os
import sys
import time

import requests
from confluent_kafka import Producer

# Structured log format: timestamp, component name, level, message
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [producer] %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)

# All configuration comes from environment variables — no hardcoded values.
# This makes the same script work locally and inside Docker without code changes.
API_URL = os.getenv("API_URL", "https://dummyjson.com/carts")
API_VERIFY_SSL = os.getenv("API_VERIFY_SSL", "true").lower() not in {"0", "false", "no"}
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
TOPIC_NAME = os.getenv("KAFKA_TOPIC", "ecommerce_orders")
ORDER_LIMIT = int(os.getenv("ORDER_LIMIT", "10"))
# How long to wait between fetch cycles — controls the ingestion rate
SLEEP_SECONDS = float(os.getenv("PRODUCER_SLEEP_SECONDS", "5"))


def fetch_orders():
    """Fetch a batch of e-commerce carts from the DummyJSON REST API.

    DummyJSON returns a JSON object with a "carts" array.
    We pass ?limit=N to control how many orders we fetch per run.
    """
    params = {"limit": ORDER_LIMIT} if API_URL.rstrip("/").endswith("carts") else None
    response = requests.get(API_URL, params=params, timeout=10, verify=API_VERIFY_SSL)
    response.raise_for_status()  # raises an exception on 4xx/5xx responses
    data = response.json()
    return data.get("carts", [data])


def delivery_report(error, message):
    """Callback invoked by Kafka after each message is acknowledged by the broker.

    Kafka delivery is asynchronous — this callback is how we know
    whether a message was actually stored or dropped.
    """
    if error is not None:
        log.error("Delivery failed: %s", error)
    else:
        log.info(
            "Delivered → topic=%s partition=%d offset=%d",
            message.topic(),
            message.partition(),
            message.offset(),
        )


def send_orders_to_kafka(producer, orders, run_number):
    """Serialize each order as JSON and publish it to the Kafka topic.

    - message key: "{order_id}-{run_number}" — used by Kafka for partition routing
    - message value: full order JSON — consumed downstream by Spark
    - producer.poll(0): triggers delivery callbacks without blocking
    - producer.flush(): waits until all in-flight messages are acknowledged
    """
    for order in orders:
        producer.produce(
            TOPIC_NAME,
            key=f"{order['id']}-{run_number}",
            value=json.dumps(order),
            callback=delivery_report,
        )
        producer.poll(0)
    producer.flush()


def main():
    """Entry point: infinite loop that continuously fetches and publishes orders.

    Design decisions:
    - Uses confluent-kafka (C-backed library) instead of kafka-python (unmaintained)
    - Catches exceptions and retries instead of crashing — keeps the container alive
    - SLEEP_SECONDS controls the ingestion rate (default: every 5 seconds)
    """
    # Create one producer instance and reuse it across all runs
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
    run_number = 1

    log.info("Starting continuous producer → topic=%s sleep=%ss", TOPIC_NAME, SLEEP_SECONDS)

    while True:
        try:
            orders = fetch_orders()
            log.info("Run %d: fetched %d orders from API", run_number, len(orders))
            send_orders_to_kafka(producer, orders, run_number)
            run_number += 1
        except requests.exceptions.RequestException as exc:
            # API is temporarily unavailable — log and retry on next cycle
            log.error("API request failed: %s — retrying in %ss", exc, SLEEP_SECONDS)
        except Exception as exc:
            # Catch-all to prevent the container from crashing on unexpected errors
            log.error("Unexpected error: %s — retrying in %ss", exc, SLEEP_SECONDS)

        time.sleep(SLEEP_SECONDS)


if __name__ == "__main__":
    main()

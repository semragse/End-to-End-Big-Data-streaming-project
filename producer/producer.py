import json
import logging
import os
import sys
import time

import requests
from confluent_kafka import Producer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [producer] %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)

API_URL = os.getenv("API_URL", "https://dummyjson.com/carts")
API_VERIFY_SSL = os.getenv("API_VERIFY_SSL", "true").lower() not in {"0", "false", "no"}
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
TOPIC_NAME = os.getenv("KAFKA_TOPIC", "ecommerce_orders")
ORDER_LIMIT = int(os.getenv("ORDER_LIMIT", "10"))
SLEEP_SECONDS = float(os.getenv("PRODUCER_SLEEP_SECONDS", "5"))


def fetch_orders():
    params = {"limit": ORDER_LIMIT} if API_URL.rstrip("/").endswith("carts") else None
    response = requests.get(API_URL, params=params, timeout=10, verify=API_VERIFY_SSL)
    response.raise_for_status()
    data = response.json()
    return data.get("carts", [data])


def delivery_report(error, message):
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
            log.error("API request failed: %s — retrying in %ss", exc, SLEEP_SECONDS)
        except Exception as exc:
            log.error("Unexpected error: %s — retrying in %ss", exc, SLEEP_SECONDS)

        time.sleep(SLEEP_SECONDS)


if __name__ == "__main__":
    main()

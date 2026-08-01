"""Fetch e-commerce carts from DummyJSON and send them to Kafka.

Mini-projet: API -> Python producer -> Kafka topic
"""

import json
import os
import sys
import time

from confluent_kafka import Producer
import requests

API_URL = os.getenv("API_URL", "https://dummyjson.com/carts")
API_VERIFY_SSL = os.getenv("API_VERIFY_SSL", "true").lower() not in {"0", "false", "no"}
ALLOW_SAMPLE_FALLBACK = os.getenv("ALLOW_SAMPLE_FALLBACK", "false").lower() in {"1", "true", "yes"}
SAMPLE_ORDERS_PATH = os.getenv("SAMPLE_ORDERS_PATH", "data/sample_orders.json")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
TOPIC_NAME = os.getenv("KAFKA_TOPIC", "ecommerce_orders")
ORDER_LIMIT = int(os.getenv("ORDER_LIMIT", "10"))
PRODUCER_RUNS = int(os.getenv("PRODUCER_RUNS", "1"))
PRODUCER_SLEEP_SECONDS = float(os.getenv("PRODUCER_SLEEP_SECONDS", "0"))


def fetch_orders():
    """Call DummyJSON and return carts used as fake orders."""
    params = {"limit": ORDER_LIMIT} if API_URL.rstrip("/").endswith("carts") else None
    try:
        response = requests.get(API_URL, params=params, timeout=10, verify=API_VERIFY_SSL)
        response.raise_for_status()
    except requests.exceptions.RequestException:
        if not ALLOW_SAMPLE_FALLBACK:
            raise

        with open(SAMPLE_ORDERS_PATH, encoding="utf-8") as sample_file:
            return json.load(sample_file)

    data = response.json()
    return data.get("carts", [data])


def display_orders(orders):
    """Display a readable summary of each order in the terminal."""
    for order in orders:
        print("=" * 60)
        print(f"Order ID      : {order['id']}")
        print(f"User ID       : {order['userId']}")
        print(f"Total products: {order['totalProducts']}")
        print(f"Total quantity: {order['totalQuantity']}")
        print(f"Total amount  : ${order['total']}")
        print("Products:")

        for product in order["products"]:
            print(
                f"  - {product['title']} | "
                f"qty={product['quantity']} | "
                f"price=${product['price']}"
            )


def create_kafka_producer():
    """Create a Kafka producer connected to the local Kafka broker."""
    return Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})


def delivery_report(error, message):
    """Display whether Kafka accepted the produced message."""
    if error is not None:
        print(f"Kafka delivery failed: {error}")
        return

    print(
        "Kafka delivery succeeded: "
        f"topic={message.topic()} partition={message.partition()} offset={message.offset()}"
    )


def send_orders_to_kafka(orders, run_number=1):
    """Send each order as one JSON message to Kafka."""
    producer = create_kafka_producer()

    for order in orders:
        message_value = json.dumps(order)
        message_key = f"{order['id']}-{run_number}"

        producer.produce(
            TOPIC_NAME,
            key=message_key,
            value=message_value,
            callback=delivery_report,
        )
        producer.poll(0)

    producer.flush()


def main():
    """Main entry point of the script."""
    try:
        run_number = 1
        while PRODUCER_RUNS == 0 or run_number <= PRODUCER_RUNS:
            orders = fetch_orders()
            display_orders(orders)
            send_orders_to_kafka(orders, run_number)

            run_number += 1
            if PRODUCER_RUNS == 0 or run_number <= PRODUCER_RUNS:
                time.sleep(PRODUCER_SLEEP_SECONDS)
    except requests.exceptions.RequestException as error:
        print(f"API request failed: {error}")
        sys.exit(1)


if __name__ == "__main__":
    main()

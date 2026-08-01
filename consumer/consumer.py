"""Read e-commerce orders from Kafka and display them.

Mini-projet 04: Kafka topic -> Python consumer -> terminal
"""

import json
import os
import time

from confluent_kafka import Consumer, KafkaException

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
TOPIC_NAME = os.getenv("KAFKA_TOPIC", "ecommerce_orders")


def create_consumer():
    """Create a Kafka consumer connected to the ecommerce_orders topic."""
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": "ecommerce-python-consumer",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([TOPIC_NAME])
    return consumer


def display_message(message):
    """Display one Kafka message in a readable way."""
    order = json.loads(message.value().decode("utf-8"))

    print("=" * 60)
    print(f"Topic    : {message.topic()}")
    print(f"Partition: {message.partition()}")
    print(f"Offset   : {message.offset()}")
    print("Order:")
    print(json.dumps(order, indent=2))


def main():
    """Read messages from Kafka until the timeout is reached."""
    consumer = create_consumer()

    print(f"Reading messages from Kafka topic: {TOPIC_NAME}")
    print("Waiting for messages...")

    message_count = 0
    end_time = time.monotonic() + 10

    try:
        while time.monotonic() < end_time:
            message = consumer.poll(1.0)

            if message is None:
                continue

            if message.error():
                raise KafkaException(message.error())

            display_message(message)
            message_count += 1
    finally:
        consumer.close()

    print(f"Finished. Messages read: {message_count}")


if __name__ == "__main__":
    main()

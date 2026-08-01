"""Validate that the streaming pipeline wrote usable data to PostgreSQL."""

import os
import sys

import psycopg2

POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5433")),
    "dbname": os.getenv("POSTGRES_DB", "ecommerce"),
    "user": os.getenv("POSTGRES_USER", "ecommerce_user"),
    "password": os.getenv("POSTGRES_PASSWORD", "ecommerce_password"),
}
MIN_ORDER_ITEMS = int(os.getenv("MIN_ORDER_ITEMS", "1"))


def main():
    """Exit with a non-zero status when the pipeline output is missing."""
    query = """
        SELECT
            COUNT(*) AS order_items,
            COUNT(DISTINCT order_id) AS orders,
            COALESCE(SUM(product_total), 0) AS gross_sales
        FROM order_items;
    """

    with psycopg2.connect(**POSTGRES_CONFIG) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            order_items, orders, gross_sales = cursor.fetchone()

    print(f"order_items={order_items}")
    print(f"orders={orders}")
    print(f"gross_sales={gross_sales}")

    if order_items < MIN_ORDER_ITEMS:
        print(
            f"Validation failed: expected at least {MIN_ORDER_ITEMS} order item(s), "
            f"found {order_items}.",
            file=sys.stderr,
        )
        return 1

    print("Validation passed: PostgreSQL contains transformed order items.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
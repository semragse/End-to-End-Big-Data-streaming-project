import sys
import unittest
from unittest.mock import Mock, mock_open, patch

sys.modules.setdefault("confluent_kafka", Mock(Producer=Mock()))

from producer import producer


class FetchOrdersTests(unittest.TestCase):
    def test_fetch_orders_returns_carts_from_collection_endpoint(self):
        response = Mock()
        response.json.return_value = {"carts": [{"id": 1}, {"id": 2}]}
        response.raise_for_status.return_value = None

        with patch("producer.producer.requests.get", return_value=response) as requests_get:
            with patch.object(producer, "API_URL", "https://dummyjson.com/carts"):
                with patch.object(producer, "ORDER_LIMIT", 2):
                    orders = producer.fetch_orders()

        self.assertEqual(orders, [{"id": 1}, {"id": 2}])
        requests_get.assert_called_once_with(
            "https://dummyjson.com/carts",
            params={"limit": 2},
            timeout=10,
            verify=True,
        )

    def test_fetch_orders_wraps_single_cart_response(self):
        response = Mock()
        response.json.return_value = {"id": 1, "products": []}
        response.raise_for_status.return_value = None

        with patch("producer.producer.requests.get", return_value=response) as requests_get:
            with patch.object(producer, "API_URL", "https://dummyjson.com/carts/1"):
                orders = producer.fetch_orders()

        self.assertEqual(orders, [{"id": 1, "products": []}])
        requests_get.assert_called_once_with(
            "https://dummyjson.com/carts/1",
            params=None,
            timeout=10,
            verify=True,
        )

    def test_fetch_orders_uses_sample_fallback_when_enabled(self):
        sample_orders = '[{"id": 101}]'

        with patch(
            "producer.producer.requests.get",
            side_effect=producer.requests.exceptions.HTTPError("API unavailable"),
        ):
            with patch.object(producer, "ALLOW_SAMPLE_FALLBACK", True):
                with patch("builtins.open", mock_open(read_data=sample_orders)) as sample_file:
                    orders = producer.fetch_orders()

        self.assertEqual(orders, [{"id": 101}])
        sample_file.assert_called_once_with(producer.SAMPLE_ORDERS_PATH, encoding="utf-8")


if __name__ == "__main__":
    unittest.main()
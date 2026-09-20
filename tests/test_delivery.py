import threading
import unittest
from unittest.mock import Mock

from orders import decode_order, validate_order
from producer import publish_order
from tracker import consume

ORDER = {"order_id": "deab7f68-371d-48bc-adb3-486d376e66ce",
         "user": "marvin", "item": "burger", "quantity": 1}


class DeliveryTests(unittest.TestCase):
    def test_invalid_orders(self):
        for value in (None, b'not json', b'[]', b'\xff'):
            with self.subTest(value=value), self.assertRaises(ValueError):
                decode_order(value)
        for quantity in (True, 0, -1, 1.5, "1"):
            with self.subTest(quantity=quantity), self.assertRaises(ValueError):
                validate_order({**ORDER, "quantity": quantity})

    def test_publish_requires_successful_callback(self):
        for outcome in ("success", "failure", "timeout"):
            with self.subTest(outcome=outcome):
                producer = Mock()
                def flush(timeout):
                    if outcome != "timeout":
                        callback = producer.produce.call_args.kwargs["on_delivery"]
                        callback(None if outcome == "success" else "failed", Mock())
                    return int(outcome == "timeout")
                producer.flush.side_effect = flush
                if outcome == "success":
                    publish_order(producer, "orders", ORDER)
                else:
                    with self.assertRaises(RuntimeError):
                        publish_order(producer, "orders", ORDER)
                self.assertEqual(producer.produce.call_args.kwargs["key"], ORDER["order_id"].encode())

    def test_queue_full_is_failure_and_flushes(self):
        producer = Mock()
        producer.produce.side_effect = BufferError("full")
        producer.flush.return_value = 0
        with self.assertRaises(BufferError):
            publish_order(producer, "orders", ORDER)
        producer.flush.assert_called_once()

    def test_commit_only_after_successful_handling(self):
        import json
        for failure in (None, "decode", "handler", "commit"):
            with self.subTest(failure=failure):
                stop = threading.Event()
                msg = Mock()
                msg.error.return_value = None
                msg.value.return_value = b'bad' if failure == "decode" else json.dumps(ORDER).encode()
                consumer = Mock()
                consumer.poll.return_value = msg
                consumer.commit.return_value = []
                if failure == "commit":
                    consumer.commit.return_value = [Mock(error="commit failed")]
                def handler(order):
                    consumer.commit.assert_not_called()
                    stop.set()
                    if failure == "handler":
                        raise RuntimeError("business operation failed")
                if failure:
                    with self.assertRaises(Exception):
                        consume(consumer, "orders", stop, handler)
                else:
                    consume(consumer, "orders", stop, handler)
                if failure in ("decode", "handler"):
                    consumer.commit.assert_not_called()
                else:
                    consumer.commit.assert_called_once_with(message=msg, asynchronous=False)
                consumer.close.assert_called_once()

    def test_stop_closes_without_polling(self):
        stop = threading.Event()
        stop.set()
        consumer = Mock()
        consume(consumer, "orders", stop)
        consumer.poll.assert_not_called()
        consumer.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()

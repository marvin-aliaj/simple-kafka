"""Publish one order and return a nonzero status unless delivery is confirmed."""
import argparse
import json
import logging
import signal
import uuid

from confluent_kafka import Producer

from config import configure_logging, kafka_config, orders_topic
from orders import validate_order

LOG = logging.getLogger(__name__)
DELIVERY_TIMEOUT_SECONDS = 10


def publish_order(producer, topic, order):
    validate_order(order)
    delivered = False

    def delivery_report(err, msg):
        nonlocal delivered
        if err is not None:
            LOG.error("Delivery failed order_id=%s error=%s", order["order_id"], err)
        else:
            delivered = True
            LOG.info("Delivered order_id=%s topic=%s partition=%s offset=%s",
                     order["order_id"], msg.topic(), msg.partition(), msg.offset())

    try:
        producer.produce(
            topic=topic,
            key=order["order_id"].encode("utf-8"),
            value=json.dumps(order).encode("utf-8"),
            on_delivery=delivery_report,
        )
    finally:
        remaining = producer.flush(DELIVERY_TIMEOUT_SECONDS)
        if remaining:
            LOG.error("Delivery unconfirmed: %s message(s) still queued", remaining)
    if not delivered:
        raise RuntimeError("Order delivery was not confirmed")


def main():
    configure_logging()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--user", default="marvin")
    parser.add_argument("--item", default="burger")
    parser.add_argument("--quantity", type=int, default=1)
    parser.add_argument("--order-id", default=None, help="Stable UUID for application deduplication")
    args = parser.parse_args()

    def stop(signum, frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, stop)
    try:
        order = validate_order({
            "order_id": args.order_id or str(uuid.uuid4()),
            "user": args.user, "item": args.item, "quantity": args.quantity,
        })
        producer = Producer({
            **kafka_config("order-producer"),
            "enable.idempotence": True,
            "acks": "all",
            "message.timeout.ms": 5000,
        })
        publish_order(producer, orders_topic(), order)
        return 0
    except KeyboardInterrupt:
        LOG.warning("Producer interrupted; delivery may be uncertain")
        return 130
    except Exception:
        LOG.exception("Unable to publish order")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

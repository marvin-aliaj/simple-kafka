"""Consume orders with explicit, synchronous commits after successful handling."""
import logging
import os
import signal
import threading

from confluent_kafka import Consumer, KafkaException

from config import configure_logging, kafka_config, orders_topic
from orders import decode_order

LOG = logging.getLogger(__name__)


def handle_order(order):
    # Replace with durable business work. Deduplicate by order_id in that store.
    LOG.info("Processed order_id=%s item=%s quantity=%s",
             order["order_id"], order["item"], order["quantity"])


def consume(consumer, topic, stop, handler=handle_order):
    try:
        consumer.subscribe([topic])
        LOG.info("Tracker running topic=%s", topic)
        while not stop.is_set():
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            try:
                order = decode_order(msg.value())
                handler(order)
                partitions = consumer.commit(message=msg, asynchronous=False)
                for partition in partitions or []:
                    if partition.error is not None:
                        raise KafkaException(partition.error)
            except Exception:
                LOG.exception("Processing failed topic=%s partition=%s offset=%s; stopping",
                              msg.topic(), msg.partition(), msg.offset())
                raise
    finally:
        consumer.close()
        LOG.info("Consumer closed")


def main():
    configure_logging()
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda signum, frame: stop.set())
    try:
        consumer = Consumer({
            **kafka_config("order-tracker"),
            "group.id": os.getenv("KAFKA_GROUP_ID", "order-tracker"),
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "allow.auto.create.topics": False,
        })
        consume(consumer, orders_topic(), stop)
        return 0
    except Exception:
        LOG.exception("Tracker stopped with an error")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

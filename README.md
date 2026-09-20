# Simple Kafka orders demo

A small Python producer and tracker with confirmed delivery, explicit offset
commits, validation, and graceful shutdown. Requires Python 3.10+ and Docker Compose.

## Run

```sh
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
# Optional configuration (the scripts read exported environment variables):
cp .env.example .env
set -a
source .env
set +a
docker compose up -d
docker compose wait kafka-init  # exits 0 after the orders topic is ready
python tracker.py
```

In another terminal with the same virtual environment and exported configuration:

```sh
python producer.py --user marvin --item burger --quantity 2
```

The producer sends one message keyed by its order ID and exits 0 only after a
successful delivery callback. Supply `--order-id <uuid>` to retain a business ID
across retries. Kafka idempotence prevents duplicates from internal producer
retries; separate invocations can still create duplicate orders, even with the
same key. Delivery timeouts are uncertain outcomes, not proof of non-delivery.

The tracker validates and handles each message before synchronously committing
its next offset. This provides **at-least-once processing**: a crash after handling
but before committing can replay an order. `handle_order` currently logs only;
real business work should durably deduplicate `order_id` in the same transaction
as its side effects. Keep handling within Kafka's consumer poll interval.

Invalid messages, handler failures, poll errors, and commit failures stop the
tracker with a nonzero exit status. Failed processing is not committed; fix the
cause before restarting. A malformed record will block progress until an operator
repairs or deliberately skips it. There is no automatic discard or dead-letter
queue. SIGINT/SIGTERM finish the current message and close the consumer.

## Configuration

See `.env.example` for broker, topic, group, logging, and optional SASL/TLS settings.
Python does not automatically load `.env`; export it in each terminal. Compose
uses `.env` for topic initialization. Clients inside the Compose network should
use `kafka:29092`; host clients use `localhost:9092`.

The initializer creates a three-partition topic with seven-day retention. Existing
topics are left unchanged. The broker has a readiness check, disables automatic
topic creation, binds its host port to loopback, and persists data in a named volume.

**Upgrading the original demo:** it stored logs at `/tmp/kraft-combined-logs`, outside
the volume. This version uses `/var/lib/kafka/data`. Recreating an old container can
lose its original messages; export or migrate any data you need before running
`docker compose up` with this configuration.

```sh
python -m unittest discover -s tests -v
docker compose config --quiet
docker compose logs kafka-init
docker compose down  # retains the named data volume
```

This remains a single-broker local demo with plaintext listeners and replication
factor 1. Deployment needs a secured, replicated cluster, appropriate replication
and minimum in-sync replica settings, monitoring/alerts, and durable business
processing. Synchronous per-message commits favor clarity over throughput.

Commit behavior follows the [Confluent Python client API](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html).

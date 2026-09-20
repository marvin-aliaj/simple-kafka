"""Shared environment configuration; importing this module has no side effects."""
import logging
import os


def configure_logging():
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def kafka_config(client_id):
    config = {
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "client.id": client_id,
        "security.protocol": os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"),
    }
    for env, key in {
        "KAFKA_SASL_MECHANISM": "sasl.mechanism",
        "KAFKA_SASL_USERNAME": "sasl.username",
        "KAFKA_SASL_PASSWORD": "sasl.password",
        "KAFKA_SSL_CA_LOCATION": "ssl.ca.location",
    }.items():
        if os.getenv(env):
            config[key] = os.environ[env]
    return config


def orders_topic():
    return os.getenv("KAFKA_ORDERS_TOPIC", "orders")

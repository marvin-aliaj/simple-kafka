import json
import uuid

from confluent_kafka import Producer

producer = Producer({
    'bootstrap.servers': 'localhost:9092',
    'message.timeout.ms': 5000
})

def delivery_report(err, msg):
    if err:
        print('Message delivery failed: {}'.format(err))
    else:
        print(f'Message delivered to topic {msg.topic()} and partition {msg.partition()} at offset {msg.offset()}.')
        print(dir(msg))


order = {
    "order_id": str(uuid.uuid4()),
    "user": "marvin",
    "item": "burger",
    "quantity": 1
}

value = json.dumps(order).encode("utf-8")
try:
    producer.produce(topic="orders", value=value, callback=delivery_report)
    remaining = producer.flush(10)

    if remaining > 0:
        print("Kafka is unavailable. Message was not delivered.")

except BufferError as e:
    print(f"Producer queue is full: {e}")

except Exception as e:
    print(f"Kafka error: {e}")

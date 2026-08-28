"""
producer.py -- real Kafka producer (needs the broker from docker-compose.yml).

Publishes the same order-event schema used by data_generator/generate_events.py,
but onto an actual Kafka topic instead of a local JSON file. This is what the
checkout service would run in production.

Usage (after `docker compose up -d` and creating the topic):
    pip install kafka-python
    python kafka/producer.py --rate 5 --duration 60
"""
import argparse
import json
import sys
import time

sys.path.insert(0, "data_generator")
from generate_events import make_event  # noqa: E402

from kafka import KafkaProducer  # pip install kafka-python

TOPIC = "orders.raw"


def main(bootstrap_servers, rate_per_sec, duration_sec):
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",  # durability: wait for all in-sync replicas
        retries=5,
        linger_ms=20,  # small batching window for throughput
    )

    end = time.time() + duration_sec
    sent = 0
    try:
        while time.time() < end:
            event = make_event()
            # Partition key = customer_id -> guarantees per-customer ordering,
            # which matters if downstream logic assumes event order per customer.
            producer.send(TOPIC, key=event["customer_id"], value=event)
            sent += 1
            time.sleep(1.0 / rate_per_sec)
    finally:
        producer.flush()
        producer.close()
    print(f"Sent {sent} events to topic '{TOPIC}'")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--rate", type=float, default=5.0, help="events/sec")
    parser.add_argument("--duration", type=float, default=30.0, help="seconds")
    args = parser.parse_args()
    main(args.bootstrap, args.rate, args.duration)

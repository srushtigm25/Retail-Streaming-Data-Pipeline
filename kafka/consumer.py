"""
consumer.py -- plain Kafka consumer, useful to prove events are flowing
before/instead of watching the Spark streaming job. Spark itself consumes
the topic directly via the spark-sql-kafka connector (see
spark_jobs/01_bronze_ingest_kafka.py) -- this script is just for eyeballing.
"""
import argparse
import json

from kafka import KafkaConsumer  # pip install kafka-python

TOPIC = "orders.raw"


def main(bootstrap_servers, group_id):
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    print(f"Listening on '{TOPIC}' as group '{group_id}'... Ctrl+C to stop.")
    for msg in consumer:
        e = msg.value
        print(
            f"partition={msg.partition} offset={msg.offset} "
            f"order={e['order_id']} customer={e['customer_id']} "
            f"total=${e['order_total']} status={e['status']}"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--group", default="debug-console")
    args = parser.parse_args()
    main(args.bootstrap, args.group)

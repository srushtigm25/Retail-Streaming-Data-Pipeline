"""
generate_events.py
-------------------
Simulates upstream OLTP order events the way they'd arrive on a Kafka topic
(`orders.raw`) from an e-commerce checkout service.

In production this data is produced by the checkout microservice via a Kafka
producer (see kafka/producer.py for the real Kafka version). For local/offline
demo purposes (no broker available), this script writes the same JSON payloads
as newline-delimited JSON files that spark_jobs/01_bronze_ingest.py reads with
Spark's streaming file source -- structurally identical to consuming a Kafka
topic, so the Spark code doesn't change when you point it at a real broker.
"""
import json
import os
import random
import time
import uuid
from datetime import datetime, timedelta

from faker import Faker

fake = Faker()
random.seed(42)

OUTPUT_DIR = os.environ.get("RAW_EVENTS_DIR", "../lake/raw_stream")
os.makedirs(OUTPUT_DIR, exist_ok=True)

PRODUCTS = [
    {"product_id": f"P{str(i).zfill(4)}", "name": n, "category": c, "price": p}
    for i, (n, c, p) in enumerate([
        ("Wireless Mouse", "Electronics", 24.99),
        ("Mechanical Keyboard", "Electronics", 89.99),
        ("USB-C Hub", "Electronics", 34.50),
        ("Standing Desk", "Furniture", 349.00),
        ("Office Chair", "Furniture", 199.99),
        ("Notebook Set", "Stationery", 12.75),
        ("Water Bottle", "Lifestyle", 18.00),
        ("Desk Lamp", "Furniture", 45.25),
        ("Noise Cancelling Headphones", "Electronics", 149.99),
        ("Yoga Mat", "Lifestyle", 29.99),
        ("Backpack", "Lifestyle", 59.00),
        ("Monitor Arm", "Electronics", 79.99),
    ], start=1)
]

CUSTOMERS = [
    {
        "customer_id": f"C{str(i).zfill(4)}",
        "name": fake.name(),
        "email": fake.email(),
        "region": random.choice(["West", "East", "Midwest", "South"]),
        "signup_date": fake.date_between(start_date="-3y", end_date="-30d").isoformat(),
    }
    for i in range(1, 61)
]

STATUSES = ["PLACED", "PAID", "SHIPPED", "CANCELLED", "REFUNDED"]
STATUS_WEIGHTS = [0.35, 0.35, 0.18, 0.07, 0.05]


def make_event(event_time=None):
    product = random.choice(PRODUCTS)
    customer = random.choice(CUSTOMERS)
    qty = random.randint(1, 4)
    event_time = event_time or datetime.utcnow()
    return {
        "event_id": str(uuid.uuid4()),
        "order_id": f"O{uuid.uuid4().hex[:10].upper()}",
        "customer_id": customer["customer_id"],
        "customer_name": customer["name"],
        "customer_region": customer["region"],
        "product_id": product["product_id"],
        "product_name": product["name"],
        "product_category": product["category"],
        "unit_price": product["price"],
        "quantity": qty,
        "order_total": round(product["price"] * qty, 2),
        "status": random.choices(STATUSES, weights=STATUS_WEIGHTS)[0],
        "event_ts": event_time.isoformat(),
        "ingest_source": "checkout-service-v2",
    }


def backfill_batch(n_events=5000, days_back=45, out_path=None):
    """Bulk-generate a historical batch (simulates 45 days of order history)."""
    events = []
    now = datetime.utcnow()
    for _ in range(n_events):
        ts = now - timedelta(
            days=random.randint(0, days_back),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
        )
        events.append(make_event(ts))
    events.sort(key=lambda e: e["event_ts"])

    out_path = out_path or os.path.join(OUTPUT_DIR, "batch_000_history.json")
    with open(out_path, "w") as f:
        for e in events:
            f.write(json.dumps(e) + "\n")
    print(f"Wrote {len(events)} historical events to {out_path}")


def stream_batches(n_batches=6, events_per_batch=40, delay_sec=0.0):
    """
    Simulates a live Kafka stream by dropping a new file into the raw dir
    every few seconds -- Spark Structured Streaming's file source picks each
    one up exactly like it would poll a Kafka topic's new offsets.
    """
    for b in range(n_batches):
        events = [make_event() for _ in range(events_per_batch)]
        out_path = os.path.join(OUTPUT_DIR, f"batch_{b+1:03d}_live.json")
        with open(out_path, "w") as f:
            for e in events:
                f.write(json.dumps(e) + "\n")
        print(f"[stream] wrote batch {b+1}/{n_batches} -> {out_path}")
        if delay_sec:
            time.sleep(delay_sec)

def make_scd2_test_event():
    event = make_event()
    event["customer_id"] = "C0001"
    event["customer_name"] = "Christopher Oliver"
    event["customer_region"] = "East"
    event["event_ts"] = datetime.utcnow().isoformat()
    return event


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["backfill", "stream"], default="backfill")
    parser.add_argument("--n", type=int, default=5000)
    parser.add_argument("--batches", type=int, default=6)
    parser.add_argument("--delay", type=float, default=0.0)
    args = parser.parse_args()

    if args.mode == "backfill":
        backfill_batch(n_events=args.n)
    else:
        stream_batches(n_batches=args.batches, delay_sec=args.delay)

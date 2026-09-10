import json
import os
import random
import time
from datetime import datetime, timezone
from uuid import uuid4

from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_ORDER_TOPIC", "orders.raw")

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)


def make_event() -> dict:
    qty = random.randint(1, 5)
    unit_price = round(random.uniform(10, 300), 2)
    return {
        "event_id": str(uuid4()),
        "event_type": "order_created",
        "event_ts": datetime.now(timezone.utc).isoformat(),
        "order_id": f"ORD-{random.randint(10000, 99999)}",
        "customer_id": f"C-{random.randint(1000, 9999)}",
        "product_id": f"SKU-{random.randint(100, 199)}",
        "quantity": qty,
        "unit_price": unit_price,
        "amount": round(qty * unit_price, 2),
        "source_system": random.choice(["ERP", "CRM", "ECOM"]),
        "schema_version": 1,
    }


if __name__ == "__main__":
    for _ in range(25):
        event = make_event()
        producer.send(TOPIC, event)
        print(event)
        time.sleep(0.25)
    producer.flush()

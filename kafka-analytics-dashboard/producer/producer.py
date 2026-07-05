"""
Event Generator / Kafka Producer
---------------------------------
Simulates real-time user activity on an e-commerce site and publishes
events to three Kafka topics:

    - page_views   : every time a user views a page/product
    - user_clicks  : every time a user clicks a UI element
    - purchases    : every time a user completes a purchase

Run multiple instances of this script to simulate multiple traffic
sources and demonstrate Kafka's ability to handle concurrent producers.
"""

import json
import random
import time
from datetime import datetime, timezone

from faker import Faker
from kafka import KafkaProducer

fake = Faker()

BOOTSTRAP_SERVERS = "localhost:9092"

PRODUCTS = [
    {"id": "P1001", "name": "Wireless Headphones", "price": 59.99, "category": "Electronics"},
    {"id": "P1002", "name": "Running Shoes", "price": 89.99, "category": "Footwear"},
    {"id": "P1003", "name": "Coffee Maker", "price": 129.99, "category": "Home"},
    {"id": "P1004", "name": "Yoga Mat", "price": 24.99, "category": "Fitness"},
    {"id": "P1005", "name": "Smart Watch", "price": 199.99, "category": "Electronics"},
    {"id": "P1006", "name": "Backpack", "price": 45.99, "category": "Accessories"},
    {"id": "P1007", "name": "Desk Lamp", "price": 34.99, "category": "Home"},
    {"id": "P1008", "name": "Bluetooth Speaker", "price": 49.99, "category": "Electronics"},
]

REGIONS = ["North America", "Europe", "Asia", "South America", "Africa", "Oceania"]

# Simulate a pool of "active" users so the same user can generate
# multiple events, which lets us demonstrate active-user counting.
USER_POOL = [fake.uuid4() for _ in range(200)]


def make_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        linger_ms=50,       # small batching for realistic throughput
        retries=5,
    )


def random_event(event_type: str) -> dict:
    user_id = random.choice(USER_POOL)
    region = random.choice(REGIONS)
    base = {
        "event_type": event_type,
        "user_id": user_id,
        "region": region,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    if event_type == "page_view":
        product = random.choice(PRODUCTS)
        base["product_id"] = product["id"]
        base["product_name"] = product["name"]
        base["category"] = product["category"]

    elif event_type == "user_click":
        base["element"] = random.choice(
            ["add_to_cart", "wishlist", "product_image", "review_tab", "checkout_button"]
        )

    elif event_type == "purchase":
        product = random.choice(PRODUCTS)
        quantity = random.randint(1, 3)
        base.update(
            {
                "product_id": product["id"],
                "product_name": product["name"],
                "category": product["category"],
                "quantity": quantity,
                "unit_price": product["price"],
                "total_amount": round(product["price"] * quantity, 2),
            }
        )

    return base


def main():
    producer = make_producer()
    print(f"[producer] connected to {BOOTSTRAP_SERVERS}, streaming events... (Ctrl+C to stop)")

    # Weighted so purchases are rarer than views/clicks — mirrors real funnels.
    event_types = ["page_view"] * 5 + ["user_click"] * 3 + ["purchase"] * 1

    try:
        while True:
            event_type = random.choice(event_types)
            event = random_event(event_type)
            topic = {
                "page_view": "page_views",
                "user_click": "user_clicks",
                "purchase": "purchases",
            }[event_type]

            # Keying by user_id keeps all events for a given user on the
            # same partition — useful if you extend this to per-user state.
            producer.send(topic, key=event["user_id"], value=event)
            print(f"[producer] -> {topic}: {event_type} ({event['user_id'][:8]})")

            time.sleep(random.uniform(0.1, 0.5))  # simulate realistic arrival rate

    except KeyboardInterrupt:
        print("\n[producer] shutting down...")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()

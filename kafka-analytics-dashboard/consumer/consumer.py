"""
Stream Processor / Kafka Consumer
----------------------------------
Consumes raw event topics (page_views, user_clicks, purchases), maintains
live aggregates in memory, and every SNAPSHOT_INTERVAL seconds:

    1. Publishes a metrics snapshot to the `dashboard_metrics` topic
       (so the API layer can just be another consumer — no tight coupling).
    2. Persists the snapshot to Postgres for historical querying.

This demonstrates: consumer groups, windowed aggregation, and a
Kafka-to-Kafka pipeline (raw events -> derived/aggregated topic).
"""

import json
import threading
import time
from collections import Counter, defaultdict, deque
from datetime import datetime, timezone

import psycopg2
from kafka import KafkaConsumer, KafkaProducer

BOOTSTRAP_SERVERS = "localhost:9092"
RAW_TOPICS = ["page_views", "user_clicks", "purchases"]
METRICS_TOPIC = "dashboard_metrics"
SNAPSHOT_INTERVAL_SECONDS = 2
ACTIVE_USER_WINDOW_SECONDS = 60  # a user counts as "active" if seen in last 60s

PG_CONFIG = dict(
    host="localhost",
    port=5432,
    dbname="analytics",
    user="kafka_user",
    password="kafka_pass",
)

# ---- shared state (protected by lock) ----
lock = threading.RLock()
state = {
    "total_revenue": 0.0,
    "total_purchases": 0,
    "product_counter": Counter(),      # product_name -> units sold
    "region_counter": Counter(),       # region -> revenue
    "recent_events_count": 0,          # events since last snapshot
}
# deque of (timestamp, user_id) for sliding active-user window
recent_user_activity = deque()


def record_event(event: dict):
    global state
    now = time.time()
    with lock:
        recent_user_activity.append((now, event["user_id"]))
        state["recent_events_count"] += 1

        if event["event_type"] == "purchase":
            state["total_revenue"] += event["total_amount"]
            state["total_purchases"] += 1
            state["product_counter"][event["product_name"]] += event["quantity"]
            state["region_counter"][event["region"]] += event["total_amount"]


def active_user_count() -> int:
    cutoff = time.time() - ACTIVE_USER_WINDOW_SECONDS
    with lock:
        while recent_user_activity and recent_user_activity[0][0] < cutoff:
            recent_user_activity.popleft()
        return len({uid for _, uid in recent_user_activity})


def build_snapshot() -> dict:
    with lock:
        top_products = state["product_counter"].most_common(5)
        sales_by_region = dict(state["region_counter"])
        snapshot = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "total_revenue": round(state["total_revenue"], 2),
            "total_purchases": state["total_purchases"],
            "active_users": active_user_count(),
            "events_last_interval": state["recent_events_count"],
            "top_products": [{"name": n, "units": u} for n, u in top_products],
            "sales_by_region": sales_by_region,
        }
        state["recent_events_count"] = 0
    return snapshot


def consume_raw_events():
    """Thread target: continuously consume raw event topics and update state."""
    print("[consumer] connecting to Kafka (for raw event topics)...")
    try:
        consumer = KafkaConsumer(
            *RAW_TOPICS,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            group_id="analytics-processor",   # named consumer group -> enables scaling out
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="latest",
            enable_auto_commit=True,
            api_version=(2, 8, 1),   # skip kafka-python's flaky auto-negotiation
            api_version_auto_timeout_ms=30000,
        )
    except Exception as e:
        print(f"[consumer] FAILED to connect Kafka consumer: {e}")
        return
    print("[consumer] listening on:", RAW_TOPICS)
    for message in consumer:
        record_event(message.value)


def publish_snapshots():
    """Thread target: periodically publish + persist aggregated metrics."""
    print("[consumer] connecting to Kafka (for metrics topic)...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            api_version=(2, 8, 1),
            api_version_auto_timeout_ms=30000,
        )
        print("[consumer] Kafka producer connected.")
    except Exception as e:
        print(f"[consumer] FAILED to connect Kafka producer: {e}")
        return

    print("[consumer] connecting to Postgres...")
    try:
        pg_conn = psycopg2.connect(connect_timeout=5, **PG_CONFIG)
        pg_conn.autocommit = True
        cursor = pg_conn.cursor()
        print("[consumer] Postgres connected.")
    except Exception as e:
        print(f"[consumer] FAILED to connect Postgres: {e}")
        print("[consumer] Continuing WITHOUT Postgres persistence (metrics will still stream live).")
        pg_conn = None
        cursor = None

    print(f"[consumer] snapshot loop starting (every {SNAPSHOT_INTERVAL_SECONDS}s)...")
    while True:
        time.sleep(SNAPSHOT_INTERVAL_SECONDS)
        snapshot = build_snapshot()

        # 1. Publish to Kafka so any number of downstream consumers (API,
        #    alerting jobs, other dashboards) can react to it independently.
        try:
            producer.send(METRICS_TOPIC, value=snapshot)
        except Exception as e:
            print(f"[consumer] Kafka publish error: {e}")

        # 2. Persist to Postgres for history/reporting (optional — skipped if unavailable).
        if cursor is not None:
            try:
                cursor.execute(
                    """
                    INSERT INTO dashboard_metrics_history (ts, total_revenue, active_users, metrics)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (
                        snapshot["timestamp"],
                        snapshot["total_revenue"],
                        snapshot["active_users"],
                        json.dumps(snapshot),
                    ),
                )
            except Exception as e:
                print(f"[consumer] Postgres insert error: {e}")

        print(f"[consumer] snapshot -> revenue=${snapshot['total_revenue']} "
              f"active_users={snapshot['active_users']} "
              f"events={snapshot['events_last_interval']}")


def main():
    t1 = threading.Thread(target=consume_raw_events, daemon=True)
    t2 = threading.Thread(target=publish_snapshots, daemon=True)
    t1.start()
    t2.start()
    print("[consumer] processor running. Ctrl+C to stop.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[consumer] shutting down...")


if __name__ == "__main__":
    main()
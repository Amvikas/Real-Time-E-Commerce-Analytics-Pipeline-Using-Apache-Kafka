"""
API Layer
---------
Bridges the `dashboard_metrics` Kafka topic to the frontend:

    Kafka (dashboard_metrics) --> [background consumer thread] --> WebSocket clients

Also exposes a REST endpoint to fetch historical metrics from Postgres,
so the frontend can render trend charts on load instead of starting blank.
"""

import asyncio
import json
import threading
from typing import Optional, Set

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from kafka import KafkaConsumer

BOOTSTRAP_SERVERS = "localhost:9092"
METRICS_TOPIC = "dashboard_metrics"

PG_CONFIG = dict(
    host="localhost",
    port=5432,
    dbname="analytics",
    user="kafka_user",
    password="kafka_pass",
)

app = FastAPI(title="Kafka Analytics Dashboard API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Connected websocket clients
connected_clients: Set[WebSocket] = set()
main_loop: Optional[asyncio.AbstractEventLoop] = None


def kafka_listener_thread():
    """Runs in a background thread; pushes each metrics message to all
    connected websocket clients via the main asyncio event loop."""
    consumer = KafkaConsumer(
        METRICS_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id="api-broadcaster",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
    )
    print("[api] listening for metrics on Kafka...")
    for message in consumer:
        if main_loop is not None:
            asyncio.run_coroutine_threadsafe(broadcast(message.value), main_loop)


async def broadcast(payload: dict):
    dead = []
    for ws in connected_clients:
        try:
            await ws.send_json(payload)
        except Exception:
            dead.append(ws)
    for ws in dead:
        connected_clients.discard(ws)


@app.on_event("startup")
async def startup():
    global main_loop
    main_loop = asyncio.get_event_loop()
    thread = threading.Thread(target=kafka_listener_thread, daemon=True)
    thread.start()


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connected_clients.add(websocket)
    try:
        while True:
            # We don't expect messages from the client, but keep the
            # connection alive and detect disconnects.
            await websocket.receive_text()
    except WebSocketDisconnect:
        connected_clients.discard(websocket)


@app.get("/api/history")
def get_history(limit: int = 50):
    """Returns the most recent N metric snapshots from Postgres."""
    conn = psycopg2.connect(**PG_CONFIG)
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cursor.execute(
        "SELECT ts, total_revenue, active_users, metrics "
        "FROM dashboard_metrics_history ORDER BY ts DESC LIMIT %s",
        (limit,),
    )
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    # reverse so it's chronological for charting
    return list(reversed(rows))


@app.get("/api/health")
def health():
    return {"status": "ok"}

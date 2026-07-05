# Live Commerce Pulse — Real-Time Kafka Analytics Dashboard

A real-time analytics pipeline that simulates e-commerce user activity, streams it through
Apache Kafka, aggregates it in a stream processor, and displays it on a live dashboard.

<img width="945" height="505" alt="Screenshot 2026-07-05 114616" src="https://github.com/user-attachments/assets/f6056b73-c199-4e32-9194-f1cfa8002aaf" />


## Architecture

```
 ┌────────────┐     ┌─────────────────────────┐     ┌──────────────────┐
 │  Producer  │────▶│  Kafka Topics           │────▶│  Consumer /       │
 │ (producer/ │     │  page_views              │     │  Stream Processor │
 │  producer  │     │  user_clicks             │     │  (consumer/       │
 │   .py)     │     │  purchases               │     │   consumer.py)    │
 └────────────┘     └─────────────────────────┘     └────────┬──────────┘
                                                                │
                                              ┌─────────────────┼─────────────────┐
                                              ▼                                   ▼
                                     ┌──────────────────┐             ┌─────────────────────┐
                                     │ dashboard_metrics │             │  Postgres            │
                                     │ (Kafka topic)     │             │  dashboard_metrics_   │
                                     └────────┬──────────┘             │  history (for /api/   │
                                              │                        │  history)             │
                                              ▼                        └─────────────────────┘
                                     ┌──────────────────┐
                                     │  FastAPI (api/    │
                                     │  main.py)          │
                                     │  Kafka consumer -> │
                                     │  WebSocket bridge  │
                                     └────────┬──────────┘
                                              │ WebSocket
                                              ▼
                                     ┌──────────────────┐
                                     │  Dashboard (HTML/  │
                                     │  JS, Chart.js)     │
                                     └──────────────────┘
```

**Why it's built this way:** the consumer publishes aggregated metrics back onto
*another Kafka topic* rather than writing straight to the API's memory. That means
the API is just one more consumer — you could add a second consumer (e.g. an alerting
service, or a second dashboard) without touching the processing logic at all. This
is the core Kafka idea of decoupling producers from consumers via topics.

## Prerequisites

- Docker + Docker Compose
- Python 3.10+

## Setup

### 1. Start the infrastructure
```bash
docker-compose up -d
```
This starts Zookeeper, Kafka, Kafka UI (visual topic browser at http://localhost:8080),
and Postgres (with the schema in `init-db/init.sql` auto-applied on first boot).

Give it ~15-20 seconds for Kafka to fully come up before starting the app processes.

### 2. Install dependencies (do this once per component)
```bash
pip install -r producer/requirements.txt --break-system-packages
pip install -r consumer/requirements.txt --break-system-packages
pip install -r api/requirements.txt --break-system-packages
```
(Or use a virtualenv per component if you prefer — recommended for the final report's
"how to run" section.)

### 3. Run each component in its own terminal

```bash
# Terminal 1 — stream processor (must be running before the dashboard connects)
python consumer/consumer.py

# Terminal 2 — event generator
python producer/producer.py

# Terminal 3 — API
cd api && uvicorn main:app --reload --port 8000
```

Optional: run `python producer/producer.py` in 2-3 extra terminals to simulate
multiple simultaneous traffic sources and generate a livelier demo.

### 4. Open the dashboard
Just open `frontend/index.html` directly in a browser (no build step needed).
You should see revenue, active users, and top products updating every 2 seconds.



## Project structure
```
kafka-analytics-dashboard/
├── docker-compose.yml       # Kafka, Zookeeper, Kafka UI, Postgres
├── init-db/init.sql          # Postgres schema
├── producer/
│   ├── producer.py           # Simulates user activity events
│   └── requirements.txt
├── consumer/
│   ├── consumer.py           # Aggregates events, publishes metrics + persists history
│   └── requirements.txt
├── api/
│   ├── main.py                # FastAPI: Kafka -> WebSocket bridge + history endpoint
│   └── requirements.txt
└── frontend/
    └── index.html              # Live dashboard (vanilla JS + Chart.js)
```

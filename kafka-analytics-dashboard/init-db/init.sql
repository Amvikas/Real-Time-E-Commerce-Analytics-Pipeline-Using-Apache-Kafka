CREATE TABLE IF NOT EXISTS dashboard_metrics_history (
    id SERIAL PRIMARY KEY,
    ts TIMESTAMP NOT NULL DEFAULT NOW(),
    total_revenue NUMERIC NOT NULL,
    active_users INTEGER NOT NULL,
    metrics JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_dashboard_metrics_ts ON dashboard_metrics_history (ts);

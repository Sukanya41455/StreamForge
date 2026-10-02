CREATE TABLE IF NOT EXISTS minute_metrics (
    window_start TIMESTAMPTZ NOT NULL,
    metric_name TEXT NOT NULL,
    metric_value DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (window_start, metric_name)
);

CREATE TABLE IF NOT EXISTS product_window_metrics (
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    product_id TEXT NOT NULL,
    order_count BIGINT NOT NULL,
    revenue DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (window_start, window_end, product_id)
);

CREATE TABLE IF NOT EXISTS stream_errors (
    event_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source_topic TEXT NOT NULL,
    error_reason TEXT NOT NULL,
    raw_payload TEXT NOT NULL
);

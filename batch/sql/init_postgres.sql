CREATE TABLE IF NOT EXISTS batch_label_indicators (
    label TEXT PRIMARY KEY,
    flow_count BIGINT NOT NULL,
    percentage DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS batch_top_source_ports (
    port BIGINT,
    flow_count BIGINT
);

CREATE TABLE IF NOT EXISTS batch_top_destination_ports (
    port BIGINT,
    flow_count BIGINT
);

CREATE TABLE IF NOT EXISTS batch_protocol_indicators (
    protocol BIGINT,
    flow_count BIGINT,
    distinct_labels BIGINT
);

CREATE TABLE IF NOT EXISTS batch_attack_types (
    attack_type TEXT,
    flow_count BIGINT
);

CREATE TABLE IF NOT EXISTS batch_feature_statistics (
    feature TEXT,
    statistic TEXT,
    value DOUBLE PRECISION
);

-- Optional table for end-to-end latency instrumentation.
-- Person A should populate producer_timestamp and db_inserted_at in UTC.
CREATE TABLE IF NOT EXISTS ingested_events (
    event_id TEXT PRIMARY KEY,
    producer_timestamp TIMESTAMPTZ NOT NULL,
    db_inserted_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

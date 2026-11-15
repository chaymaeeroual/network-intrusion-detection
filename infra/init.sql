
CREATE TABLE IF NOT EXISTS intrusion_predictions (
    event_id VARCHAR(100) PRIMARY KEY,
    event_ts TIMESTAMP,
    actual_label VARCHAR(100),
    prediction DOUBLE PRECISION,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table pour les indicateurs batch 
CREATE TABLE IF NOT EXISTS batch_indicators (
    id SERIAL PRIMARY KEY,
    indicator_name VARCHAR(100) NOT NULL,
    indicator_value VARCHAR(255),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index
CREATE INDEX IF NOT EXISTS idx_predictions_label ON intrusion_predictions(prediction);
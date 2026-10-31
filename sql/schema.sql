-- Database Schema for Forex Data Platform

CREATE TABLE IF NOT EXISTS curated_rates (
    pair VARCHAR(10) NOT NULL,
    timestamp_utc TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    open NUMERIC(12, 6) NOT NULL,
    high NUMERIC(12, 6) NOT NULL,
    low NUMERIC(12, 6) NOT NULL,
    close NUMERIC(12, 6) NOT NULL,
    volume BIGINT NOT NULL,
    is_filled BOOLEAN DEFAULT FALSE,
    gap_length INT DEFAULT 0,
    ingest_ts TIMESTAMP WITHOUT TIME ZONE,
    PRIMARY KEY (pair, timestamp_utc)
);

CREATE INDEX IF NOT EXISTS idx_curated_pair_ts ON curated_rates (pair, timestamp_utc DESC);

CREATE TABLE IF NOT EXISTS quarantine (
    id SERIAL PRIMARY KEY,
    pair VARCHAR(10),
    date_str VARCHAR(50),
    open NUMERIC(12, 6),
    high NUMERIC(12, 6),
    low NUMERIC(12, 6),
    close NUMERIC(12, 6),
    volume BIGINT,
    quarantine_reason VARCHAR(100),
    quarantined_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS quality_results (
    id SERIAL PRIMARY KEY,
    run_timestamp TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    total_records INT,
    passed_records INT,
    quarantined_records INT,
    pass_rate_pct NUMERIC(5, 2)
);

CREATE TABLE IF NOT EXISTS fill_log (
    id SERIAL PRIMARY KEY,
    pair VARCHAR(10) NOT NULL,
    filled_timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    gap_length INT NOT NULL,
    columns_filled VARCHAR(100),
    fill_method VARCHAR(50),
    recorded_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS model_registry (
    model_version VARCHAR(50) PRIMARY KEY,
    registered_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    pair VARCHAR(10) NOT NULL,
    model_type VARCHAR(50) NOT NULL,
    horizon_hours INT NOT NULL,
    mae NUMERIC(10, 6),
    rmse NUMERIC(10, 6),
    directional_accuracy NUMERIC(5, 2),
    pct_improvement_over_naive NUMERIC(6, 2),
    data_range_start TIMESTAMP WITHOUT TIME ZONE,
    data_range_end TIMESTAMP WITHOUT TIME ZONE,
    features_list TEXT
);

CREATE TABLE IF NOT EXISTS predictions (
    id SERIAL PRIMARY KEY,
    model_version VARCHAR(50) REFERENCES model_registry(model_version),
    pair VARCHAR(10) NOT NULL,
    forecast_origin TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    target_horizon INT NOT NULL,
    target_timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    predicted_log_return NUMERIC(10, 6) NOT NULL,
    predicted_implied_price NUMERIC(12, 6) NOT NULL,
    predicted_direction VARCHAR(10) NOT NULL,
    scored_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (pair, forecast_origin, target_horizon)
);

CREATE TABLE IF NOT EXISTS monitoring_alerts (
    id SERIAL PRIMARY KEY,
    detected_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    pair VARCHAR(10) NOT NULL,
    metric_name VARCHAR(50) NOT NULL,
    metric_value NUMERIC(10, 4) NOT NULL,
    threshold_value NUMERIC(10, 4) NOT NULL,
    alert_level VARCHAR(20) NOT NULL,
    message TEXT
);


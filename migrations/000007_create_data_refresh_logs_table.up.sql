CREATE TABLE IF NOT EXISTS data_refresh_logs (
    log_id SERIAL PRIMARY KEY,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    status VARCHAR(20) NOT NULL, -- 'STARTED', 'COMPLETED', 'FAILED'
    rows_processed INT DEFAULT 0,
    error_message TEXT,
    triggered_by VARCHAR(100) NOT NULL DEFAULT 'SYSTEM'
);
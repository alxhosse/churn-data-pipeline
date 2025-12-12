-- Create metric_name table for churn analytics schema
CREATE TABLE IF NOT EXISTS churn_analytics.metric_name
(
    metric_name_id SERIAL PRIMARY KEY,
    metric_name TEXT UNIQUE NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_metric_name_name 
    ON churn_analytics.metric_name (metric_name);

-- Create metric table for churn analytics schema
-- Note: account_id is TEXT (not INTEGER) to match event table
CREATE TABLE IF NOT EXISTS churn_analytics.metric
(
    account_id TEXT NOT NULL,
    metric_time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    metric_name_id INTEGER NOT NULL REFERENCES churn_analytics.metric_name(metric_name_id),
    metric_value REAL NOT NULL,
    CONSTRAINT pk_metric PRIMARY KEY (account_id, metric_name_id, metric_time)
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_metric_account_id
    ON churn_analytics.metric (account_id);

CREATE INDEX IF NOT EXISTS idx_metric_account_time
    ON churn_analytics.metric (account_id, metric_name_id, metric_time);

CREATE INDEX IF NOT EXISTS idx_metric_time
    ON churn_analytics.metric (metric_time, metric_name_id);

CREATE INDEX IF NOT EXISTS idx_metric_type
    ON churn_analytics.metric (metric_name_id);


-- Create schema for churn analytics events data
CREATE SCHEMA IF NOT EXISTS churn_analytics;

-- Set search path
SET search_path TO churn_analytics;

-- Create event_type table
CREATE TABLE IF NOT EXISTS churn_analytics.event_type
(
    event_type_id SERIAL PRIMARY KEY,
    event_type_name TEXT UNIQUE NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_event_type_name 
    ON churn_analytics.event_type (event_type_name);

-- Create event table
-- Note: The CSV has account_id, event_time, event_type, product_id, additional_data
-- We'll map event_type to event_type_id via the event_type table
-- account_id appears to be a string (UUID/hash), not an integer
CREATE TABLE IF NOT EXISTS churn_analytics.event
(
    account_id TEXT NOT NULL,
    event_time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    event_type_id INTEGER NOT NULL REFERENCES churn_analytics.event_type(event_type_id),
    product_id TEXT,
    additional_data TEXT,
    CONSTRAINT pk_event PRIMARY KEY (account_id, event_time, event_type_id)
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_event_account_id
    ON churn_analytics.event (account_id);

CREATE INDEX IF NOT EXISTS idx_event_account_time
    ON churn_analytics.event (account_id, event_time);

CREATE INDEX IF NOT EXISTS idx_event_time
    ON churn_analytics.event (event_time);

CREATE INDEX IF NOT EXISTS idx_event_type
    ON churn_analytics.event (event_type_id);

CREATE INDEX IF NOT EXISTS idx_event_product_id
    ON churn_analytics.event (product_id);


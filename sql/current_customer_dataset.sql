-- Create current customer dataset
-- One row per customer, one column per metric
-- Uses the latest metric_time for all customers
-- Create current customer dataset (no subscription table, using accounts with recent events)

WITH metric_date AS (
    SELECT MAX(metric_time) AS last_metric_time 
    FROM churn_analytics.metric
),
recent_accounts AS (
    -- Get accounts that have events in the last 90 days (active customers)
    SELECT DISTINCT account_id
    FROM churn_analytics.event e
    CROSS JOIN metric_date d
    WHERE e.event_time >= d.last_metric_time - INTERVAL '90 days'
),
metric_pivot AS (
    SELECT 
        m.account_id,
        d.last_metric_time,
        n.metric_name,
        m.metric_value
    FROM churn_analytics.metric m
    CROSS JOIN metric_date d
    INNER JOIN churn_analytics.metric_name n 
        ON m.metric_name_id = n.metric_name_id
    WHERE m.metric_time = d.last_metric_time
      AND m.account_id IN (SELECT account_id FROM recent_accounts)
)
SELECT 
    account_id,
    last_metric_time,
    %metric_columns
FROM metric_pivot
PIVOT (
    SUM(metric_value) FOR metric_name IN (%metric_names)
) AS p
GROUP BY account_id, last_metric_time
ORDER BY account_id;


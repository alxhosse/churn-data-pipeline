-- Insert count metric for a specific event type
-- Counts events in the 28 days prior to each metric_date
-- Insert count metric for a specific event type (TEXT account_id)

WITH date_vals AS (
    SELECT i::timestamp AS metric_date 
    FROM generate_series('%from_yyyy-mm-dd'::date, '%to_yyyy-mm-dd'::date, '7 day'::interval) i
)
INSERT INTO churn_analytics.metric (account_id, metric_time, metric_name_id, metric_value)
SELECT 
    e.account_id, 
    d.metric_date, 
    %new_metric_id, 
    COUNT(*) AS metric_value
FROM churn_analytics.event e 
INNER JOIN date_vals d
    ON e.event_time < d.metric_date 
    AND e.event_time >= d.metric_date - INTERVAL '28 day'
INNER JOIN churn_analytics.event_type t 
    ON t.event_type_id = e.event_type_id
WHERE t.event_type_name = '%event2measure'
GROUP BY e.account_id, d.metric_date
ON CONFLICT (account_id, metric_name_id, metric_time) DO NOTHING;


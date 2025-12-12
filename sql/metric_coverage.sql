-- Calculate metric coverage statistics
-- Shows what percentage of accounts have each metric
-- Calculate metric coverage statistics (no subscription table, using distinct accounts from events)

WITH date_range AS (
    SELECT 
        '%from_yyyy-mm-dd'::timestamp AS start_date,
        '%to_yyyy-mm-dd'::timestamp AS end_date
),
account_count AS (
    SELECT COUNT(DISTINCT account_id) AS n_account
    FROM churn_analytics.event e
    CROSS JOIN date_range d
    WHERE e.event_time >= d.start_date
      AND e.event_time <= d.end_date
)
SELECT 
    n.metric_name,
    COUNT(DISTINCT m.account_id) AS count_with_metric,
    n_account AS n_account,
    (COUNT(DISTINCT m.account_id))::float / n_account::float AS pcnt_with_metric,
    AVG(m.metric_value) AS avg_value,
    MIN(m.metric_value) AS min_value,
    MAX(m.metric_value) AS max_value,
    MIN(m.metric_time) AS earliest_metric,
    MAX(m.metric_time) AS last_metric
FROM churn_analytics.metric m
CROSS JOIN account_count
CROSS JOIN date_range d
INNER JOIN churn_analytics.metric_name n 
    ON m.metric_name_id = n.metric_name_id
WHERE m.metric_time >= d.start_date
  AND m.metric_time <= d.end_date
GROUP BY n.metric_name, n_account
ORDER BY n.metric_name;


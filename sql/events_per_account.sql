-- Events per account per month analysis
-- This query answers:
-- - What events are most common?
-- - What events are least common?
-- - How many events average more than X events per customer per month?

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
    et.event_type_name,
    COUNT(*) AS n_event,
    n_account AS n_account,
    COUNT(*)::float / n_account::float AS events_per_account,
    EXTRACT(DAYS FROM end_date - start_date)::float / 28 AS n_months,
    (COUNT(*)::float / n_account::float) / 
        (EXTRACT(DAYS FROM end_date - start_date)::float / 28.0) AS events_per_account_per_month
FROM churn_analytics.event e
CROSS JOIN account_count
INNER JOIN churn_analytics.event_type et ON et.event_type_id = e.event_type_id
CROSS JOIN date_range d
WHERE e.event_time >= d.start_date
  AND e.event_time <= d.end_date
GROUP BY e.event_type_id, n_account, end_date, start_date, et.event_type_name
ORDER BY events_per_account_per_month DESC;


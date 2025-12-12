-- Events per day analysis for a specific event type
-- This helps answer:
-- - Do events happen equally every day, or are there patterns?
-- - Are there any gaps in the record of any events?
-- - Are there any events that only occur in part of the history?
-- - Are there any extreme outliers or anomalies in the number of events?

WITH date_range AS (
    SELECT i::timestamp AS calc_date
    FROM generate_series('%from_yyyy-mm-dd'::date, '%to_yyyy-mm-dd'::date, '1 day'::interval) i
),
the_event AS (
    SELECT * 
    FROM churn_analytics.event e
    INNER JOIN churn_analytics.event_type et ON et.event_type_id = e.event_type_id
    WHERE et.event_type_name = '%event2measure'
)
SELECT 
    calc_date AS event_date,
    COUNT(e.*) AS n_event
FROM date_range d
LEFT OUTER JOIN the_event e ON d.calc_date = e.event_time::date
GROUP BY calc_date
ORDER BY calc_date;


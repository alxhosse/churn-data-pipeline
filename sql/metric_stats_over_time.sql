-- Calculate metric statistics over time
-- Shows average, min, max, and count of metrics at each time point
-- Calculate metric statistics over time

WITH date_range AS (
    SELECT i::timestamp AS calc_date 
    FROM generate_series('%from_yyyy-mm-dd'::date, '%to_yyyy-mm-dd'::date, '7 day'::interval) i
),
the_metric AS (
    SELECT * 
    FROM churn_analytics.metric m
    INNER JOIN churn_analytics.metric_name n 
        ON m.metric_name_id = n.metric_name_id
    WHERE n.metric_name = '%metric2measure'
)
SELECT 
    calc_date,
    AVG(metric_value) AS avg,
    COUNT(the_metric.*) AS n_calc,
    MIN(metric_value) AS min,
    MAX(metric_value) AS max
FROM date_range d
LEFT OUTER JOIN the_metric 
    ON d.calc_date = the_metric.metric_time
GROUP BY calc_date
ORDER BY calc_date;


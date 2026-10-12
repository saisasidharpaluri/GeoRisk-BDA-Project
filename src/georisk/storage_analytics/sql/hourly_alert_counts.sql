-- Athena/Trino and DuckDB: UTC hour buckets from normalized event-time strings.
SELECT
    substr(event_time, 1, 13) AS hour_bucket,
    COUNT(*) AS alerted_event_count
FROM georisk_risk_events
WHERE event_date BETWEEN DATE '2026-01-01' AND DATE '2026-01-31'
    AND risk_alert
GROUP BY substr(event_time, 1, 13)
ORDER BY hour_bucket;

-- Athena/Trino and DuckDB: count only alerted events for each exposed vessel.
SELECT
    vessel_id,
    COUNT(*) AS alerted_event_count,
    MIN(event_time) AS first_alerted_event_time,
    MAX(event_time) AS last_alerted_event_time
FROM georisk_risk_events
WHERE event_date BETWEEN DATE '2026-01-01' AND DATE '2026-01-31'
    AND risk_alert
GROUP BY vessel_id
ORDER BY alerted_event_count DESC, vessel_id;

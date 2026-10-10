-- Athena: date-bounded risk summary over curated risk events.
SELECT
    COUNT(*) AS risk_event_count,
    SUM(CASE WHEN risk_alert THEN 1 ELSE 0 END) AS alerted_event_count,
    COUNT(DISTINCT vessel_id) AS vessel_count,
    COUNT(DISTINCT CASE WHEN risk_alert THEN vessel_id END) AS exposed_vessel_count
FROM georisk_risk_events
WHERE event_date BETWEEN DATE '2026-01-01' AND DATE '2026-01-31';

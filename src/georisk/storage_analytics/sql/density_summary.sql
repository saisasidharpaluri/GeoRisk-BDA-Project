-- Athena: H3 density history for one date-bounded period.
SELECT
    window_start,
    window_end,
    h3_index,
    h3_resolution,
    unique_vessel_count
FROM georisk_density_metrics
WHERE event_date BETWEEN DATE '2026-01-01' AND DATE '2026-01-31'
ORDER BY window_start, h3_index;

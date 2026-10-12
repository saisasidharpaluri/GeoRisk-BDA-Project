-- Athena/Trino and DuckDB: peak vessel density per cell for a bounded period.
SELECT
    h3_index,
    MAX(unique_vessel_count) AS peak_unique_vessel_count
FROM georisk_density_metrics
WHERE event_date BETWEEN DATE '2026-01-01' AND DATE '2026-01-31'
GROUP BY h3_index
ORDER BY peak_unique_vessel_count DESC, h3_index
LIMIT 20;

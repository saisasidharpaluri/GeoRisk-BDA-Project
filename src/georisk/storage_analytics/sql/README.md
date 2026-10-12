# Lake SQL queries

These packaged queries use SQL supported by Athena/Trino and DuckDB. All use
inclusive `event_date` partition bounds (January 2026 in the examples). Replace
the example dates and table names for your data. Routes are computed in Python.

| Query | Result |
| --- | --- |
| `risk_summary.sql` | Total and alerted events, distinct vessels, and exposed vessels. |
| `density_summary.sql` | Density windows ordered by window start and H3 cell. |
| `top_congested_cells.sql` | Top 20 cells by peak unique vessel count across the selected windows; ties ordered by H3 cell. |
| `exposure_by_vessel.sql` | Alerted event count and first/last alerted event time per exposed vessel; ordered by count descending, then vessel ID. |
| `hourly_alert_counts.sql` | Alerted event counts per UTC hour, ordered chronologically. Hours without alerts are omitted. |

Risk queries read `georisk_risk_events`; density queries read
`georisk_density_metrics`. These tables must expose `event_date` as a `DATE`.
The local tests create DuckDB views over the curated lake, for example:

```sql
CREATE VIEW georisk_risk_events AS
SELECT * FROM read_parquet(
    'output/lake/curated/risk_events/*/*.parquet',
    hive_partitioning = true,
    hive_types = {'event_date': DATE}
);
```

Create the density view the same way over `curated/density_metrics`. On Athena,
use the corresponding Glue Catalog tables. The view setup above is specific to
DuckDB; the five query files use portable SQL without `UNNEST` or timestamp
parsing. Hour buckets use `substr(event_time, 1, 13)` (`YYYY-MM-DDTHH`). First and
last event times use string ordering over the normalized UTC timestamps produced
by the pipeline.

Install the `test` extra with `-c constraints.txt` and run
`.venv\Scripts\python -m pytest tests/test_sql.py -q` for local verification.
Tests skip with a reason if DuckDB is unavailable or its DLL cannot load.

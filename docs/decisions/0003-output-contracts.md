# 0003: Versioned risk and density output contracts

## Context
Processing, Parquet, SQL, routing, and dashboard consumers share record fields.

## Decision
Use the packaged [v1 schemas](../../src/georisk/schemas/v1/). Risk requires
`schema_version`, `event_id`, `event_time`, `vessel_id`, `latitude`,
`longitude`, `h3_index`, `h3_resolution`, `coverage_mode`, `risk_alert`, and
`matching_hazards`. A hazard match requires `alert_id`, `hazard_type`,
`severity`, `valid_from`, and `valid_until` (after `valid_from`). `risk_alert`
must equal whether matches exist, and `h3_index` must be a valid cell at
`h3_resolution`. Density requires `schema_version`, `window_start`,
`window_end` (after the start), `h3_index` (valid at its resolution),
`h3_resolution`, and `unique_vessel_count`. Empty hazard matches are `[]`.

## Consequences
Retain UTC event time, resolution, and source identity for traceability.
Explicit Arrow schemas prevent empty lists or mixed hazard geometries from
changing Parquet types. Contract consistency tests enforce field agreement;
weather geometry is stored as `geometry_json` and restored on reads.
Breaking shared-contract changes require owner agreement under the schema README.

## Status
Accepted. The 2026-10 v1 tightenings that made the formerly optional risk and
match fields required are listed in the schema README changelog.

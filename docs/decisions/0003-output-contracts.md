# 0003: Versioned risk and density output contracts

## Context
Processing, Parquet, SQL, routing, and dashboard consumers share record fields.

## Decision
Use the packaged [v1 schemas](../../src/georisk/schemas/v1/). Risk requires
`schema_version`, `event_time`, `vessel_id`, `h3_index`, `h3_resolution`,
`risk_alert`, and `matching_hazards`. Additive optional fields `event_id`,
`latitude`, `longitude`, and `coverage_mode` are always emitted by current
processors. A hazard match requires `alert_id` and `severity`; current writers
also include `hazard_type`, `valid_from`, and `valid_until`.
Density requires `schema_version`, `window_start`, `window_end`, `h3_index`,
`h3_resolution`, and `unique_vessel_count`. Empty hazard matches are `[]`.

## Consequences
Retain UTC event time, resolution, and source identity for traceability.
Explicit Arrow schemas prevent empty lists or mixed hazard geometries from
changing Parquet types. Contract consistency tests enforce field agreement;
weather geometry is stored as `geometry_json` and restored on reads.
Breaking shared-contract changes require owner agreement under the schema README.

## Status
Accepted; no schema changes are introduced by this record.

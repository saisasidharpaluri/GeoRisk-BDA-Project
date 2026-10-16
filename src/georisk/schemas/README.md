# Shared event contracts

The `v1` JSON Schemas define producer and consumer interfaces. All event timestamps are strict RFC 3339 UTC: `YYYY-MM-DDTHH:MM:SS[.fraction]` followed by `Z`, `+00:00`, or `-00:00` (uppercase `T`/`Z`; 1–9 fraction digits, truncated to microseconds). Vessel coordinates use separate `latitude` and `longitude` fields; GeoJSON hazard coordinates follow GeoJSON longitude/latitude order. Breaking changes require a new schema version and agreement from all four workstream owners. The only exceptions are the v1 tightenings in the changelog below, agreed by all owners for this semester while v1 has no deployed consumers.

Python producers and processors should call `georisk.contracts.validate_event`
before accepting or publishing an event. It loads schemas from package resources,
so validation also works from an installed wheel outside the source checkout, and
enforces UTC `date-time` values through `parse_utc_timestamp()`, which gives the
same result on every supported Python version (Spark uses the equivalent regex).
NaN and infinite numbers are rejected in every contract (`<path>: must be a
finite number`). Invalid records should be quarantined by the
streaming layer rather than silently accepted. Hazard polygon coordinates use
GeoJSON longitude/latitude order and must contain at least one ring.

Validation reuses one cached validator per contract. The public `load_schema()`
helper remains uncached and returns a freshly loaded schema on each call.

Validated examples and documented rejection cases are in
[v1/examples/README.md](v1/examples/README.md).

## v1 changelog

- 2026-10 (S33): new `delay_estimate` contract for per-event linear-model
  predictions, training-mean baselines, synthetic labels, feature values, and
  train/evaluation membership. Existing event contracts are unchanged.
- 2026-10: additive optional fields on risk_event (no version bump): event_id,
  latitude, longitude, and coverage_mode (center or overlap). Hazard matches
  require alert_id and severity (1–5); only documented match fields are accepted.
- 2026-10 (S13): v1 weather-alert validation now enforces longitude/latitude
  bounds, closure of every Polygon/MultiPolygon ring, and valid_until strictly
  after valid_from. Producers sending formerly accepted invalid geometry or
  empty/inverted intervals must correct their records.
- 2026-10 (S14): v1 telemetry requires speed_knots (0–60 inclusive) and
  heading_deg (0 inclusive to 360 exclusive). Producers must include both fields.
- 2026-10-16: further v1 tightenings (no deployed consumers; current writers
  already comply). Timestamps must match the strict RFC 3339 UTC profile above,
  and NaN/Infinity is rejected everywhere. weather_alert rings need at least
  three distinct positions, non-zero area, and no edge spanning more than 180°
  of longitude (split antimeridian-crossing areas into a MultiPolygon).
  risk_event now requires event_id, latitude, longitude, and coverage_mode;
  each match requires alert_id, hazard_type, severity, valid_from, and
  valid_until (after valid_from). risk_alert must equal whether matches exist,
  and h3_index must be a valid cell at h3_resolution. density_metric requires
  window_end after window_start and a valid h3_index at h3_resolution.

# Shared event contracts

The `v1` JSON Schemas define producer and consumer interfaces. All event timestamps are UTC ISO 8601. Vessel coordinates use separate `latitude` and `longitude` fields; GeoJSON hazard coordinates follow GeoJSON longitude/latitude order. Breaking changes require a new schema version and agreement from all four workstream owners.

Python producers and processors should call `georisk.contracts.validate_event`
before accepting or publishing an event. It loads schemas from package resources,
so validation also works from an installed wheel outside the source checkout, and
enforces UTC `date-time` values. Invalid records should be quarantined by the
streaming layer rather than silently accepted. Hazard polygon coordinates use
GeoJSON longitude/latitude order and must contain at least one ring.

Validation reuses one cached validator per contract. The public `load_schema()`
helper remains uncached and returns a freshly loaded schema on each call.

## v1 changelog

- 2026-10: additive optional fields on risk_event (no version bump): event_id,
  latitude, longitude, and coverage_mode (center or overlap). Hazard matches
  require alert_id and severity (1–5); only documented match fields are accepted.
- 2026-10 (S13): v1 weather-alert validation now enforces longitude/latitude
  bounds, closure of every Polygon/MultiPolygon ring, and valid_until strictly
  after valid_from. Producers sending formerly accepted invalid geometry or
  empty/inverted intervals must correct their records.
- 2026-10 (S14): v1 telemetry requires speed_knots (0–60 inclusive) and
  heading_deg (0 inclusive to 360 exclusive). Producers must include both fields.

# Member 2 - Spark streaming and spatial processing

Own event validation, H3 conversion, active-hazard matching, risk events, and 15-minute corridor density using Spark Structured Streaming in AWS Glue.

## Starter modules

- `h3_index.py`: point-to-cell and hazard-geometry coverage interfaces.
- `stream_job.py`: stream processing entry point.

Use resolution 7 as the initial demonstration default and retain the resolution on every output. Expire hazards according to `valid_until`. Deduplicate by vessel, H3 cell, and window before counting unique vessels. Document H3 boundary semantics; refine candidate matches geometrically if accuracy needs it.

The local Phase 3 core is implemented in `h3_index.py` and `stream_job.py`.
`process_events()` validates inputs, maps points and hazard geometries to H3,
matches only hazards active at event time, emits one risk row per telemetry
event, and creates 15-minute unique-vessel density metrics. The local runner
accepts Phase 2 JSONL files through `input_dir` and writes `risk-events.jsonl`
and `density-metrics.jsonl` when `output_dir` is configured.

Each batch computes H3 coverage once per alert and indexes the alerts by cell.
Telemetry matching checks only the alerts covering its cell, applies the
half-open validity interval (`valid_from <= event_time < valid_until`), and
returns matching hazards sorted by `alert_id`.

H3 coverage defaults to `overlap` (GEORISK_H3_COVERAGE_MODE). It includes
boundary cells missed by `center`, but can flag vessels just outside the
polygon. Both modes are candidate screens, not exact point containment.
Set the mode to `center` for cell-center containment. The mode and resolution
are retained on every risk output and in the pipeline report.

## Handoff

Consume Member 1's event schemas and publish risk and density rows using `src/georisk/schemas/v1/risk_event.schema.json` and `src/georisk/schemas/v1/density_metric.schema.json`.

Invalid records are quarantined by default while valid records continue through the batch. Results include quarantine entries and input/quarantine statistics. The runner always writes quarantine.jsonl, including malformed JSON lines (line number and up to 1,000 raw characters); its timestamps exclude it from the reproducible artifact manifest. Set GEORISK_STRICT_VALIDATION=true (or strict=True in process_events) to raise on invalid input.

Risk rows carry the source event_id, latitude, longitude, and coverage_mode. Only the first valid telemetry record per event_id is processed in each batch; later copies are counted in duplicates_dropped without quarantine or density inflation.

A re-issued alert replaces the earlier version for the whole batch. Among valid alerts sharing an alert_id, the latest event_time wins; ties keep the later input record. Superseded valid versions are counted in alert_revisions_superseded, and coverage is computed only for the retained versions.

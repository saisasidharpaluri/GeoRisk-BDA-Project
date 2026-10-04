# Member 2 - Spark streaming and spatial processing

Own event validation, H3 conversion, active-hazard matching, risk events, and 15-minute corridor density using Spark Structured Streaming in AWS Glue.

## Starter modules

- `h3_index.py`: point-to-cell and hazard-geometry coverage interfaces.
- `stream_job.py`: stream processing entry point.

Use resolution 7 as the initial demonstration default and retain the resolution on every output. Expire hazards according to `valid_until`. Deduplicate by vessel, H3 cell, and window before counting unique vessels. Document H3 boundary semantics; refine candidate matches geometrically if accuracy needs it.

## Handoff

Consume Member 1's event schemas and publish risk and density rows using `schemas/v1/risk_event.schema.json` and `schemas/v1/density_metric.schema.json`.

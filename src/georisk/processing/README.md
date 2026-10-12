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

Density windows default to 15 minutes. Set GEORISK_DENSITY_WINDOW_MINUTES
(or `window_minutes` in the processor/runner) to an integer from 1 to 1440
that divides 1440. Windows are UTC epoch-aligned, half-open intervals;
60- and 120-minute windows therefore group correctly across hour boundaries.

Telemetry is evaluated in input (arrival) order. After validation and event-ID
deduplication, events older than `max_event_time - allowed_lateness` are
quarantined with `reason="late_event"` and excluded from risk and density.
Events exactly on the watermark are accepted. This policy also applies with
strict validation enabled. Invalid inputs and duplicates do not advance time.
GEORISK_ALLOWED_LATENESS_MINUTES defaults to 10 (integer range 0..1440).
Stats include `late_dropped`, `max_event_time`, and `watermark` (UTC Z, or null
without accepted telemetry). Watermark state is local to each batch. Accepted
risk output is sorted by event time, vessel ID, and event ID; density output
is sorted by window and cell. Sorting occurs after arrival-order decisions.

The runner returns `metadata` and writes `run-metadata.json` alongside the
JSONL files. It records processing time, latest accepted event time, watermark,
settings, final stats (including malformed JSON), and `simulated: true`.
The pipeline embeds it under `processing.metadata`. This wall-clock-dependent
file is excluded from the reproducible artifact manifest.

The run and validation CLIs configure JSON-lines logs on stderr, leaving
JSON results on stdout. GEORISK_LOG_LEVEL defaults to INFO; DEBUG includes
one record per quarantine entry. INFO records contain processing settings,
counts, generation counts, lake paths, model metrics, and route decisions.
Library calls use named loggers without configuring application logging.
Each line contains `ts`, `level`, `logger`, `message`, and structured fields.

## Spark batch transforms

`spark_transforms.py` reads typed JSONL, captures malformed records, validates
required telemetry fields and bounds, and assigns H3 cells with a pandas UDF.
`split_valid()` returns valid and reason-tagged invalid DataFrames. Apply it
before `assign_h3()`. UTC timestamps, schema version 1, speed 0..60 knots and
heading 0..<360 degrees are required. Static alerts retain geometry as JSON.

`hazard_cells_df()` validates the small driver-side alert list and expands only
its latest revisions. `match_risk()` broadcasts those cells and uses half-open
validity intervals. Unmatched events have `matching_hazards=[]`; matched hazards
are sorted by alert ID. Output types match the Arrow lake schema.

`density()` counts distinct vessels in configurable UTC windows (15 minutes by
default). Repeated positions never inflate the vessel count. Windows must divide
1440 minutes, including larger 60- and 120-minute windows.

## Spark file streaming

In the WSL Spark environment described in the project README:

```python
from georisk.config import GeoRiskConfig
from georisk.spark_session import get_spark
from georisk.processing.spark_stream_job import run_stream

settings = GeoRiskConfig.from_env()
spark = get_spark()
query = run_stream(
    spark, settings.stream_dir, settings.spark_output_dir, settings.spark_checkpoint_dir,
    resolution=settings.resolution, coverage_mode=settings.coverage_mode,
    window_minutes=settings.density_window_minutes,
    allowed_lateness_minutes=settings.allowed_lateness_minutes,
)
query.awaitTermination()
spark.stop()
```

Publish inputs with `FileStreamPublisher`: `vessel-telemetry/*.jsonl` and
`weather-alerts/*.jsonl`. The query processes all available files then exits;
pass `available_now=False` to keep watching. `run_stream()` returns the query
immediately; its caller owns waiting/stopping and the Spark session lifecycle.

GEORISK_SPARK_OUTPUT_DIR defaults to `output/spark` and
GEORISK_SPARK_CHECKPOINT_DIR to `output/spark/checkpoints`. Risk and density use
the existing `curated/<dataset>/event_date=YYYY-MM-DD/` lake layout and can be
read with `lake.read_dataset()`. Invalid telemetry and alert revisions go under
`quarantine/` with a reason. Alerts are reloaded at each new telemetry batch;
alert-only arrivals do not rewrite historical risk outputs.

Risk processing deduplicates event IDs within each batch and against affected
date partitions already in the lake. Event IDs must retain their original event
date. Checkpoints under `risk/` remember consumed files. Accepted batches are
snapshotted under `georisk-batches/` before sink writes so retries use the same
accepted records and hazard results. Keep the checkpoint with its output and
use one writer per output root. Do not modify consumed input files in place.

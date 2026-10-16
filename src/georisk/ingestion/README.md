# Member 1 - Ingestion and event contracts

`georisk generate` writes the configured timed scenario to `GEORISK_INPUT_DIR`.
`georisk replay --speed 60` reads those JSONL files and uses `FileStreamPublisher`
to append batches under `GEORISK_STREAM_DIR`; `--speed 0` disables pacing.
Use `--vessel-count`, `--duration-minutes`, and `--interval-seconds` when generating.
Flags override shell settings and the selected `.env` file.

Own synthetic event generation, repeatable replay, validation, and publishing to two Amazon Kinesis Data Streams: `vessel-telemetry` and `weather-alerts`.

The demo geography is offshore Chennai in the Bay of Bengal. Shared coordinates,
lane paths, nautical-mile distances, and bearings live in `georisk.geo_demo`.
Vessels alternate inner and outer lanes, start at seeded fractions along them,
and move by heading and speed. A seeded initial position jitter of ±0.002°
provides track variation without adding artificial velocity noise. Vessels
advance through waypoints and stop at the destination.

## Starter modules

- `generator.py`: deterministic telemetry and alert generation.
- `kinesis_producer.py`: publish versioned events to Kinesis.

Use the schemas in `src/georisk/schemas/v1/`. Preserve event IDs and UTC event timestamps. `generate_scenario()` (used by the CLI) and the legacy `generate_events()` return deterministic `telemetry` and `weather_alerts` lists. `replay_events()` provides bounded replay with clear generated, published, and rejected counters; `max_events` bounds the records read, valid or not. `write_jsonl()` is the local sink for development and fixtures; it rejects NaN/Infinity so quarantine output stays valid JSON.

Telemetry IDs are `telemetry-s{seed}-{vessel:05d}-{YYYYMMDDTHHMMSSZ}` in both
generators. `generate_events()` alerts use `alert-event-s{seed}-{index:04d}`;
scenario alerts use `alert-event-{alert_id}`. Different seeds never collide and
reruns reproduce the same IDs.

The two stream names are `vessel-telemetry` and `weather-alerts`. The Kinesis
adapter validates an event before publishing and uses vessel ID or alert ID as
the default partition key. It accepts an injected client so AWS is not needed
for local tests.

Example local fixture generation:

```python
from georisk.ingestion.generator import write_jsonl
from georisk.ingestion.scenario import ScenarioConfig, generate_scenario

events = generate_scenario(ScenarioConfig(seed=7, vessel_count=10))
stats = write_jsonl(events, "output/data")
```

## Handoff

Give Member 2 a replayable stream and fixture events that follow the checked-in contracts. Agree how hazard validity and polygon coordinates are represented before publishing.

Replay validates each event, counts invalid events as rejected, and optionally calls on_invalid(stream_name, event, exc). Invalid events do not abort replay. A publisher failure rejects only that record; after the batch, `ReplayError` (a `RuntimeError` subclass) is raised with the final counters in `.stats`. The JSONL sink writes rejected records and validation errors to quarantine.jsonl. Both paths maintain generated = published + rejected. `georisk replay` skips malformed JSON lines, reports them on stderr, and counts them as generated and rejected (exit 1).

Scenario telemetry and alerts are each sorted by `(event_time, event_id)`;
legacy `generate_events()` telemetry is sorted by `(event_time, vessel_id)`.
Either way, replay input follows event-time order.

`generate_scenario(ScenarioConfig())` generates 20 vessels over 120 minutes at
300-second intervals. Eight slow vessels start around the chokepoint at 6 knots
(capped by the total vessel count). H1 covers the chokepoint from minute 0 to 150;
H2 covers part of the outer lane from minute 30 to 90. Validity is half-open.
`iter_events_by_time()` merges streams chronologically, with alerts before
telemetry at equal timestamps and event IDs breaking ties. Replay and JSONL
writing use this order; malformed timestamps are sorted last and quarantined.

Replay accepts `speed_factor` (0 or `None` disables pacing) and an injectable
`sleep` function. Positive gaps wait `min(5, event_gap_seconds / speed_factor)`.
`GEORISK_REPLAY_SPEED_FACTOR=0` and `GEORISK_STREAM_DIR=output/stream` are the
configuration defaults. File batches are written to hidden temporary files,
fsynced, then atomically renamed; a failed rename removes the temporary file.
Closing the publisher flushes partial batches. Use
one publisher per root; restarting continues after existing batch numbers.

```python
from georisk.config import GeoRiskConfig
from georisk.ingestion.file_stream import FileStreamPublisher
from georisk.ingestion.generator import replay_events
from georisk.ingestion.scenario import ScenarioConfig, generate_scenario

settings = GeoRiskConfig.from_env()
with FileStreamPublisher(settings.stream_dir) as publish:
    replay_events(
        generate_scenario(ScenarioConfig()), publish, speed_factor=settings.replay_speed_factor
    )
```

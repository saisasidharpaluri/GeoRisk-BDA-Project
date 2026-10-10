# Member 1 - Ingestion and event contracts

Own synthetic event generation, repeatable replay, validation, and publishing to two Amazon Kinesis Data Streams: `vessel-telemetry` and `weather-alerts`.

## Starter modules

- `generator.py`: deterministic telemetry and alert generation.
- `kinesis_producer.py`: publish versioned events to Kinesis.

Use the schemas in `schemas/v1/`. Preserve event IDs and UTC event timestamps. `generate_events()` returns deterministic `telemetry` and `weather_alerts` lists. `replay_events()` provides bounded replay with clear generated, published, and rejected counters. `write_jsonl()` is the local sink for development and fixtures.

The two stream names are `vessel-telemetry` and `weather-alerts`. The Kinesis
adapter validates an event before publishing and uses vessel ID or alert ID as
the default partition key. It accepts an injected client so AWS is not needed
for local tests.

Example local fixture generation:

```python
from georisk.ingestion.generator import generate_events, write_jsonl

events = generate_events(seed=7, vessel_count=10)
stats = write_jsonl(events, "output/data")
```

## Handoff

Give Member 2 a replayable stream and fixture events that follow the checked-in contracts. Agree how hazard validity and polygon coordinates are represented before publishing.

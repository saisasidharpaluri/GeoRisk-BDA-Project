# Member 1 - Ingestion and event contracts

Own synthetic event generation, repeatable replay, validation, and publishing to two Amazon Kinesis Data Streams: `vessel-telemetry` and `weather-alerts`.

## Starter modules

- `generator.py`: deterministic telemetry and alert generation.
- `kinesis_producer.py`: publish versioned events to Kinesis.

Use the schemas in `schemas/v1/`. Preserve event IDs and UTC event timestamps. Add bounded replay controls and clear counters for generated, published, and rejected records.

## Handoff

Give Member 2 a replayable stream and fixture events that follow the checked-in contracts. Agree how hazard validity and polygon coordinates are represented before publishing.

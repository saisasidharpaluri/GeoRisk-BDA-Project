# 0004: Quarantine events older than the watermark

## Context
Out-of-order telemetry must have explicit, reproducible acceptance semantics.

## Decision
Use `max_event_time - allowed_lateness` with a default lateness of ten minutes.
Accept the boundary; quarantine strictly older valid, unique telemetry with
`reason=late_event`. Invalid or duplicate events do not advance the maximum.
Local batches advance in arrival order, then sort accepted outputs by event
time. Their watermark resets per invocation. Spark compares against the
previous microbatch maximum and persists it after successful sink writes.
Telemetry more than `max_future_minutes` (default 60) after the processing
wall clock is quarantined as `future_event` before the watermark check, so a
bad clock cannot advance the maximum and mark later valid data as late.

## Consequences
Late records do not affect risk or density, even with strict validation enabled.
Unordered records within a Spark microbatch can differ from per-record local
processing. This is application-managed watermark state, not a claim of
native Spark state eviction. Preserve checkpoints with output; snapshots and
density keys have no automatic retention cleanup. See the
[processing README](../../src/georisk/processing/README.md) for recovery limits.

## Status
Accepted; `GEORISK_ALLOWED_LATENESS_MINUTES=10`, configurable from 0 to 1440;
`GEORISK_MAX_FUTURE_MINUTES=60`, configurable from 0 to 10080.

"""Deterministic synthetic events and bounded local replay."""

from __future__ import annotations

import json
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping

from georisk.contracts import SCHEMA_VERSION, validate_event

UTC = timezone.utc
DEFAULT_START_TIME = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
DEFAULT_TELEMETRY_POINTS = 3
DEFAULT_INTERVAL_SECONDS = 300
MAX_VESSELS = 10_000
MAX_TELEMETRY_POINTS = 1_000

Event = dict[str, Any]
PublishFunction = Callable[[str, Event], Any]


@dataclass(frozen=True)
class ReplayStats:
    """Counters returned by a bounded event replay."""

    generated: int
    published: int
    rejected: int


def _utc_timestamp(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("start_time must be timezone-aware UTC")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _polygon(center_latitude: float, center_longitude: float, size: float) -> dict:
    coordinates = [
        [
            [center_longitude - size, center_latitude - size],
            [center_longitude + size, center_latitude - size],
            [center_longitude + size, center_latitude + size],
            [center_longitude - size, center_latitude + size],
            [center_longitude - size, center_latitude - size],
        ]
    ]
    return {"type": "Polygon", "coordinates": coordinates}


def _weather_alert(
    rng: random.Random, index: int, start_time: datetime
) -> Event:
    center_latitude = 12.0 + rng.uniform(-0.15, 0.15)
    center_longitude = 78.0 + rng.uniform(-0.15, 0.15)
    valid_from = start_time - timedelta(minutes=5)
    valid_until = start_time + timedelta(minutes=40)
    event_time = valid_from
    return {
        "schema_version": SCHEMA_VERSION,
        "event_id": f"alert-event-{index:04d}",
        "alert_id": f"alert-{index:04d}",
        "event_time": _utc_timestamp(event_time),
        "valid_from": _utc_timestamp(valid_from),
        "valid_until": _utc_timestamp(valid_until),
        "hazard_type": ["high_waves", "strong_wind", "storm"][index % 3],
        "severity": rng.randint(2, 5),
        "geometry": _polygon(center_latitude, center_longitude, 0.18),
    }


def generate_events(
    seed: int,
    vessel_count: int,
    *,
    telemetry_points: int = DEFAULT_TELEMETRY_POINTS,
    interval_seconds: int = DEFAULT_INTERVAL_SECONDS,
    start_time: datetime = DEFAULT_START_TIME,
) -> dict[str, list[Event]]:
    """Generate deterministic, schema-valid telemetry and hazard events.

    Vessel tracks are clustered around the generated hazard area so the next
    processing phase has useful positive and negative risk cases.
    """
    if not isinstance(seed, int):
        raise TypeError("seed must be an integer")
    if not isinstance(vessel_count, int) or isinstance(vessel_count, bool):
        raise TypeError("vessel_count must be an integer")
    if not 0 <= vessel_count <= MAX_VESSELS:
        raise ValueError(f"vessel_count must be between 0 and {MAX_VESSELS}")
    if not isinstance(telemetry_points, int) or isinstance(telemetry_points, bool):
        raise TypeError("telemetry_points must be an integer")
    if not 1 <= telemetry_points <= MAX_TELEMETRY_POINTS:
        raise ValueError(f"telemetry_points must be between 1 and {MAX_TELEMETRY_POINTS}")
    if not isinstance(interval_seconds, int) or isinstance(interval_seconds, bool):
        raise TypeError("interval_seconds must be an integer")
    if interval_seconds <= 0:
        raise ValueError("interval_seconds must be greater than zero")
    if start_time.tzinfo is None or start_time.utcoffset() != timedelta(0):
        raise ValueError("start_time must be timezone-aware UTC")

    rng = random.Random(seed)
    alerts = [_weather_alert(rng, index, start_time) for index in range(2)]
    telemetry: list[Event] = []

    for vessel_index in range(vessel_count):
        latitude = 12.0 + rng.uniform(-0.5, 0.5)
        longitude = 78.0 + rng.uniform(-0.5, 0.5)
        speed = rng.uniform(8.0, 22.0)
        heading = rng.uniform(0.0, 359.999)
        for point_index in range(telemetry_points):
            event_time = start_time + timedelta(
                seconds=interval_seconds * point_index
            )
            event = {
                "schema_version": SCHEMA_VERSION,
                "event_id": f"telemetry-{vessel_index:05d}-{point_index:04d}",
                "event_time": _utc_timestamp(event_time),
                "vessel_id": f"vessel-{vessel_index:05d}",
                "latitude": round(latitude, 6),
                "longitude": round(longitude, 6),
                "speed_knots": round(speed, 3),
                "heading_deg": round(heading, 3),
            }
            validate_event("vessel_telemetry", event)
            telemetry.append(event)
            latitude = max(-90.0, min(90.0, latitude + rng.uniform(-0.025, 0.025)))
            longitude = max(-180.0, min(180.0, longitude + rng.uniform(-0.025, 0.025)))
            heading = (heading + rng.uniform(-8.0, 8.0)) % 360

    for alert in alerts:
        validate_event("weather_alert", alert)
    return {"telemetry": telemetry, "weather_alerts": alerts}


def iter_events(events: Mapping[str, Iterable[Event]]) -> Iterable[tuple[str, Event]]:
    """Yield events in deterministic stream order."""
    for stream_key, contract in (
        ("weather_alerts", "weather_alert"),
        ("telemetry", "vessel_telemetry"),
    ):
        records = events.get(stream_key)
        if records is None:
            raise ValueError(f"events must contain '{stream_key}'")
        for event in records:
            validate_event(contract, event)
            yield stream_key, dict(event)


def replay_events(
    events: Mapping[str, Iterable[Event]],
    publish: PublishFunction,
    *,
    max_events: int | None = None,
) -> ReplayStats:
    """Publish at most ``max_events`` valid events and count failures.

    A publisher exception rejects only the current record; the exception is
    re-raised after counting so callers cannot mistake a partial replay for
    success.
    """
    if max_events is not None and (
        not isinstance(max_events, int) or isinstance(max_events, bool) or max_events < 0
    ):
        raise ValueError("max_events must be a non-negative integer or None")

    generated = published = rejected = 0
    first_error: Exception | None = None
    for stream_key, event in iter_events(events):
        if max_events is not None and generated >= max_events:
            break
        generated += 1
        stream_name = (
            "weather-alerts" if stream_key == "weather_alerts" else "vessel-telemetry"
        )
        try:
            publish(stream_name, event)
        except Exception as exc:
            rejected += 1
            if first_error is None:
                first_error = exc
        else:
            published += 1
    if first_error is not None:
        raise RuntimeError(
            f"Replay failed for {rejected} of {generated} generated events"
        ) from first_error
    return ReplayStats(generated, published, rejected)


def write_jsonl(events: Mapping[str, Iterable[Event]], output_dir: str | Path) -> ReplayStats:
    """Write validated stream fixtures to separate JSONL files."""
    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)
    paths = {
        "weather_alerts": destination / "weather-alerts.jsonl",
        "telemetry": destination / "vessel-telemetry.jsonl",
    }
    counts = {key: 0 for key in paths}
    handles = {
        key: path.open("w", encoding="utf-8") for key, path in paths.items()
    }
    try:
        for stream_key, event in iter_events(events):
            handles[stream_key].write(json.dumps(event, sort_keys=True) + "\n")
            counts[stream_key] += 1
    finally:
        for handle in handles.values():
            handle.close()
    return ReplayStats(
        generated=counts["weather_alerts"] + counts["telemetry"],
        published=counts["weather_alerts"] + counts["telemetry"],
        rejected=0,
    )

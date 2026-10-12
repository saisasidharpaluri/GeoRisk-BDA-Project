"""Deterministic synthetic events and bounded local replay."""

from __future__ import annotations

import json
import math
import random
from contextlib import ExitStack
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping

from georisk.contracts import ContractValidationError, SCHEMA_VERSION, validate_event
from georisk.geo_demo import DEMO_NODES, DEMO_PATHS, haversine_nm, initial_bearing_deg

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
    center_latitude = DEMO_NODES["chokepoint"][0] + rng.uniform(-0.15, 0.15)
    center_longitude = DEMO_NODES["chokepoint"][1] + rng.uniform(-0.15, 0.15)
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


def _vessel_track(
    rng: random.Random, vessel_index: int, points: int, interval_seconds: int,
    start_time: datetime, *, position: tuple[float, float] | None = None,
    speed_knots: float | None = None,
) -> list[Event]:
    """Move one vessel along a lane, carrying excess distance across waypoints."""
    path = DEMO_PATHS["inner" if position is not None or vessel_index % 2 == 0 else "outer"]
    waypoints = [DEMO_NODES[node] for node in path]
    target_index = 1
    if position is None:
        lengths = [haversine_nm(*a, *b) for a, b in zip(waypoints, waypoints[1:])]
        # Leave the short port approach outside the offshore telemetry box.
        distance = rng.uniform(0.06, 0.95) * sum(lengths)
        while distance > lengths[target_index - 1]:
            distance -= lengths[target_index - 1]
            target_index += 1
        a, b = waypoints[target_index - 1:target_index + 1]
        fraction = distance / lengths[target_index - 1]
        position = (a[0] + fraction * (b[0] - a[0]), a[1] + fraction * (b[1] - a[1]))
    latitude = position[0] + rng.uniform(-0.002, 0.002)
    longitude = position[1] + rng.uniform(-0.002, 0.002)
    speed = rng.uniform(8.0, 22.0) if speed_knots is None else speed_knots
    records = []
    for point_index in range(points):
        if target_index < len(waypoints) - 1 and haversine_nm(
            latitude, longitude, *waypoints[target_index]
        ) < 1:
            target_index += 1
        heading = initial_bearing_deg(latitude, longitude, *waypoints[target_index])
        event = {
            "schema_version": SCHEMA_VERSION,
            "event_id": f"telemetry-{vessel_index:05d}-{point_index:04d}",
            "event_time": _utc_timestamp(start_time + timedelta(seconds=interval_seconds * point_index)),
            "vessel_id": f"vessel-{vessel_index:05d}",
            "latitude": round(latitude, 6), "longitude": round(longitude, 6),
            "speed_knots": round(speed, 3), "heading_deg": round(heading, 3) % 360,
        }
        validate_event("vessel_telemetry", event)
        records.append(event)
        remaining = speed * interval_seconds / 3600
        while remaining > 0:
            target = waypoints[target_index]
            distance = haversine_nm(latitude, longitude, *target)
            if remaining >= distance:
                latitude, longitude = target
                remaining -= distance
                if target_index == len(waypoints) - 1:
                    speed = 0.0
                    break
                target_index += 1
            else:
                heading = math.radians(initial_bearing_deg(latitude, longitude, *target))
                longitude += remaining * math.sin(heading) / (60 * math.cos(math.radians(latitude)))
                latitude += remaining * math.cos(heading) / 60
                break
    return records


def generate_events(
    seed: int,
    vessel_count: int,
    *,
    telemetry_points: int = DEFAULT_TELEMETRY_POINTS,
    interval_seconds: int = DEFAULT_INTERVAL_SECONDS,
    start_time: datetime = DEFAULT_START_TIME,
) -> dict[str, list[Event]]:
    """Generate deterministic, schema-valid telemetry and hazard events.

    Vessel tracks alternate between inner and outer lanes at seeded speeds.
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
        telemetry.extend(_vessel_track(
            rng, vessel_index, telemetry_points, interval_seconds, start_time
        ))

    for alert in alerts:
        validate_event("weather_alert", alert)
    telemetry.sort(key=lambda event: (event["event_time"], event["vessel_id"]))
    return {"telemetry": telemetry, "weather_alerts": alerts}


def iter_events(
    events: Mapping[str, Iterable[Event]], validate: bool = True
) -> Iterable[tuple[str, Event]]:
    """Yield events in deterministic stream order."""
    for stream_key, contract in (
        ("weather_alerts", "weather_alert"),
        ("telemetry", "vessel_telemetry"),
    ):
        records = events.get(stream_key)
        if records is None:
            raise ValueError(f"events must contain '{stream_key}'")
        for event in records:
            if validate:
                validate_event(contract, event)
            yield stream_key, dict(event) if isinstance(event, Mapping) else event


def iter_events_by_time(
    events: Mapping[str, Iterable[Event]], validate: bool = True,
) -> Iterable[tuple[str, Event]]:
    """Yield by UTC time, alerts first at ties, then event ID.

    Invalid timestamps sort last when validation is disabled, allowing replay
    to quarantine malformed records without disrupting valid event order.
    """
    def key(item: tuple[str, Event]) -> tuple[datetime, int, str]:
        stream, event = item
        timestamp = datetime.max.replace(tzinfo=UTC)
        event_id = ""
        if isinstance(event, Mapping):
            event_id = str(event.get("event_id", ""))
            try:
                parsed = datetime.fromisoformat(event["event_time"].replace("Z", "+00:00"))
                if parsed.tzinfo is not None:
                    timestamp = parsed.astimezone(UTC)
            except (KeyError, TypeError, ValueError, AttributeError):
                pass
        return timestamp, 0 if stream == "weather_alerts" else 1, event_id

    yield from sorted(iter_events(events, validate=validate), key=key)


def replay_events(
    events: Mapping[str, Iterable[Event]],
    publish: PublishFunction,
    *,
    max_events: int | None = None,
    on_invalid: Callable[[str, Event, Exception], Any] | None = None,
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
    for stream_key, event in iter_events_by_time(events, validate=False):
        if max_events is not None and generated >= max_events:
            break
        generated += 1
        stream_name = (
            "weather-alerts" if stream_key == "weather_alerts" else "vessel-telemetry"
        )
        contract = "weather_alert" if stream_key == "weather_alerts" else "vessel_telemetry"
        try:
            validate_event(contract, event)
        except (ContractValidationError, TypeError) as exc:
            rejected += 1
            if on_invalid is not None:
                on_invalid(stream_name, event, exc)
            continue
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
    with ExitStack() as stack:
        handles = {
            stream: stack.enter_context((destination / f"{stream}.jsonl").open("w", encoding="utf-8"))
            for stream in ("weather-alerts", "vessel-telemetry", "quarantine")
        }

        def publish(stream: str, event: Event) -> None:
            handles[stream].write(json.dumps(event, sort_keys=True) + "\n")

        def quarantine(stream: str, event: Event, exc: Exception) -> None:
            publish("quarantine", {
                "stream": stream,
                "errors": exc.errors if isinstance(exc, ContractValidationError) else [str(exc)],
                "record": event,
            })

        return replay_events(events, publish, on_invalid=quarantine)

"""Telemetry-derived delay features with reproducible synthetic labels."""

from __future__ import annotations

import math
import random
from collections.abc import Iterable, Mapping
from typing import Any

from georisk.geo_demo import haversine_nm
from georisk.processing.stream_job import _parse_utc, _window_start
from georisk.storage_analytics.delay_model import make_synthetic_label


def build_delay_rows(
    telemetry: Iterable[Mapping[str, Any]],
    risk_events: Iterable[Mapping[str, Any]],
    density_metrics: Iterable[Mapping[str, Any]],
    *,
    destination: tuple[float, float],
    window_minutes: int,
    seed: int,
    noise_std_hours: float = 0.25,
) -> list[dict[str, Any]]:
    """Join risks to telemetry and cell/window density; destination is lat/lon."""
    if (not isinstance(window_minutes, int) or isinstance(window_minutes, bool)
            or not 1 <= window_minutes <= 1440 or 1440 % window_minutes != 0):
        raise ValueError("window_minutes must be 1..1440 and divide 1440")
    if (not isinstance(noise_std_hours, (int, float)) or isinstance(noise_std_hours, bool)
            or not math.isfinite(noise_std_hours) or noise_std_hours < 0):
        raise ValueError("noise_std_hours must be a non-negative finite number")
    telemetry_by_id = {}
    for event in telemetry:
        telemetry_by_id.setdefault(event["event_id"], event)
    density_by_window_cell = {
        (_parse_utc(row["window_start"]), row["h3_index"]): row["unique_vessel_count"]
        for row in density_metrics
    }
    rng = random.Random(seed)
    rows = []
    for event in risk_events:
        event_id = event.get("event_id")
        if event_id not in telemetry_by_id:
            raise ValueError(f"risk event has no matching telemetry event_id: {event_id!r}")
        position = telemetry_by_id[event_id]
        speed = max(1.0, float(position["speed_knots"]))
        distance = haversine_nm(
            float(position["latitude"]), float(position["longitude"]), *destination,
        )
        window = _window_start(_parse_utc(event["event_time"]), window_minutes)
        row = {
            "event_id": event_id,
            "vessel_id": event["vessel_id"],
            "event_time": event["event_time"],
            "speed_knots": speed,
            "route_distance_nm": distance,
            "baseline_hours": distance / speed,
            "density": density_by_window_cell.get((window, event["h3_index"]), 0),
            "hazard_severity": max(
                (hazard["severity"] for hazard in event["matching_hazards"]), default=0,
            ),
        }
        row["delay_hours"] = make_synthetic_label(row, rng.gauss(0, noise_std_hours))
        rows.append(row)
    return rows

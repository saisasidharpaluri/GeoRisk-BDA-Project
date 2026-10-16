"""Deterministic multi-window traffic and timed hazard scenarios."""

from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import timedelta

from georisk.contracts import SCHEMA_VERSION, validate_event
from georisk.geo_demo import DEMO_NODES
from georisk.ingestion.generator import (
    DEFAULT_START_TIME,
    MAX_TELEMETRY_POINTS,
    MAX_VESSELS,
    Event,
    _polygon,
    _utc_timestamp,
    _vessel_track,
)


@dataclass(frozen=True)
class HazardSpec:
    """A square synthetic hazard with a half-open validity interval."""

    alert_id: str
    hazard_type: str
    severity: int
    center_lat: float
    center_lon: float
    half_size_deg: float
    appear_minute: int
    expire_minute: int

    def __post_init__(self) -> None:
        if self.half_size_deg <= 0:
            raise ValueError("half_size_deg must be positive")
        if not 0 <= self.appear_minute < self.expire_minute:
            raise ValueError("hazard must expire after its non-negative appear_minute")


DEFAULT_HAZARDS = (
    HazardSpec("H1", "storm", 4, 13.10, 81.20, 0.12, 0, 150),
    HazardSpec("H2", "high_waves", 3, 13.45, 80.93, 0.08, 30, 90),
)


@dataclass(frozen=True)
class ScenarioConfig:
    """Bounded scenario settings; congestion is capped by vessel_count."""

    seed: int = 7
    vessel_count: int = 20
    duration_minutes: int = 120
    interval_seconds: int = 300
    congestion_vessels: int = 8
    hazards: tuple[HazardSpec, ...] = DEFAULT_HAZARDS

    def __post_init__(self) -> None:
        for name in (
            "seed",
            "vessel_count",
            "duration_minutes",
            "interval_seconds",
            "congestion_vessels",
        ):
            value = getattr(self, name)
            if not isinstance(value, int) or isinstance(value, bool):
                raise TypeError(f"{name} must be an integer")
        if not 0 <= self.vessel_count <= MAX_VESSELS:
            raise ValueError(f"vessel_count must be between 0 and {MAX_VESSELS}")
        if self.congestion_vessels < 0:
            raise ValueError("congestion_vessels must be non-negative")
        if self.duration_minutes <= 0 or self.interval_seconds <= 0:
            raise ValueError("duration_minutes and interval_seconds must be positive")
        if not 1 <= self.duration_minutes * 60 // self.interval_seconds <= MAX_TELEMETRY_POINTS:
            raise ValueError(f"scenario must contain 1..{MAX_TELEMETRY_POINTS} telemetry steps")
        if not isinstance(self.hazards, tuple) or not all(
            isinstance(h, HazardSpec) for h in self.hazards
        ):
            raise TypeError("hazards must be a tuple of HazardSpec values")
        if len({hazard.alert_id for hazard in self.hazards}) != len(self.hazards):
            raise ValueError("hazard alert_ids must be unique")


def generate_scenario(config: ScenarioConfig) -> dict[str, list[Event]]:
    """Generate lane traffic, a slow convoy, and appearing/expiring hazards."""
    rng = random.Random(config.seed)
    telemetry: list[Event] = []
    for index in range(config.vessel_count):
        congested = index < config.congestion_vessels
        position = None
        if congested:
            lat, lon = DEMO_NODES["chokepoint"]
            position = (lat + rng.uniform(-0.008, 0.008), lon + rng.uniform(-0.008, 0.008))
        telemetry.extend(
            _vessel_track(
                rng,
                index,
                config.duration_minutes * 60 // config.interval_seconds,
                config.interval_seconds,
                DEFAULT_START_TIME,
                position=position,
                speed_knots=6.0 if congested else None,
                seed=config.seed,
            )
        )
    alerts = []
    for hazard in config.hazards:
        valid_from = _utc_timestamp(DEFAULT_START_TIME + timedelta(minutes=hazard.appear_minute))
        alert = {
            "schema_version": SCHEMA_VERSION,
            "event_id": f"alert-event-{hazard.alert_id}",
            "alert_id": hazard.alert_id,
            "event_time": valid_from,
            "valid_from": valid_from,
            "valid_until": _utc_timestamp(
                DEFAULT_START_TIME + timedelta(minutes=hazard.expire_minute)
            ),
            "hazard_type": hazard.hazard_type,
            "severity": hazard.severity,
            "geometry": _polygon(hazard.center_lat, hazard.center_lon, hazard.half_size_deg),
        }
        validate_event("weather_alert", alert)
        alerts.append(alert)
    telemetry.sort(key=lambda row: (row["event_time"], row["event_id"]))
    alerts.sort(key=lambda row: (row["event_time"], row["event_id"]))
    return {"telemetry": telemetry, "weather_alerts": alerts}

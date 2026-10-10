"""Local event-time spatial processing core for the Phase 3 pipeline."""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

from georisk.contracts import SCHEMA_VERSION, validate_event
from georisk.processing.h3_index import DEFAULT_RESOLUTION, point_to_cell, polygon_to_cells

UTC = timezone.utc
WINDOW_MINUTES = 15


def _parse_utc(value: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError("timestamp must be a string")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"invalid timestamp: {value}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise ValueError("timestamps must be timezone-aware UTC")
    return parsed.astimezone(UTC)


def _timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _window_start(value: datetime) -> datetime:
    value = value.astimezone(UTC)
    return value.replace(
        minute=value.minute - value.minute % WINDOW_MINUTES,
        second=0,
        microsecond=0,
    )


def _hazard_cells(alert: Mapping[str, Any], resolution: int) -> set[str]:
    return polygon_to_cells(alert["geometry"], resolution)


def _matching_hazards(
    cell: str,
    event_time: datetime,
    alerts: Iterable[Mapping[str, Any]],
    resolution: int,
) -> list[dict[str, Any]]:
    matches = []
    for alert in alerts:
        valid_from = _parse_utc(alert["valid_from"])
        valid_until = _parse_utc(alert["valid_until"])
        if valid_from <= event_time < valid_until and cell in _hazard_cells(
            alert, resolution
        ):
            matches.append(
                {
                    "alert_id": alert["alert_id"],
                    "hazard_type": alert["hazard_type"],
                    "severity": alert["severity"],
                    "valid_from": alert["valid_from"],
                    "valid_until": alert["valid_until"],
                }
            )
    return sorted(matches, key=lambda match: match["alert_id"])


def process_events(
    telemetry: Iterable[Mapping[str, Any]],
    alerts: Iterable[Mapping[str, Any]],
    *,
    resolution: int = DEFAULT_RESOLUTION,
) -> dict[str, list[dict[str, Any]]]:
    """Create risk events and unique-vessel density metrics from one batch."""
    alert_records = [dict(alert) for alert in alerts]
    telemetry_records = [dict(event) for event in telemetry]
    for alert in alert_records:
        validate_event("weather_alert", alert)
        if _parse_utc(alert["valid_until"]) <= _parse_utc(alert["valid_from"]):
            raise ValueError(f"alert {alert['alert_id']} has an invalid validity range")

    risk_events = []
    density_keys: set[tuple[datetime, datetime, str, str]] = set()
    for event in telemetry_records:
        validate_event("vessel_telemetry", event)
        event_time = _parse_utc(event["event_time"])
        cell = point_to_cell(event["latitude"], event["longitude"], resolution)
        matches = _matching_hazards(cell, event_time, alert_records, resolution)
        risk_events.append(
            {
                "schema_version": SCHEMA_VERSION,
                "event_time": event["event_time"],
                "vessel_id": event["vessel_id"],
                "h3_index": cell,
                "h3_resolution": resolution,
                "risk_alert": bool(matches),
                "matching_hazards": matches,
            }
        )
        window_start = _window_start(event_time)
        density_keys.add(
            (window_start, window_start + timedelta(minutes=WINDOW_MINUTES), cell, event["vessel_id"])
        )

    density_vessels: defaultdict[tuple[datetime, datetime, str], set[str]] = defaultdict(set)
    for window_start, window_end, cell, vessel_id in density_keys:
        density_vessels[(window_start, window_end, cell)].add(vessel_id)
    density_metrics = [
        {
            "schema_version": SCHEMA_VERSION,
            "window_start": _timestamp(window_start),
            "window_end": _timestamp(window_end),
            "h3_index": cell,
            "h3_resolution": resolution,
            "unique_vessel_count": len(vessels),
        }
        for (window_start, window_end, cell), vessels in sorted(density_vessels.items())
    ]
    for event in risk_events:
        validate_event("risk_event", event)
    for metric in density_metrics:
        validate_event("density_metric", metric)
    return {"risk_events": risk_events, "density_metrics": density_metrics}


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    records = []
    with path.open(encoding="utf-8") as source:
        for line_number, line in enumerate(source, 1):
            if not line.strip():
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"invalid JSON in {path}:{line_number}") from exc
            if not isinstance(record, dict):
                raise ValueError(f"expected an object in {path}:{line_number}")
            records.append(record)
    return records


def _write_jsonl(path: Path, records: Iterable[Mapping[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as output:
        for record in records:
            output.write(json.dumps(record, sort_keys=True) + "\n")


def run_streaming_job(config: Mapping[str, Any]) -> dict[str, list[dict[str, Any]]]:
    """Run the local batch-compatible processing path.

    The config accepts either in-memory ``telemetry``/``weather_alerts`` lists
    or an ``input_dir`` containing Phase 2 JSONL files. Results are returned
    and optionally written to ``output_dir`` as JSONL.
    """
    if not isinstance(config, Mapping):
        raise TypeError("config must be a mapping")
    resolution = config.get("resolution", DEFAULT_RESOLUTION)
    if "telemetry" in config or "weather_alerts" in config:
        telemetry = config.get("telemetry", [])
        alerts = config.get("weather_alerts", [])
    else:
        input_dir = config.get("input_dir")
        if not input_dir:
            raise ValueError("config requires telemetry/weather_alerts or input_dir")
        directory = Path(input_dir)
        telemetry = _read_jsonl(directory / "vessel-telemetry.jsonl")
        alerts = _read_jsonl(directory / "weather-alerts.jsonl")
    results = process_events(telemetry, alerts, resolution=resolution)
    if config.get("output_dir"):
        output_dir = Path(config["output_dir"])
        output_dir.mkdir(parents=True, exist_ok=True)
        _write_jsonl(output_dir / "risk-events.jsonl", results["risk_events"])
        _write_jsonl(output_dir / "density-metrics.jsonl", results["density_metrics"])
    return results

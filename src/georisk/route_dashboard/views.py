"""View helpers that shape pipeline outputs for the dashboard, and cached file readers."""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from georisk.contracts import parse_utc_timestamp
from georisk.geo_demo import DEMO_NODES
from georisk.processing.stream_job import alert_revisions

ROUTE_SCENARIOS = ("As generated", "No hazards", "Close inner lane")
# A live run whose metadata has not been updated for this long is treated as stopped.
LIVE_STALL_SECONDS = 120

# Streamlit re-executes the dashboard script on every rerun, so its own globals
# reset; these caches live here and survive reruns. Entries are reused only
# while the files they were read from are unchanged.
_JSONL_CACHE: dict[Path, tuple[int, int, list[dict[str, Any]]]] = {}
_HISTORY_CACHE: dict[str, tuple[tuple[tuple[str, int, int], ...], list[dict[str, Any]]]] = {}


def read_jsonl(path: str | Path) -> list[dict[str, Any]]:
    """Read a JSONL file of objects; a missing file is empty.

    Parsed rows are cached until the file's size or modification time changes.
    Callers receive a new list but share the row dicts, which the dashboard
    never modifies.
    """
    target = Path(path)
    try:
        stat = target.stat()
    except FileNotFoundError:
        return []
    cached = _JSONL_CACHE.get(target)
    if cached is not None and cached[:2] == (stat.st_mtime_ns, stat.st_size):
        return list(cached[2])
    rows = []
    with target.open(encoding="utf-8") as source:
        for line_number, line in enumerate(source, 1):
            if not line.strip():
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"invalid JSON in {target}:{line_number}") from exc
            if not isinstance(record, dict):
                raise ValueError(f"expected an object in {target}:{line_number}")
            rows.append(record)
    _JSONL_CACHE[target] = (stat.st_mtime_ns, stat.st_size, rows)
    return list(rows)


def _utc(value: str | datetime) -> datetime:
    if isinstance(value, str):
        return parse_utc_timestamp(value)
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError("timestamps must be timezone-aware")
    return value.astimezone(timezone.utc)


def latest_timestamp(values: Iterable[str]) -> str | None:
    """Return the chronologically latest timestamp string, or ``None`` if empty.

    Comparison uses parsed instants, so mixed ``Z``/``+00:00`` spellings and
    fractional seconds order correctly.
    """
    return max(values, key=_utc, default=None)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _max_severity(event: Mapping[str, Any]) -> int:
    return max(
        (hazard.get("severity", 0) for hazard in event.get("matching_hazards") or []), default=0
    )


def active_alerts(
    alerts: Iterable[Mapping[str, Any]], at_time: str | datetime
) -> list[dict[str, Any]]:
    """Return alerts valid at ``at_time`` (half-open) using the revision current then.

    Uses the same as-of revision rule as processing and route scoring: a
    revision applies from its issue time until the next revision, and the first
    revision also covers earlier times.
    """
    timestamp = _utc(at_time)
    return sorted(
        (
            dict(revision.alert)
            for revision in alert_revisions(alerts)
            if revision.applies_at(timestamp)
        ),
        key=lambda alert: (-alert["severity"], alert["alert_id"]),
    )


def severity_distribution(risk_events: Iterable[Mapping[str, Any]]) -> dict[int, int]:
    """Count alerted events by their maximum matched hazard severity."""
    counts: dict[int, int] = {}
    for event in risk_events:
        if event.get("risk_alert"):
            severity = _max_severity(event)
            counts[severity] = counts.get(severity, 0) + 1
    return dict(sorted(counts.items()))


def filter_risk_events(
    risk_events: Iterable[Mapping[str, Any]],
    *,
    hazard_types: Iterable[str] | None = None,
    min_severity: int | None = None,
    start: str | datetime | None = None,
    end: str | datetime | None = None,
) -> list[dict[str, Any]]:
    """Filter by matched hazard type, minimum severity, and inclusive event-time range.

    ``None`` disables a filter. Hazard and severity filters keep only events
    with a qualifying matched hazard, so they exclude non-alerted positions.
    """
    types = set(hazard_types) if hazard_types is not None else None
    lower = _utc(start) if start is not None else None
    upper = _utc(end) if end is not None else None
    result = []
    for event in risk_events:
        hazards = event.get("matching_hazards") or []
        if types is not None and not any(hazard.get("hazard_type") in types for hazard in hazards):
            continue
        if min_severity is not None and _max_severity(event) < min_severity:
            continue
        timestamp = _utc(event["event_time"])
        if (lower is not None and timestamp < lower) or (upper is not None and timestamp > upper):
            continue
        result.append(dict(event))
    return result


def density_windows(density_metrics: Iterable[Mapping[str, Any]]) -> list[str]:
    """Return the sorted unique ``window_start`` values."""
    return sorted({row["window_start"] for row in density_metrics}, key=_utc)


def density_for_window(
    density_metrics: Iterable[Mapping[str, Any]],
    window_start: str,
) -> list[dict[str, Any]]:
    """Return one window's rows, busiest cell first."""
    rows = [dict(row) for row in density_metrics if row["window_start"] == window_start]
    return sorted(rows, key=lambda row: (-row["unique_vessel_count"], row["h3_index"]))


def compare_adjacent_windows(
    density_metrics: Iterable[Mapping[str, Any]],
    window_start: str,
) -> list[dict[str, Any]]:
    """Compare each cell's count with the immediately preceding and following windows.

    Neighbours are the calendar-adjacent windows of the same length, even when
    they hold no rows (a quiet window counts as 0). A neighbour outside the
    observed data range is ``None``. Unknown windows return an empty list.
    """
    rows = list(density_metrics)
    current = [row for row in rows if row["window_start"] == window_start]
    if not current:
        return []
    start = _utc(window_start)
    length = _utc(current[0]["window_end"]) - start
    observed = [_utc(row["window_start"]) for row in rows]
    first, last = min(observed), max(observed)
    neighbours = {
        "previous": start - length if start - length >= first else None,
        "current": start,
        "next": start + length if start + length <= last else None,
    }
    counts: dict[str, dict[str, int]] = {}
    for row in rows:
        row_start = _utc(row["window_start"])
        for name, window in neighbours.items():
            if window is not None and row_start == window:
                counts.setdefault(row["h3_index"], {})[name] = row["unique_vessel_count"]
    result = [
        {
            "h3_index": cell,
            **{
                name: (None if window is None else values.get(name, 0))
                for name, window in neighbours.items()
            },
        }
        for cell, values in counts.items()
    ]
    return sorted(result, key=lambda row: (-row["current"], row["h3_index"]))


def latest_positions(telemetry: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Return the last report per vessel (later rows win ties), sorted by vessel."""
    latest: dict[str, Mapping[str, Any]] = {}
    for row in telemetry:
        previous = latest.get(row["vessel_id"])
        if previous is None or _utc(row["event_time"]) >= _utc(previous["event_time"]):
            latest[row["vessel_id"]] = row
    return [dict(latest[vessel]) for vessel in sorted(latest)]


def latest_risk_by_vessel(risk_events: Iterable[Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    """Return each vessel's most recent risk event."""
    return {row["vessel_id"]: row for row in latest_positions(risk_events)}


def live_state(metadata: Mapping[str, Any] | None, now: datetime) -> dict[str, Any] | None:
    """Return a live run's progress, or ``None`` when the outputs come from a batch run.

    ``following`` is true while ``georisk live`` is still streaming or finishing
    and has updated its metadata within ``LIVE_STALL_SECONDS``; ``stalled``
    marks a run that claims to be active but has stopped updating.
    """
    live = (metadata or {}).get("live")
    if not isinstance(live, Mapping):
        return None
    active = live.get("status") in ("running", "finishing")
    try:
        quiet = (_utc(now) - _utc(live["updated_at"])).total_seconds()
    except (KeyError, TypeError, ValueError):
        quiet = None
    following = active and quiet is not None and quiet <= LIVE_STALL_SECONDS
    return {**live, "following": following, "stalled": active and not following}


def freshness(
    metadata: Mapping[str, Any] | None,
    now: datetime,
    stale_after_minutes: int,
) -> dict[str, Any]:
    """Classify the last processing run as ``fresh``, ``stale``, or ``unavailable``.

    ``age_minutes`` is wall-clock time since processing. ``processing_lag_minutes``
    is processing time minus the latest event time; seeded historical scenarios
    therefore show a large lag even when the run itself is fresh.
    """
    if (
        not isinstance(stale_after_minutes, int)
        or isinstance(stale_after_minutes, bool)
        or stale_after_minutes <= 0
    ):
        raise ValueError("stale_after_minutes must be a positive integer")
    metadata = metadata or {}
    result: dict[str, Any] = {
        "processed_at": metadata.get("processed_at"),
        "latest_event_time": metadata.get("latest_event_time"),
        "watermark": metadata.get("watermark"),
        "age_minutes": None,
        "processing_lag_minutes": None,
        "state": "unavailable",
    }
    if not result["processed_at"]:
        return result
    try:
        processed_at = _utc(result["processed_at"])
    except ValueError:
        return result
    age = max(0.0, (_utc(now) - processed_at).total_seconds() / 60)
    result["age_minutes"] = round(age, 1)
    result["state"] = "fresh" if age <= stale_after_minutes else "stale"
    if result["latest_event_time"]:
        try:
            lag = (processed_at - _utc(result["latest_event_time"])).total_seconds() / 60
        except ValueError:
            lag = None
        result["processing_lag_minutes"] = None if lag is None else round(lag, 1)
    return result


def risk_table_rows(risk_events: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Flatten risk events into display rows, newest first."""
    rows = [
        {
            "event_time": event["event_time"],
            "vessel_id": event["vessel_id"],
            "hazards": ", ".join(
                sorted(
                    {
                        f"{hazard['hazard_type']} ({hazard['alert_id']})"
                        for hazard in event.get("matching_hazards") or []
                    }
                )
            ),
            "max_severity": _max_severity(event),
            "h3_index": event.get("h3_index"),
            "coverage_mode": event.get("coverage_mode"),
            "h3_resolution": event.get("h3_resolution"),
            "latitude": event.get("latitude"),
            "longitude": event.get("longitude"),
        }
        for event in risk_events
    ]
    return sorted(rows, key=lambda row: (_utc(row["event_time"]), row["vessel_id"]), reverse=True)


def latest_delay_estimates(estimates: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Return each vessel's latest estimate with ``error_hours`` = predicted − label."""
    return [
        {
            "vessel_id": row["vessel_id"],
            "event_time": row["event_time"],
            "predicted_delay_hours": row["predicted_delay_hours"],
            "baseline_delay_hours": row["baseline_delay_hours"],
            "error_hours": row["predicted_delay_hours"] - row["synthetic_label_hours"],
            "synthetic_label_hours": row["synthetic_label_hours"],
            "hazard_severity": row.get("hazard_severity"),
            "density": row.get("density"),
            "speed_knots": row.get("speed_knots"),
            "route_distance_nm": row.get("route_distance_nm"),
            "baseline_hours": row.get("baseline_hours"),
            "split": row.get("split"),
        }
        for row in latest_positions(estimates)
    ]


def delay_mae(estimates: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    """Mean absolute error of the model and the mean baseline on held-out rows."""
    held_out = [row for row in estimates if row.get("split") == "evaluation"]
    if not held_out:
        return {"evaluation_count": 0, "model_mae_hours": None, "baseline_mae_hours": None}
    count = len(held_out)
    return {
        "evaluation_count": count,
        "model_mae_hours": sum(
            abs(row["predicted_delay_hours"] - row["synthetic_label_hours"]) for row in held_out
        )
        / count,
        "baseline_mae_hours": sum(
            abs(row["baseline_delay_hours"] - row["synthetic_label_hours"]) for row in held_out
        )
        / count,
    }


def route_scenario_alerts(
    scenario: str,
    alerts: Iterable[Mapping[str, Any]],
    at_time: str | datetime,
) -> list[dict[str, Any]]:
    """Build the alert list for a route what-if scenario in ``ROUTE_SCENARIOS``."""
    if scenario not in ROUTE_SCENARIOS:
        raise ValueError(f"scenario must be one of {ROUTE_SCENARIOS}")
    if scenario == "No hazards":
        return []
    result = [dict(alert) for alert in alerts]
    if scenario == "Close inner lane":
        timestamp = _utc(at_time)
        latitude, longitude = DEMO_NODES["chokepoint"]
        half = 0.12
        result.append(
            {
                "schema_version": 1,
                "event_id": "what-if-closure-event",
                "alert_id": "WHAT-IF-CLOSURE",
                "event_time": _iso(timestamp),
                "valid_from": _iso(timestamp),
                "valid_until": _iso(timestamp + timedelta(hours=6)),
                "hazard_type": "lane_closure",
                "severity": 5,
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [longitude - half, latitude - half],
                            [longitude + half, latitude - half],
                            [longitude + half, latitude + half],
                            [longitude - half, latitude + half],
                            [longitude - half, latitude - half],
                        ]
                    ],
                },
            }
        )
    return result


def lane_status(
    edge_scores: Mapping[str, list[Mapping[str, Any]]],
    route: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Describe each scored lane: risk, its drivers, and whether it is open or blocked."""
    threshold = route["risk_threshold"]

    def legs(leg: Mapping[str, Any] | None) -> set[tuple[str, str]]:
        path = leg["path"] if leg else []
        return set(zip(path, path[1:], strict=False))

    baseline, alternative = legs(route["baseline"]), legs(route.get("alternative"))
    rows = []
    for source, edges in sorted(edge_scores.items()):
        for edge in edges:
            leg = (source, edge["to"])
            detail = edge.get("risk_detail") or {}
            drivers = [f"severity {detail.get('max_severity', 0)}"]
            if detail.get("alert_ids"):
                drivers[0] += f" ({', '.join(detail['alert_ids'])})"
            drivers.append(f"density {detail.get('max_density', 0)}")
            used = [
                name
                for name, members in (("baseline", baseline), ("alternative", alternative))
                if leg in members
            ]
            rows.append(
                {
                    "lane": f"{source} → {edge['to']}",
                    "distance_nm": round(edge.get("distance_nm", 0.0), 1),
                    "travel_hours": edge["travel_hours"],
                    "risk": edge["risk"],
                    "risk_detail": " · ".join(drivers),
                    "status": "blocked" if edge["risk"] > threshold else "open",
                    "used_by": " + ".join(used) or "—",
                }
            )
    return rows


def exposure_series(risk_events: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Count distinct vessels inside an active hazard at each event time."""
    exposed: dict[str, set[str]] = {}
    for event in risk_events:
        vessels = exposed.setdefault(event["event_time"], set())
        if event.get("risk_alert"):
            vessels.add(event["vessel_id"])
    return [
        {"event_time": time, "exposed_vessels": len(exposed[time])}
        for time in sorted(exposed, key=_utc)
    ]


def daily_history(lake_dir: str | Path) -> list[dict[str, Any]]:
    """Summarise the curated lake per ``event_date``; an empty lake returns [].

    A local lake's summary is cached until any of its Parquet files changes.
    """
    root = Path(lake_dir)
    signature = (
        tuple(
            sorted(
                (str(path), path.stat().st_mtime_ns, path.stat().st_size)
                for path in root.rglob("*.parquet")
            )
        )
        if root.is_dir()
        else ()
    )
    cached = _HISTORY_CACHE.get(str(lake_dir))
    if signature and cached is not None and cached[0] == signature:
        return [dict(row) for row in cached[1]]
    history = _daily_history(lake_dir)
    if signature:
        _HISTORY_CACHE[str(lake_dir)] = (signature, history)
    return [dict(row) for row in history]


def _daily_history(lake_dir: str | Path) -> list[dict[str, Any]]:
    from georisk.storage_analytics.lake import read_dataset

    days: dict[str, dict[str, Any]] = {}

    def day(event_date: str) -> dict[str, Any]:
        return days.setdefault(
            event_date,
            {
                "event_date": event_date,
                "risk_events": 0,
                "alerted_events": 0,
                "exposed_vessels": set(),
                "max_density": 0,
            },
        )

    for row in read_dataset(lake_dir, "risk_events"):
        entry = day(row["event_date"])
        entry["risk_events"] += 1
        if row["risk_alert"]:
            entry["alerted_events"] += 1
            entry["exposed_vessels"].add(row["vessel_id"])
    for row in read_dataset(lake_dir, "density_metrics"):
        entry = day(row["event_date"])
        entry["max_density"] = max(entry["max_density"], row["unique_vessel_count"])
    return [
        {**entry, "exposed_vessels": len(entry["exposed_vessels"])}
        for _, entry in sorted(days.items())
    ]


# --- Presentation helpers -------------------------------------------------


def format_time(value: str | datetime | None) -> str:
    """Return ``HH:MM`` in UTC, or an em dash for a missing or invalid value."""
    try:
        return _utc(value).strftime("%H:%M") if value else "—"
    except ValueError:
        return "—"


def format_datetime(value: str | datetime | None) -> str:
    """Return ``1 Jan 2026, 13:55 UTC``, or an em dash for a missing or invalid value."""
    try:
        timestamp = _utc(value) if value else None
    except ValueError:
        timestamp = None
    if timestamp is None:
        return "—"
    return f"{timestamp.day} {timestamp:%b %Y, %H:%M} UTC"


def humanize_minutes(minutes: float | None) -> str:
    """Render a duration for people: ``<1 min``, ``12 min``, ``3 h 05 min``, ``288 days``."""
    if minutes is None:
        return "—"
    sign = "−" if minutes < 0 else ""
    value = abs(minutes)
    if value < 1:
        return f"{sign}<1 min"
    if value < 60:
        return f"{sign}{value:.0f} min"
    if value < 48 * 60:
        hours, remainder = divmod(round(value), 60)
        return f"{sign}{hours} h {remainder:02d} min"
    days = value / (24 * 60)
    return f"{sign}{days:.0f} days"


def time_span(
    rows: Iterable[Mapping[str, Any]], field: str = "event_time"
) -> tuple[str, str] | None:
    """Return the earliest and latest timestamps of ``field``, or ``None`` if empty."""
    values = sorted((row[field] for row in rows if row.get(field)), key=_utc)
    return (values[0], values[-1]) if values else None


def vessel_exposure(risk_events: Iterable[Mapping[str, Any]], vessel_id: str) -> dict[str, Any]:
    """Summarise one vessel's hazard exposure over the whole scenario."""
    rows = sorted(
        (event for event in risk_events if event["vessel_id"] == vessel_id),
        key=lambda event: _utc(event["event_time"]),
    )
    alerted = [event for event in rows if event.get("risk_alert")]
    return {
        "positions": len(rows),
        "alerted_positions": len(alerted),
        "first_alerted": alerted[0]["event_time"] if alerted else None,
        "last_alerted": alerted[-1]["event_time"] if alerted else None,
        "alert_ids": sorted(
            {hazard["alert_id"] for event in alerted for hazard in event["matching_hazards"]}
        ),
    }


def alert_status(alert: Mapping[str, Any], at_time: str | datetime) -> dict[str, Any]:
    """Classify an alert at ``at_time`` and give the minutes until it starts or ends."""
    timestamp = _utc(at_time)
    start, end = _utc(alert["valid_from"]), _utc(alert["valid_until"])
    if timestamp < start:
        return {"state": "upcoming", "minutes": (start - timestamp).total_seconds() / 60}
    if timestamp >= end:
        return {"state": "expired", "minutes": (timestamp - end).total_seconds() / 60}
    return {"state": "active", "minutes": (end - timestamp).total_seconds() / 60}


def vessels_inside(alert_id: str, latest_risk: Mapping[str, Mapping[str, Any]]) -> list[str]:
    """Vessels whose latest position matched ``alert_id``."""
    return sorted(
        vessel
        for vessel, event in latest_risk.items()
        if any(hazard["alert_id"] == alert_id for hazard in event.get("matching_hazards") or [])
    )


def density_trend(density_metrics: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Per window: busiest cell's unique-vessel count and the number of occupied cells."""
    windows: dict[str, dict[str, Any]] = {}
    for row in density_metrics:
        entry = windows.setdefault(row["window_start"], {"peak": 0, "cells": 0})
        entry["peak"] = max(entry["peak"], row["unique_vessel_count"])
        entry["cells"] += 1
    return [
        {"window_start": start, "window": format_time(start), **windows[start]}
        for start in sorted(windows, key=_utc)
    ]


def count_by(rows: Iterable[Mapping[str, Any]], field: str) -> dict[str, int]:
    """Count rows by one field, largest first; missing values count as ``unknown``."""
    counts: dict[str, int] = {}
    for row in rows:
        key = str(row.get(field) or "unknown")
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items(), key=lambda item: (-item[1], item[0])))


def acceptance_summary(report: Mapping[str, Any] | None) -> dict[str, Any] | None:
    """Condense a run report's acceptance block; ``None`` when no report is available."""
    acceptance = (report or {}).get("acceptance")
    if not isinstance(acceptance, Mapping):
        return None
    checks = [
        {
            "check": str(item.get("name", "")).replace("_", " "),
            "passed": bool(item.get("passed")),
            "detail": str(item.get("detail", "")),
        }
        for item in acceptance.get("checks") or []
        if isinstance(item, Mapping)
    ]
    return {
        "passed": bool(acceptance.get("passed")),
        "total": len(checks),
        "passed_count": sum(check["passed"] for check in checks),
        "checked_at": acceptance.get("checked_at"),
        "checks": checks,
    }


def chart_time_labels(values: Iterable[str | datetime]) -> list[str]:
    """Label instants for chart axes in UTC: ``HH:MM`` within one day, else ``MM-DD HH:MM``.

    Charts receive these strings instead of datetimes because the browser would
    otherwise convert timestamps to the viewer's local time zone. Both formats
    sort chronologically as text.
    """
    instants = [_utc(value) for value in values]
    single_day = len({instant.date() for instant in instants}) <= 1
    return [instant.strftime("%H:%M" if single_day else "%m-%d %H:%M") for instant in instants]


# --- Scenario replay helpers ----------------------------------------------


def scenario_times(telemetry: Iterable[Mapping[str, Any]]) -> list[str]:
    """Distinct telemetry timestamps in chronological order: the replay clock's ticks."""
    return sorted({row["event_time"] for row in telemetry}, key=_utc)


def up_to(rows: Iterable[Mapping[str, Any]], at_time: str | datetime) -> list[dict[str, Any]]:
    """Rows whose ``event_time`` is at or before ``at_time``."""
    limit = _utc(at_time)
    return [dict(row) for row in rows if _utc(row["event_time"]) <= limit]


def vessel_tracks(
    telemetry: Iterable[Mapping[str, Any]], at_time: str | datetime
) -> dict[str, list[list[float]]]:
    """Each vessel's ``[lon, lat]`` track up to ``at_time``, oldest first."""
    tracks: dict[str, list[tuple[datetime, list[float]]]] = {}
    for row in up_to(telemetry, at_time):
        tracks.setdefault(row["vessel_id"], []).append(
            (_utc(row["event_time"]), [row["longitude"], row["latitude"]])
        )
    return {
        vessel: [point for _, point in sorted(points, key=lambda item: item[0])]
        for vessel, points in sorted(tracks.items())
    }


def window_containing(
    density_metrics: Iterable[Mapping[str, Any]], at_time: str | datetime
) -> str | None:
    """The ``window_start`` of the density window that contains ``at_time``, if any."""
    timestamp = _utc(at_time)
    for row in density_metrics:
        if _utc(row["window_start"]) <= timestamp < _utc(row["window_end"]):
            return row["window_start"]
    return None

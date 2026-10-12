"""Pure view helpers that shape pipeline outputs for the dashboard."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Mapping

from georisk.geo_demo import DEMO_NODES

ROUTE_SCENARIOS = ("As generated", "No hazards", "Close inner lane")


def _utc(value: str | datetime) -> datetime:
    timestamp = (datetime.fromisoformat(value.replace("Z", "+00:00"))
                 if isinstance(value, str) else value)
    if not isinstance(timestamp, datetime) or timestamp.tzinfo is None:
        raise ValueError("timestamps must be timezone-aware")
    return timestamp.astimezone(timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _max_severity(event: Mapping[str, Any]) -> int:
    return max((hazard.get("severity", 0) for hazard in event.get("matching_hazards") or []),
               default=0)


def active_alerts(alerts: Iterable[Mapping[str, Any]], at_time: str | datetime) -> list[dict[str, Any]]:
    """Return alerts valid at ``at_time`` (half-open), using each alert's latest revision.

    Revisions published after ``at_time`` are ignored, matching route scoring.
    """
    timestamp = _utc(at_time)
    latest: dict[str, Mapping[str, Any]] = {}
    for alert in alerts:
        if _utc(alert["event_time"]) > timestamp:
            continue
        previous = latest.get(alert["alert_id"])
        if previous is None or _utc(alert["event_time"]) >= _utc(previous["event_time"]):
            latest[alert["alert_id"]] = alert
    return sorted(
        (dict(alert) for alert in latest.values()
         if _utc(alert["valid_from"]) <= timestamp < _utc(alert["valid_until"])),
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
    risk_events: Iterable[Mapping[str, Any]], *, hazard_types: Iterable[str] | None = None,
    min_severity: int | None = None, start: str | datetime | None = None,
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
    density_metrics: Iterable[Mapping[str, Any]], window_start: str,
) -> list[dict[str, Any]]:
    """Return one window's rows, busiest cell first."""
    rows = [dict(row) for row in density_metrics if row["window_start"] == window_start]
    return sorted(rows, key=lambda row: (-row["unique_vessel_count"], row["h3_index"]))


def compare_adjacent_windows(
    density_metrics: Iterable[Mapping[str, Any]], window_start: str,
) -> list[dict[str, Any]]:
    """Compare each cell's count with the previous and next windows.

    A neighbour that does not exist is ``None``; a cell absent from an existing
    window counts as 0. Unknown windows return an empty list.
    """
    rows = list(density_metrics)
    windows = density_windows(rows)
    if window_start not in windows:
        return []
    index = windows.index(window_start)
    neighbours = {
        "previous": windows[index - 1] if index > 0 else None,
        "current": window_start,
        "next": windows[index + 1] if index + 1 < len(windows) else None,
    }
    counts: dict[str, dict[str, int]] = {}
    for row in rows:
        for name, window in neighbours.items():
            if window is not None and row["window_start"] == window:
                counts.setdefault(row["h3_index"], {})[name] = row["unique_vessel_count"]
    result = [
        {"h3_index": cell, **{
            name: (None if window is None else values.get(name, 0))
            for name, window in neighbours.items()
        }}
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


def freshness(
    metadata: Mapping[str, Any] | None, now: datetime, stale_after_minutes: int,
) -> dict[str, Any]:
    """Classify the last processing run as ``fresh``, ``stale``, or ``unavailable``."""
    if (not isinstance(stale_after_minutes, int) or isinstance(stale_after_minutes, bool)
            or stale_after_minutes <= 0):
        raise ValueError("stale_after_minutes must be a positive integer")
    metadata = metadata or {}
    result: dict[str, Any] = {
        "processed_at": metadata.get("processed_at"),
        "latest_event_time": metadata.get("latest_event_time"),
        "watermark": metadata.get("watermark"),
        "age_minutes": None,
        "state": "unavailable",
    }
    if not result["processed_at"]:
        return result
    age = max(0.0, (_utc(now) - _utc(result["processed_at"])).total_seconds() / 60)
    result["age_minutes"] = round(age, 1)
    result["state"] = "fresh" if age <= stale_after_minutes else "stale"
    return result


def risk_table_rows(risk_events: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Flatten risk events into display rows, newest first."""
    rows = [
        {
            "event_time": event["event_time"],
            "vessel_id": event["vessel_id"],
            "hazards": ", ".join(sorted({
                f"{hazard['hazard_type']} ({hazard['alert_id']})"
                for hazard in event.get("matching_hazards") or []
            })),
            "max_severity": _max_severity(event),
            "h3_index": event.get("h3_index"),
            "coverage_mode": event.get("coverage_mode"),
            "h3_resolution": event.get("h3_resolution"),
            "latitude": event.get("latitude"),
            "longitude": event.get("longitude"),
        }
        for event in risk_events
    ]
    return sorted(rows, key=lambda row: (row["event_time"], row["vessel_id"]), reverse=True)


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
        "model_mae_hours": sum(abs(row["predicted_delay_hours"] - row["synthetic_label_hours"])
                               for row in held_out) / count,
        "baseline_mae_hours": sum(abs(row["baseline_delay_hours"] - row["synthetic_label_hours"])
                                  for row in held_out) / count,
    }


def route_scenario_alerts(
    scenario: str, alerts: Iterable[Mapping[str, Any]], at_time: str | datetime,
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
        result.append({
            "schema_version": 1, "event_id": "what-if-closure-event",
            "alert_id": "WHAT-IF-CLOSURE", "event_time": _iso(timestamp),
            "valid_from": _iso(timestamp), "valid_until": _iso(timestamp + timedelta(hours=6)),
            "hazard_type": "lane_closure", "severity": 5,
            "geometry": {"type": "Polygon", "coordinates": [[
                [longitude - half, latitude - half], [longitude + half, latitude - half],
                [longitude + half, latitude + half], [longitude - half, latitude + half],
                [longitude - half, latitude - half],
            ]]},
        })
    return result


def lane_status(
    edge_scores: Mapping[str, list[Mapping[str, Any]]], route: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Describe each scored lane: risk, its drivers, and whether it is open or blocked."""
    threshold = route["risk_threshold"]

    def legs(path: list[str]) -> set[tuple[str, str]]:
        return set(zip(path, path[1:]))

    baseline, alternative = legs(route["baseline"]["path"]), legs(route["alternative"]["path"])
    rows = []
    for source, edges in sorted(edge_scores.items()):
        for edge in edges:
            leg = (source, edge["to"])
            detail = edge.get("risk_detail") or {}
            drivers = [f"severity {detail.get('max_severity', 0)}"]
            if detail.get("alert_ids"):
                drivers[0] += f" ({', '.join(detail['alert_ids'])})"
            drivers.append(f"density {detail.get('max_density', 0)}")
            used = [name for name, members in (("baseline", baseline), ("alternative", alternative))
                    if leg in members]
            rows.append({
                "lane": f"{source} → {edge['to']}",
                "distance_nm": round(edge.get("distance_nm", 0.0), 1),
                "travel_hours": edge["travel_hours"],
                "risk": edge["risk"],
                "risk_detail": " · ".join(drivers),
                "status": "blocked" if edge["risk"] > threshold else "open",
                "used_by": " + ".join(used) or "—",
            })
    return rows


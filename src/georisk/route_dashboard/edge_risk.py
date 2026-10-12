"""Derive lane risk from active hazard coverage and current-window density."""

from __future__ import annotations

import math
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Mapping, Iterable

import h3

from georisk.processing.h3_index import COVERAGE_MODES, point_to_cell, polygon_to_cells


def edge_cells(
    nodes: Mapping[str, tuple[float, float]], source: str, target: str, resolution: int,
) -> list[str]:
    """Return H3 cells along the grid path joining an edge's endpoints."""
    return list(h3.grid_path_cells(
        point_to_cell(*nodes[source], resolution), point_to_cell(*nodes[target], resolution)
    ))


def _utc(value: str | datetime) -> datetime:
    timestamp = datetime.fromisoformat(value.replace("Z", "+00:00")) if isinstance(value, str) else value
    if not isinstance(timestamp, datetime) or timestamp.tzinfo is None:
        raise ValueError("timestamps must be timezone-aware")
    return timestamp.astimezone(timezone.utc)


def score_graph(
    graph: Mapping[str, list[dict[str, Any]]], nodes: Mapping[str, tuple[float, float]],
    alerts: Iterable[dict[str, Any]], density_metrics: Iterable[dict[str, Any]], *,
    at_time: str | datetime, resolution: int, coverage_mode: str,
    severity_weight: float = 0.7, density_weight: float = 0.3, density_saturation: float = 5,
) -> dict[str, list[dict[str, Any]]]:
    """Return a scored deep copy; validity and density windows are half-open."""
    timestamp = _utc(at_time)
    if coverage_mode not in COVERAGE_MODES:
        raise ValueError("coverage_mode must be center or overlap")
    for name, value in (("severity_weight", severity_weight), ("density_weight", density_weight),
                        ("density_saturation", density_saturation)):
        if (not isinstance(value, (int, float)) or isinstance(value, bool)
                or not math.isfinite(value) or value < 0):
            raise ValueError(f"{name} must be a non-negative finite number")
    if density_saturation == 0:
        raise ValueError("density_saturation must be positive")
    latest: dict[str, dict[str, Any]] = {}
    for alert in alerts:
        if _utc(alert["event_time"]) > timestamp:
            continue
        previous = latest.get(alert["alert_id"])
        if previous is None or _utc(alert["event_time"]) >= _utc(previous["event_time"]):
            latest[alert["alert_id"]] = alert
    active = [
        (alert, polygon_to_cells(alert["geometry"], resolution, coverage_mode))
        for alert in latest.values()
        if _utc(alert["valid_from"]) <= timestamp < _utc(alert["valid_until"])
    ]
    density: dict[str, int] = {}
    for row in density_metrics:
        if _utc(row["window_start"]) <= timestamp < _utc(row["window_end"]):
            cell = row["h3_index"]
            density[cell] = max(density.get(cell, 0), row["unique_vessel_count"])
    result = deepcopy(dict(graph))
    for source, edges in result.items():
        for edge in edges:
            cells = set(edge_cells(nodes, source, edge["to"], resolution))
            matches = [alert for alert, coverage in active if cells & coverage]
            severity = max((alert["severity"] for alert in matches), default=0)
            count = max((density.get(cell, 0) for cell in cells), default=0)
            edge["risk"] = round(min(
                1.0, severity_weight * severity / 5
                + density_weight * min(1.0, count / density_saturation)
            ), 3)
            edge["risk_detail"] = {
                "max_severity": severity,
                "alert_ids": sorted(alert["alert_id"] for alert in matches),
                "max_density": count,
            }
    return result

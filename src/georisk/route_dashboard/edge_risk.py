"""Derive lane risk from active hazard coverage and current-window density."""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any

import h3

from georisk.contracts import parse_utc_timestamp
from georisk.geo_demo import haversine_nm
from georisk.processing.h3_index import COVERAGE_MODES, point_to_cell, polygon_to_cells
from georisk.processing.stream_job import alert_revisions

_NM_PER_KM = 1 / 1.852


def edge_cells(
    nodes: Mapping[str, tuple[float, float]],
    source: str,
    target: str,
    resolution: int,
) -> list[str]:
    """Return H3 cells along the line joining an edge's endpoints.

    Uses H3's grid path where it is defined. H3 cannot trace paths across
    pentagons or very long distances, so those edges fall back to sampling the
    straight segment at half the average cell edge length.
    """
    start = point_to_cell(*nodes[source], resolution)
    end = point_to_cell(*nodes[target], resolution)
    try:
        return list(h3.grid_path_cells(start, end))
    except h3.H3BaseException:
        pass
    (lat_a, lon_a), (lat_b, lon_b) = nodes[source], nodes[target]
    step_nm = h3.average_hexagon_edge_length(resolution, unit="km") * _NM_PER_KM / 2
    steps = max(1, math.ceil(haversine_nm(lat_a, lon_a, lat_b, lon_b) / step_nm))
    cells = dict.fromkeys(
        point_to_cell(
            lat_a + (lat_b - lat_a) * index / steps,
            lon_a + (lon_b - lon_a) * index / steps,
            resolution,
        )
        for index in range(steps + 1)
    )
    return list(cells)


def _utc(value: str | datetime) -> datetime:
    if isinstance(value, str):
        return parse_utc_timestamp(value)
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError("timestamps must be timezone-aware")
    return value.astimezone(timezone.utc)


def score_graph(
    graph: Mapping[str, list[dict[str, Any]]],
    nodes: Mapping[str, tuple[float, float]],
    alerts: Iterable[dict[str, Any]],
    density_metrics: Iterable[dict[str, Any]],
    *,
    at_time: str | datetime,
    resolution: int,
    coverage_mode: str,
    severity_weight: float = 0.7,
    density_weight: float = 0.3,
    density_saturation: float = 5,
) -> dict[str, list[dict[str, Any]]]:
    """Return a scored deep copy; validity and density windows are half-open."""
    timestamp = _utc(at_time)
    if coverage_mode not in COVERAGE_MODES:
        raise ValueError("coverage_mode must be center or overlap")
    for name, value in (
        ("severity_weight", severity_weight),
        ("density_weight", density_weight),
        ("density_saturation", density_saturation),
    ):
        if (
            not isinstance(value, (int, float))
            or isinstance(value, bool)
            or not math.isfinite(value)
            or value < 0
        ):
            raise ValueError(f"{name} must be a non-negative finite number")
    if density_saturation == 0:
        raise ValueError("density_saturation must be positive")
    active = [
        (revision.alert, polygon_to_cells(revision.alert["geometry"], resolution, coverage_mode))
        for revision in alert_revisions(alerts)
        if revision.applies_at(timestamp)
    ]
    density: dict[str, int] = {}
    for row in density_metrics:
        if row.get("h3_resolution", resolution) != resolution:
            raise ValueError(
                f"density metrics use H3 resolution {row['h3_resolution']}, but routes are "
                f"scored at resolution {resolution}; reprocess or use the matching resolution"
            )
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
            edge["risk"] = round(
                min(
                    1.0,
                    severity_weight * severity / 5
                    + density_weight * min(1.0, count / density_saturation),
                ),
                3,
            )
            edge["risk_detail"] = {
                "max_severity": severity,
                "alert_ids": sorted(alert["alert_id"] for alert in matches),
                "max_density": count,
            }
    return result

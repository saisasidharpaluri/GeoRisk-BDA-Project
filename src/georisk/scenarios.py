"""Small deterministic demonstrations of the guide's acceptance criteria."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import h3

from georisk.geo_demo import DEMO_NODES
from georisk.processing.h3_index import point_to_cell, polygon_to_cells
from georisk.processing.stream_job import process_events
from georisk.route_dashboard.route_optimizer import demonstration_graph, find_alternative_route


def expected_scenario_names(coverage_mode: str) -> tuple[str, ...]:
    """Return the required checks for the selected coverage policy."""
    names = (
        "inside_active_hazard_alerts",
        "clearly_outside_no_alert",
        "expired_hazard_no_match",
        "duplicate_messages_no_density_inflation",
        "late_event_quarantined",
        "route_above_threshold_explained",
    )
    return names + (("boundary_cell_detected",) if coverage_mode == "overlap" else ())


def run_acceptance_scenarios(
    *,
    resolution: int,
    coverage_mode: str,
    window_minutes: int,
    allowed_lateness_minutes: int,
) -> list[dict[str, Any]]:
    """Exercise spatial, temporal, deduplication, and route behavior in memory."""
    settings = dict(
        resolution=resolution,
        coverage_mode=coverage_mode,
        window_minutes=window_minutes,
        allowed_lateness_minutes=allowed_lateness_minutes,
    )
    origin = datetime(2026, 1, 1, 12, tzinfo=timezone.utc)

    def timestamp(minutes: int) -> str:
        return (origin + timedelta(minutes=minutes)).isoformat().replace("+00:00", "Z")

    cell = point_to_cell(*DEMO_NODES["chokepoint"], resolution)
    latitude, longitude = h3.cell_to_latlng(cell)
    vertex_lat, vertex_lon = h3.cell_to_boundary(cell)[0]
    size = max(abs(vertex_lat - latitude), abs(vertex_lon - longitude)) * 0.2

    def telemetry(
        event_id: str, lat: float = latitude, lon: float = longitude, minutes: int = 0
    ) -> dict[str, Any]:
        return dict(
            schema_version=1,
            event_id=event_id,
            vessel_id="acceptance-vessel",
            event_time=timestamp(minutes),
            latitude=lat,
            longitude=lon,
            speed_knots=10,
            heading_deg=90,
        )

    def alert(
        lat: float = latitude,
        lon: float = longitude,
        half_size: float = size,
        start: int = -1,
        end: int = 30,
    ) -> dict[str, Any]:
        return dict(
            schema_version=1,
            event_id="acceptance-alert-event",
            alert_id="acceptance-alert",
            event_time=timestamp(start),
            valid_from=timestamp(start),
            valid_until=timestamp(end),
            hazard_type="storm",
            severity=5,
            geometry={
                "type": "Polygon",
                "coordinates": [
                    [
                        [lon - half_size, lat - half_size],
                        [lon + half_size, lat - half_size],
                        [lon + half_size, lat + half_size],
                        [lon - half_size, lat + half_size],
                        [lon - half_size, lat - half_size],
                    ]
                ],
            },
        )

    results = []

    def record(name: str, passed: bool, detail: str) -> None:
        results.append(dict(name=name, passed=bool(passed), detail=detail))

    def matched(event: dict[str, Any], hazard: dict[str, Any]) -> bool | None:
        result = process_events([event], [hazard], **settings)
        rows = result["risk_events"]
        return rows[0]["risk_alert"] if len(rows) == 1 and not result["quarantine"] else None

    record(
        "inside_active_hazard_alerts",
        matched(telemetry("inside"), alert()) is True,
        "vessel at the hazard center raises a risk alert",
    )
    record(
        "clearly_outside_no_alert",
        matched(telemetry("outside", *DEMO_NODES["outer_lane"]), alert()) is False,
        "vessel clearly outside the hazard raises no risk alert",
    )
    record(
        "expired_hazard_no_match",
        matched(telemetry("expired"), alert(start=-2, end=0)) is False,
        "hazard validity excludes its end timestamp",
    )
    event = telemetry("duplicate")
    duplicated = process_events([event, dict(event)], [alert()], **settings)
    record(
        "duplicate_messages_no_density_inflation",
        len(duplicated["risk_events"]) == 1
        and len(duplicated["density_metrics"]) == 1
        and duplicated["density_metrics"][0]["unique_vessel_count"] == 1
        and duplicated["stats"]["duplicates_dropped"] == 1,
        "repeated event ID produces one risk row and one unique vessel",
    )
    late = process_events(
        [telemetry("new"), telemetry("late", minutes=-allowed_lateness_minutes - 1)], [], **settings
    )
    record(
        "late_event_quarantined",
        len(late["risk_events"]) == 1
        and len(late["quarantine"]) == 1
        and late["quarantine"][0]["reason"] == "late_event",
        "arrival older than the configured watermark is quarantined",
    )
    route = find_alternative_route(demonstration_graph(), "port_a", "port_b", 0.5)
    record(
        "route_above_threshold_explained",
        route["route_changed"]
        and bool(route["alternative"]["blocked_edges"])
        and bool(route["explanation"]),
        route["explanation"],
    )
    if coverage_mode == "overlap":
        edge_lat = latitude + (vertex_lat - latitude) * 0.8
        edge_lon = longitude + (vertex_lon - longitude) * 0.8
        boundary_alert = alert(edge_lat, edge_lon, size * 0.1)
        record(
            "boundary_cell_detected",
            cell not in polygon_to_cells(boundary_alert["geometry"], resolution, "center")
            and matched(telemetry("boundary", edge_lat, edge_lon), boundary_alert) is True,
            "overlap detects an intersected cell whose center is outside the hazard",
        )
    return results

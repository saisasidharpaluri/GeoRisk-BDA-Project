import json

import pytest

from georisk.route_dashboard.dashboard import dashboard_summary, load_dashboard_data
from georisk.route_dashboard.route_optimizer import (
    demonstration_graph,
    find_alternative_route,
)


def test_blocked_edges_reported_when_threshold_exceeded():
    result = find_alternative_route(demonstration_graph(), "port_a", "port_b", 0.5)
    assert result["baseline"]["blocked_edges"] == []
    assert result["alternative"]["blocked_edges"] == [
        ["chokepoint", "port_b"], ["port_a", "chokepoint"],
    ]
    assert result["baseline_blocked_edges"] == [
        ["port_a", "chokepoint"], ["chokepoint", "port_b"],
    ]


def test_no_blocked_edges_at_threshold_one():
    result = find_alternative_route(demonstration_graph(), "port_a", "port_b", 1.0)
    assert result["alternative"]["blocked_edges"] == []
    assert result["baseline_blocked_edges"] == []
    assert result["explanation"] == "Baseline route is within the risk threshold."


def test_explanation_names_blocked_edges():
    result = find_alternative_route(demonstration_graph(), "port_a", "port_b", 0.5)
    assert result["explanation"] == (
        "Edges above risk threshold 0.50 were unavailable: port_a→chokepoint (0.90), "
        "chokepoint→port_b (0.90). "
        "Alternative adds +2.4 h and changes total risk by -1.60."
    )


def test_demonstration_route_changes_when_risky_lane_is_blocked():
    result = find_alternative_route(demonstration_graph(), "port_a", "port_b", 0.5)

    assert result["baseline"]["path"] == ["port_a", "chokepoint", "port_b"]
    assert result["alternative"]["path"] == ["port_a", "outer_lane", "port_b"]
    assert result["route_changed"] is True
    assert result["alternative"]["travel_hours"] > result["baseline"]["travel_hours"]
    assert result["alternative"]["risk_score"] < result["baseline"]["risk_score"]
    assert result["synthetic_demo"] is True


def test_route_stays_same_when_threshold_allows_baseline():
    result = find_alternative_route(demonstration_graph(), "port_a", "port_b", 1.0)
    assert result["route_changed"] is False
    assert result["baseline"]["path"] == result["alternative"]["path"]


def test_graph_edges_have_distance_and_consistent_hours():
    from georisk.geo_demo import DEMO_NODES, DEMO_SPEED_KNOTS, haversine_nm
    from georisk.route_dashboard.route_optimizer import demonstration_nodes

    nodes = demonstration_nodes()
    assert nodes == DEMO_NODES
    nodes["port_a"] = (0, 0)
    assert demonstration_nodes() == DEMO_NODES
    for source, edges in demonstration_graph().items():
        for edge in edges:
            expected = haversine_nm(*DEMO_NODES[source], *DEMO_NODES[edge["to"]])
            assert edge["distance_nm"] == pytest.approx(expected)
            assert edge["travel_hours"] == round(expected / DEMO_SPEED_KNOTS, 2)


def test_route_rejects_invalid_graph_and_unreachable_alternative():
    graph = {"a": [{"to": "b", "travel_hours": 1, "risk": 0.9}], "b": []}
    with pytest.raises(ValueError, match="no eligible route"):
        find_alternative_route(graph, "a", "b", 0.5)
    with pytest.raises(ValueError, match="unknown node"):
        find_alternative_route({"a": [{"to": "missing", "travel_hours": 1, "risk": 0.1}]}, "a", "a", 0.5)


def test_route_rejects_negative_edge_values():
    with pytest.raises(ValueError, match="cannot be negative"):
        find_alternative_route(
            {"a": [{"to": "b", "travel_hours": -1, "risk": 0.1}], "b": []},
            "a",
            "b",
            0.5,
        )


def test_dashboard_loader_and_summary(tmp_path):
    data_dir = tmp_path / "data"
    processed_dir = tmp_path / "processed"
    data_dir.mkdir()
    processed_dir.mkdir()
    telemetry = {
        "event_time": "2026-01-01T12:00:00Z",
        "vessel_id": "vessel-1",
        "latitude": 12.0,
        "longitude": 78.0,
    }
    risk = {
        "event_time": "2026-01-01T12:00:00Z",
        "vessel_id": "vessel-1",
        "risk_alert": True,
        "h3_index": "cell-1",
    }
    (data_dir / "vessel-telemetry.jsonl").write_text(json.dumps(telemetry) + "\n")
    (processed_dir / "risk-events.jsonl").write_text(json.dumps(risk) + "\n")
    loaded = load_dashboard_data(tmp_path)
    summary = dashboard_summary(loaded)

    assert summary["has_data"] is True
    assert summary["telemetry_count"] == 1
    assert summary["exposed_vessel_count"] == 1
    assert summary["latest_event_time"] == "2026-01-01T12:00:00Z"


def test_dashboard_summary_exposes_no_data_state():
    summary = dashboard_summary(
        {"telemetry": [], "weather_alerts": [], "risk_events": [], "density_metrics": []}
    )
    assert summary["has_data"] is False
    assert summary["latest_event_time"] is None
    assert summary["exposed_vessel_count"] == 0

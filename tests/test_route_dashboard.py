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
        ["chokepoint", "port_b"],
        ["port_a", "chokepoint"],
    ]
    assert result["baseline_blocked_edges"] == [
        ["port_a", "chokepoint"],
        ["chokepoint", "port_b"],
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


def test_unreachable_alternative_keeps_baseline():
    graph = {"a": [{"to": "b", "travel_hours": 1, "risk": 0.9}], "b": []}
    result = find_alternative_route(graph, "a", "b", 0.5, risk_penalty=2)
    assert result["alternative"] is None
    assert result["alternative_available"] is False
    assert result["route_changed"] is False
    assert result["baseline"]["path"] == ["a", "b"]
    assert result["baseline"]["total_cost"] == pytest.approx(1 + 2 * 0.9)
    assert result["baseline_blocked_edges"] == [["a", "b"]]
    assert result["travel_hour_delta"] is None and result["risk_delta"] is None
    assert "No route avoids" in result["explanation"]


def test_route_costs_use_the_same_penalty():
    graph = {
        "a": [
            {"to": "b", "travel_hours": 1, "risk": 0.9},
            {"to": "c", "travel_hours": 1, "risk": 0},
        ],
        "b": [{"to": "d", "travel_hours": 1, "risk": 0.9}],
        "c": [{"to": "d", "travel_hours": 2, "risk": 0.1}],
        "d": [],
    }
    result = find_alternative_route(graph, "a", "d", 0.5, risk_penalty=4)
    for leg in ("baseline", "alternative"):
        route = result[leg]
        assert route["total_cost"] == pytest.approx(route["travel_hours"] + 4 * route["risk_score"])
    assert result["baseline"]["total_cost"] > result["alternative"]["total_cost"]


def test_parallel_edges_report_only_the_edge_taken():
    graph = {
        "a": [
            {"to": "b", "travel_hours": 1, "risk": 0.9},
            {"to": "b", "travel_hours": 2, "risk": 0.1},
        ],
        "b": [],
    }
    fast = find_alternative_route(graph, "a", "b", 0.5)
    assert fast["baseline_blocked_edges"] == [["a", "b"]]
    assert fast["route_changed"] is True
    assert fast["alternative"]["travel_hours"] == 2
    swapped = {"a": list(reversed(graph["a"])), "b": []}
    slow_first = find_alternative_route(swapped, "a", "b", 0.5)
    assert slow_first["baseline"]["travel_hours"] == 1


def test_route_rejects_invalid_graph_and_disconnected_destination():
    with pytest.raises(ValueError, match="no route"):
        find_alternative_route({"a": [], "b": []}, "a", "b", 0.5)
    with pytest.raises(ValueError, match="unknown node"):
        find_alternative_route(
            {"a": [{"to": "missing", "travel_hours": 1, "risk": 0.1}]}, "a", "a", 0.5
        )


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


def test_loader_uses_explicit_dirs(tmp_path):
    inputs = tmp_path / "custom-inputs"
    processed = tmp_path / "custom-results"
    inputs.mkdir()
    processed.mkdir()
    for directory, name in [
        (inputs, "vessel-telemetry"),
        (inputs, "weather-alerts"),
        (processed, "risk-events"),
        (processed, "density-metrics"),
    ]:
        (directory / f"{name}.jsonl").write_text(json.dumps({"source": name}) + "\n")
    loaded = load_dashboard_data(tmp_path / "unused", input_dir=inputs, processed_dir=processed)
    assert loaded == {
        "telemetry": [{"source": "vessel-telemetry"}],
        "weather_alerts": [{"source": "weather-alerts"}],
        "risk_events": [{"source": "risk-events"}],
        "density_metrics": [{"source": "density-metrics"}],
    }
    assert load_dashboard_data(tmp_path, input_dir=inputs)["telemetry"] == loaded["telemetry"]


@pytest.mark.parametrize("penalty", [0.0, 9.0])
def test_dashboard_and_report_share_configured_route(tmp_path, penalty):
    from georisk.config import GeoRiskConfig
    from georisk.pipeline import run_end_to_end
    from georisk.route_dashboard.dashboard import dashboard_route

    config = GeoRiskConfig(
        input_dir=tmp_path / "inputs",
        processed_dir=tmp_path / "results",
        lake_dir=tmp_path / "lake",
        dashboard_dir=tmp_path / "unused",
        report_path=tmp_path / "report.json",
        route_risk_penalty=penalty,
    )
    report = run_end_to_end(config)
    assert report["acceptance"]["passed"]
    data = load_dashboard_data(input_dir=config.input_dir, processed_dir=config.processed_dir)
    route = dashboard_route(
        data,
        resolution=config.resolution,
        coverage_mode=config.coverage_mode,
        risk_threshold=config.route_risk_threshold,
        risk_penalty=penalty,
    )
    assert route == report["route"]
    assert route["alternative"]["total_cost"] == pytest.approx(
        route["alternative"]["travel_hours"] + penalty * route["alternative"]["risk_score"]
    )


def test_summary_counts_only_alerts_active_at_latest_event():
    def alert(alert_id, valid_until):
        return {
            "alert_id": alert_id,
            "event_time": "2026-01-01T12:00:00Z",
            "valid_from": "2026-01-01T12:00:00Z",
            "valid_until": valid_until,
            "severity": 3,
            "hazard_type": "storm",
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[81, 13], [81.1, 13], [81, 13.1], [81, 13]]],
            },
        }

    data = {
        "telemetry": [{"vessel_id": "v", "event_time": "2026-01-01T13:00:00Z"}],
        "weather_alerts": [
            alert("open", "2026-01-01T14:00:00Z"),
            alert("expired", "2026-01-01T12:30:00Z"),
        ],
        "risk_events": [],
        "density_metrics": [],
    }
    summary = dashboard_summary(data)
    assert summary["active_alert_count"] == 1
    assert summary["alert_record_count"] == 2


def test_summary_orders_timestamps_chronologically():
    data = {
        "telemetry": [
            {"vessel_id": "v", "event_time": "2026-01-01T12:00:00.5Z"},
            {"vessel_id": "v", "event_time": "2026-01-01T12:00:00Z"},
        ],
        "weather_alerts": [],
        "risk_events": [],
        "density_metrics": [],
    }
    assert dashboard_summary(data)["latest_event_time"] == "2026-01-01T12:00:00.5Z"


def test_effective_settings_follow_run_metadata():
    from georisk.config import GeoRiskConfig
    from georisk.route_dashboard.dashboard import effective_settings

    settings = GeoRiskConfig()
    metadata = {"h3_resolution": 8, "coverage_mode": "center", "window_minutes": 5}
    effective = effective_settings(settings, metadata)
    assert (effective.resolution, effective.coverage_mode, effective.density_window_minutes) == (
        8,
        "center",
        5,
    )
    assert effective_settings(settings, None) is settings
    assert effective_settings(settings, {"h3_resolution": 99}) is settings

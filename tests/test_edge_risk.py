from copy import deepcopy

import pytest

from georisk.ingestion.scenario import ScenarioConfig, generate_scenario
from georisk.route_dashboard.edge_risk import edge_cells, score_graph
from georisk.route_dashboard.route_optimizer import (
    demonstration_graph,
    demonstration_nodes,
    find_alternative_route,
)


def scored(alerts=(), density=(), at_time="2026-01-01T12:00:00Z", **kwargs):
    return score_graph(
        demonstration_graph(),
        demonstration_nodes(),
        alerts,
        density,
        at_time=at_time,
        resolution=7,
        coverage_mode="overlap",
        **kwargs,
    )


def test_active_hazard_on_inner_lane_blocks_it():
    alerts = generate_scenario(ScenarioConfig(vessel_count=0))["weather_alerts"]
    graph = scored(alerts)
    assert graph["port_a"][0]["risk"] == 0.56
    assert graph["chokepoint"][0]["risk"] == 0.56
    assert graph["port_a"][0]["risk_detail"] == {
        "max_severity": 4,
        "alert_ids": ["H1"],
        "max_density": 0,
    }
    route = find_alternative_route(graph, "port_a", "port_b", 0.5)
    assert route["route_changed"] is True
    assert route["alternative"]["path"] == ["port_a", "outer_lane", "port_b"]


def test_no_hazard_route_unchanged():
    graph = scored()
    assert all(edge["risk"] == 0 for edges in graph.values() for edge in edges)
    assert find_alternative_route(graph, "port_a", "port_b", 0.5)["route_changed"] is False


def test_expired_hazard_ignored():
    alerts = generate_scenario(ScenarioConfig(vessel_count=0))["weather_alerts"]
    assert scored(alerts, at_time="2026-01-01T14:30:00Z") == scored()


def test_density_uses_current_window_and_saturates():
    cell = edge_cells(demonstration_nodes(), "port_a", "chokepoint", 7)[0]
    row = {
        "h3_index": cell,
        "window_start": "2026-01-01T12:00:00Z",
        "window_end": "2026-01-01T12:15:00Z",
        "unique_vessel_count": 3,
    }
    assert scored(density=[row])["port_a"][0]["risk"] == 0.18
    assert scored(density=[dict(row, unique_vessel_count=20)])["port_a"][0]["risk"] == 0.3
    assert scored(density=[row], at_time=row["window_end"])["port_a"][0]["risk"] == 0


def test_scoring_does_not_mutate_inputs():
    graph = demonstration_graph()
    graph["port_a"][0]["extra"] = {"labels": ["test"]}
    before = deepcopy(graph)
    result = score_graph(
        graph,
        demonstration_nodes(),
        [],
        [],
        at_time="2026-01-01T12:00:00Z",
        resolution=7,
        coverage_mode="center",
    )
    result["port_a"][0]["extra"]["labels"].append("changed")
    assert graph == before


def test_latest_available_alert_revision_wins():
    alert = generate_scenario(ScenarioConfig(vessel_count=0))["weather_alerts"][0]
    revised = dict(alert, event_id="revision", event_time="2026-01-01T12:05:00Z", severity=1)
    assert scored([alert, revised])["port_a"][0]["risk"] == 0.56
    assert scored([revised, alert], at_time=revised["event_time"])["port_a"][0]["risk"] == 0.14


@pytest.mark.parametrize(
    "kwargs", [{"density_saturation": 0}, {"density_weight": -1}, {"severity_weight": float("nan")}]
)
def test_scoring_rejects_invalid_weights(kwargs):
    with pytest.raises(ValueError):
        scored(**kwargs)

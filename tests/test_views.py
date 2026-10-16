from datetime import datetime, timedelta, timezone

import pytest

from georisk.route_dashboard.views import (
    ROUTE_SCENARIOS,
    active_alerts,
    compare_adjacent_windows,
    daily_history,
    delay_mae,
    density_for_window,
    density_windows,
    exposure_series,
    filter_risk_events,
    freshness,
    lane_status,
    latest_delay_estimates,
    latest_positions,
    risk_table_rows,
    route_scenario_alerts,
    severity_distribution,
)


def _alert(
    alert_id="H1",
    *,
    event_time="2026-01-01T12:00:00Z",
    valid_from="2026-01-01T12:00:00Z",
    valid_until="2026-01-01T13:00:00Z",
    severity=3,
    hazard_type="storm",
):
    return {
        "alert_id": alert_id,
        "event_time": event_time,
        "valid_from": valid_from,
        "valid_until": valid_until,
        "severity": severity,
        "hazard_type": hazard_type,
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[81, 13], [81.1, 13], [81.1, 13.1], [81, 13]]],
        },
    }


def _risk(vessel="v1", time="2026-01-01T12:00:00Z", hazards=(), cell="c1"):
    return {
        "vessel_id": vessel,
        "event_time": time,
        "h3_index": cell,
        "risk_alert": bool(hazards),
        "coverage_mode": "overlap",
        "h3_resolution": 7,
        "matching_hazards": [
            {"alert_id": f"H{severity}", "hazard_type": kind, "severity": severity}
            for kind, severity in hazards
        ],
    }


def _density(cell, start, count, minutes=15):
    begin = datetime.fromisoformat(f"2026-01-01T{start}:00+00:00")
    end = (begin + timedelta(minutes=minutes)).isoformat().replace("+00:00", "Z")
    return {
        "h3_index": cell,
        "window_start": f"2026-01-01T{start}:00Z",
        "window_end": end,
        "unique_vessel_count": count,
    }


def test_active_alerts_half_open_and_latest_revision():
    alerts = [
        _alert("H1"),
        _alert("H2", valid_until="2026-01-01T12:30:00Z"),
        _alert("H3", valid_from="2026-01-01T12:45:00Z"),
        _alert("H1", event_time="2026-01-01T12:20:00Z", severity=5),
        _alert("H4", event_time="2026-01-01T12:40:00Z"),
    ]
    active = active_alerts(alerts, "2026-01-01T12:30:00Z")
    # H4's only revision was issued at 12:40 but is valid from 12:00; the first
    # revision covers earlier times, exactly as in processing.
    assert [(alert["alert_id"], alert["severity"]) for alert in active] == [
        ("H1", 5),
        ("H4", 3),
    ]
    before_revision = active_alerts(alerts, "2026-01-01T12:10:00Z")
    assert [(alert["alert_id"], alert["severity"]) for alert in before_revision] == [
        ("H1", 3),
        ("H2", 3),
        ("H4", 3),
    ]
    assert active_alerts([], "2026-01-01T12:30:00Z") == []


def test_severity_distribution_uses_max_matched_severity():
    events = [
        _risk(hazards=[("storm", 2), ("high_waves", 4)]),
        _risk(hazards=[("storm", 4)]),
        _risk(hazards=[("storm", 1)]),
        _risk(),
    ]
    assert severity_distribution(events) == {1: 1, 4: 2}
    assert severity_distribution([]) == {}


def test_filter_risk_events_by_type_severity_and_time():
    events = [
        _risk("a", "2026-01-01T12:00:00Z", [("storm", 4)]),
        _risk("b", "2026-01-01T12:10:00Z", [("high_waves", 2)]),
        _risk("c", "2026-01-01T12:20:00Z"),
    ]
    assert [e["vessel_id"] for e in filter_risk_events(events)] == ["a", "b", "c"]
    assert [e["vessel_id"] for e in filter_risk_events(events, hazard_types=["storm"])] == ["a"]
    assert [e["vessel_id"] for e in filter_risk_events(events, min_severity=2)] == ["a", "b"]
    assert [
        e["vessel_id"]
        for e in filter_risk_events(
            events, start="2026-01-01T12:10:00Z", end="2026-01-01T12:20:00Z"
        )
    ] == ["b", "c"]
    assert filter_risk_events([], min_severity=1) == []


def test_density_windows_sorted_unique():
    rows = [_density("a", "12:15", 1), _density("b", "12:00", 1), _density("c", "12:15", 2)]
    assert density_windows(rows) == ["2026-01-01T12:00:00Z", "2026-01-01T12:15:00Z"]
    assert density_windows([]) == []


def test_density_for_window_sorted_by_count():
    rows = [_density("a", "12:15", 1), _density("b", "12:00", 9), _density("c", "12:15", 2)]
    assert [r["h3_index"] for r in density_for_window(rows, "2026-01-01T12:15:00Z")] == ["c", "a"]
    assert density_for_window([], "2026-01-01T12:15:00Z") == []


def test_compare_adjacent_windows():
    rows = [
        _density("a", "12:00", 1),
        _density("a", "12:15", 3),
        _density("b", "12:15", 1),
        _density("b", "12:30", 2),
        _density("c", "12:30", 5),
    ]
    middle = compare_adjacent_windows(rows, "2026-01-01T12:15:00Z")
    assert middle == [
        {"h3_index": "a", "previous": 1, "current": 3, "next": 0},
        {"h3_index": "b", "previous": 0, "current": 1, "next": 2},
        {"h3_index": "c", "previous": 0, "current": 0, "next": 5},
    ]
    first = compare_adjacent_windows(rows, "2026-01-01T12:00:00Z")
    assert first[0] == {"h3_index": "a", "previous": None, "current": 1, "next": 3}
    assert compare_adjacent_windows(rows, "2026-01-01T09:00:00Z") == []
    assert compare_adjacent_windows([], "2026-01-01T12:00:00Z") == []


def test_compare_adjacent_windows_uses_calendar_neighbours_across_gaps():
    rows = [_density("a", "12:00", 2), _density("a", "13:00", 4)]
    later = compare_adjacent_windows(rows, "2026-01-01T13:00:00Z")
    assert later == [{"h3_index": "a", "previous": 0, "current": 4, "next": None}]


def test_latest_positions_last_row_per_vessel():
    telemetry = [
        {"vessel_id": "b", "event_time": "2026-01-01T12:05:00Z", "latitude": 1},
        {"vessel_id": "a", "event_time": "2026-01-01T12:10:00Z", "latitude": 2},
        {"vessel_id": "b", "event_time": "2026-01-01T12:00:00Z", "latitude": 3},
        {"vessel_id": "a", "event_time": "2026-01-01T12:10:00Z", "latitude": 4},
    ]
    assert [(r["vessel_id"], r["latitude"]) for r in latest_positions(telemetry)] == [
        ("a", 4),
        ("b", 1),
    ]
    assert latest_positions([]) == []


def test_freshness_states():
    now = datetime(2026, 1, 1, 13, 0, tzinfo=timezone.utc)
    metadata = {"processed_at": "2026-01-01T12:50:00Z", "latest_event_time": "x", "watermark": "y"}
    fresh = freshness(metadata, now, 30)
    assert fresh == {
        "processed_at": "2026-01-01T12:50:00Z",
        "latest_event_time": "x",
        "watermark": "y",
        "age_minutes": 10.0,
        "processing_lag_minutes": None,
        "state": "fresh",
    }
    timed = dict(metadata, latest_event_time="2026-01-01T12:20:00Z")
    assert freshness(timed, now, 30)["processing_lag_minutes"] == 30.0
    assert freshness(metadata, now, 5)["state"] == "stale"
    assert freshness(None, now, 30)["state"] == "unavailable"
    assert freshness({}, now, 30)["age_minutes"] is None
    with pytest.raises(ValueError, match="positive"):
        freshness(metadata, now, 0)


def test_risk_table_rows_flatten_hazards():
    rows = risk_table_rows([_risk("a", hazards=[("storm", 4)]), _risk("b", "2026-01-01T12:05:00Z")])
    assert rows[0]["vessel_id"] == "b" and rows[0]["hazards"] == ""
    assert rows[1]["hazards"] == "storm (H4)" and rows[1]["max_severity"] == 4
    assert rows[1]["coverage_mode"] == "overlap" and rows[1]["h3_resolution"] == 7
    assert risk_table_rows([]) == []


def _estimate(vessel, time, split, predicted, label, baseline=1.0):
    return {
        "vessel_id": vessel,
        "event_time": time,
        "split": split,
        "predicted_delay_hours": predicted,
        "baseline_delay_hours": baseline,
        "synthetic_label_hours": label,
    }


def test_delay_helpers_latest_and_mae():
    estimates = [
        _estimate("a", "2026-01-01T12:00:00Z", "train", 1.0, 1.0),
        _estimate("a", "2026-01-01T12:05:00Z", "evaluation", 2.0, 2.5),
        _estimate("b", "2026-01-01T12:05:00Z", "evaluation", 0.5, 0.0),
    ]
    latest = latest_delay_estimates(estimates)
    assert [(r["vessel_id"], r["error_hours"]) for r in latest] == [("a", -0.5), ("b", 0.5)]
    mae = delay_mae(estimates)
    assert mae["evaluation_count"] == 2
    assert mae["model_mae_hours"] == pytest.approx(0.5)
    assert mae["baseline_mae_hours"] == pytest.approx(1.25)
    assert latest_delay_estimates([]) == []
    assert delay_mae([])["model_mae_hours"] is None


def test_route_scenarios_build_alert_lists():
    alerts = [_alert()]
    assert route_scenario_alerts("As generated", alerts, "2026-01-01T12:00:00Z") == alerts
    assert route_scenario_alerts("No hazards", alerts, "2026-01-01T12:00:00Z") == []
    closed = route_scenario_alerts("Close inner lane", [], "2026-01-01T12:00:00Z")
    assert len(closed) == 1 and closed[0]["severity"] == 5
    assert active_alerts(closed, "2026-01-01T12:00:00Z") == closed
    with pytest.raises(ValueError, match="scenario"):
        route_scenario_alerts("Typo", alerts, "2026-01-01T12:00:00Z")
    assert len(ROUTE_SCENARIOS) == 3


def test_close_inner_lane_forces_detour():
    from georisk.route_dashboard.edge_risk import score_graph
    from georisk.route_dashboard.route_optimizer import (
        demonstration_graph,
        demonstration_nodes,
        find_alternative_route,
    )

    at = "2026-01-01T12:00:00Z"
    graph = score_graph(
        demonstration_graph(),
        demonstration_nodes(),
        route_scenario_alerts("Close inner lane", [], at),
        [],
        at_time=at,
        resolution=7,
        coverage_mode="overlap",
    )
    route = find_alternative_route(graph, "port_a", "port_b", 0.5)
    assert route["route_changed"] is True
    status = {row["lane"]: row for row in lane_status(graph, route)}
    assert status["port_a → chokepoint"]["status"] == "blocked"
    assert "WHAT-IF-CLOSURE" in status["port_a → chokepoint"]["risk_detail"]
    assert status["port_a → outer_lane"]["status"] == "open"
    assert status["port_a → outer_lane"]["used_by"] == "alternative"
    assert status["port_a → chokepoint"]["used_by"] == "baseline"


def test_exposure_series_counts_distinct_vessels_per_time():
    events = [
        _risk("a", "2026-01-01T12:05:00Z", [("storm", 3)]),
        _risk("a", "2026-01-01T12:05:00Z", [("storm", 3)]),
        _risk("b", "2026-01-01T12:00:00Z"),
        _risk("b", "2026-01-01T12:05:00Z", [("storm", 3)]),
    ]
    assert exposure_series(events) == [
        {"event_time": "2026-01-01T12:00:00Z", "exposed_vessels": 0},
        {"event_time": "2026-01-01T12:05:00Z", "exposed_vessels": 2},
    ]
    assert exposure_series([]) == []


def test_daily_history_groups_by_event_date(tmp_path):
    from georisk.storage_analytics.lake import write_partitioned

    def risk(event_id, vessel, time, alert):
        return {
            "schema_version": 1,
            "event_id": event_id,
            "vessel_id": vessel,
            "event_time": time,
            "latitude": 13.1,
            "longitude": 81.2,
            "h3_index": "876181091ffffff",
            "h3_resolution": 7,
            "coverage_mode": "overlap",
            "risk_alert": alert,
            "matching_hazards": [
                {
                    "alert_id": "H1",
                    "hazard_type": "storm",
                    "severity": 4,
                    "valid_from": "2026-01-01T00:00:00Z",
                    "valid_until": "2026-01-03T00:00:00Z",
                }
            ]
            if alert
            else [],
        }

    write_partitioned(
        [
            risk("e1", "v1", "2026-01-01T12:00:00Z", True),
            risk("e2", "v2", "2026-01-01T12:05:00Z", False),
            risk("e3", "v1", "2026-01-02T08:00:00Z", True),
            risk("e4", "v2", "2026-01-02T08:00:00Z", True),
        ],
        tmp_path,
        dataset_name="risk_events",
    )
    write_partitioned(
        [
            {
                "schema_version": 1,
                "h3_index": "876181091ffffff",
                "h3_resolution": 7,
                "window_start": "2026-01-02T08:00:00Z",
                "window_end": "2026-01-02T08:15:00Z",
                "unique_vessel_count": 2,
            },
        ],
        tmp_path,
        dataset_name="density_metrics",
    )
    assert daily_history(tmp_path) == [
        {
            "event_date": "2026-01-01",
            "risk_events": 2,
            "alerted_events": 1,
            "exposed_vessels": 1,
            "max_density": 0,
        },
        {
            "event_date": "2026-01-02",
            "risk_events": 2,
            "alerted_events": 2,
            "exposed_vessels": 2,
            "max_density": 2,
        },
    ]
    assert daily_history(tmp_path / "empty") == []


def test_stale_after_minutes_setting():
    from georisk.config import GeoRiskConfig

    assert GeoRiskConfig.from_env({}).stale_after_minutes == 30
    assert GeoRiskConfig.from_env({"GEORISK_STALE_AFTER_MINUTES": "5"}).stale_after_minutes == 5
    with pytest.raises(ValueError, match="GEORISK_STALE_AFTER_MINUTES"):
        GeoRiskConfig.from_env({"GEORISK_STALE_AFTER_MINUTES": "0"})


def test_time_formatting_is_utc_and_tolerant():
    from georisk.route_dashboard.views import chart_time_labels, format_datetime, format_time

    assert format_time("2026-01-01T13:55:00Z") == "13:55"
    assert format_time("2026-01-01T13:55:00+00:00") == "13:55"
    assert format_time(None) == format_time("not a time") == "—"
    assert format_datetime("2026-01-01T13:55:00Z") == "1 Jan 2026, 13:55 UTC"
    assert chart_time_labels(["2026-01-01T12:00:00Z", "2026-01-01T12:15:00Z"]) == [
        "12:00",
        "12:15",
    ]
    assert chart_time_labels(["2026-01-01T23:45:00Z", "2026-01-02T00:00:00Z"]) == [
        "01-01 23:45",
        "01-02 00:00",
    ]


@pytest.mark.parametrize(
    ("minutes", "text"),
    [(None, "—"), (0.4, "<1 min"), (12.3, "12 min"), (125, "2 h 05 min"), (414548, "288 days")],
)
def test_humanize_minutes(minutes, text):
    from georisk.route_dashboard.views import humanize_minutes

    assert humanize_minutes(minutes) == text


def test_alert_status_and_vessels_inside():
    from georisk.route_dashboard.views import alert_status, vessels_inside

    alert = _alert(valid_from="2026-01-01T12:00:00Z", valid_until="2026-01-01T13:00:00Z")
    assert alert_status(alert, "2026-01-01T11:30:00Z") == {"state": "upcoming", "minutes": 30.0}
    assert alert_status(alert, "2026-01-01T12:45:00Z") == {"state": "active", "minutes": 15.0}
    assert alert_status(alert, "2026-01-01T13:00:00Z") == {"state": "expired", "minutes": 0.0}
    latest = {
        "a": _risk("a", hazards=[("storm", 4)]),
        "b": _risk("b"),
        "c": _risk("c", hazards=[("storm", 2)]),
    }
    assert vessels_inside("H4", latest) == ["a"]
    assert vessels_inside("missing", latest) == []


def test_vessel_exposure_and_density_trend():
    from georisk.route_dashboard.views import density_trend, vessel_exposure

    events = [
        _risk("a", "2026-01-01T12:10:00Z", hazards=[("storm", 4)]),
        _risk("a", "2026-01-01T12:00:00Z"),
        _risk("a", "2026-01-01T12:05:00Z", hazards=[("storm", 4)]),
        _risk("b", "2026-01-01T12:05:00Z", hazards=[("storm", 4)]),
    ]
    assert vessel_exposure(events, "a") == {
        "positions": 3,
        "alerted_positions": 2,
        "first_alerted": "2026-01-01T12:05:00Z",
        "last_alerted": "2026-01-01T12:10:00Z",
        "alert_ids": ["H4"],
    }
    assert vessel_exposure(events, "none")["first_alerted"] is None
    trend = density_trend(
        [_density("a", "12:15", 3), _density("b", "12:15", 5), _density("a", "12:00", 1)]
    )
    assert [(row["window"], row["peak"], row["cells"]) for row in trend] == [
        ("12:00", 1, 1),
        ("12:15", 5, 2),
    ]


def test_count_by_and_acceptance_summary():
    from georisk.route_dashboard.views import acceptance_summary, count_by

    assert count_by([{"r": "late"}, {"r": "late"}, {"r": None}], "r") == {"late": 2, "unknown": 1}
    report = {
        "acceptance": {
            "passed": False,
            "checked_at": "2026-01-01T12:00:00Z",
            "checks": [
                {"name": "run_status", "passed": True, "detail": "ok"},
                {"name": "model_metrics", "passed": False, "detail": "nan"},
            ],
        }
    }
    summary = acceptance_summary(report)
    assert summary["total"] == 2 and summary["passed_count"] == 1 and not summary["passed"]
    assert summary["checks"][0]["check"] == "run status"
    assert acceptance_summary(None) is None
    assert acceptance_summary({"acceptance": "bad"}) is None

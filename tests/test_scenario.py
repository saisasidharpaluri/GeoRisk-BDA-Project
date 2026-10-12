from dataclasses import replace

import pytest

from georisk.ingestion.generator import iter_events_by_time, replay_events
from georisk.ingestion.scenario import ScenarioConfig, generate_scenario
from georisk.processing.stream_job import process_events


def test_scenario_has_expiry():
    events = generate_scenario(ScenarioConfig())
    # Probe the same point before appearance, while active, and at/after expiry.
    probe = events["telemetry"][0]
    telemetry = [dict(probe, latitude=13.45, longitude=80.93,
                      event_id=f"probe-{index}", event_time=time)
                 for index, time in enumerate(("2026-01-01T12:00:00Z", "2026-01-01T12:30:00Z",
                                                "2026-01-01T13:30:00Z", "2026-01-01T13:35:00Z"))]
    result = process_events(telemetry, events["weather_alerts"])
    assert [row["risk_alert"] for row in result["risk_events"]] == [False, True, False, False]
    assert result["risk_events"][1]["matching_hazards"][0]["alert_id"] == "H2"


def test_scenario_has_multiple_windows():
    events = generate_scenario(ScenarioConfig())
    result = process_events(events["telemetry"], events["weather_alerts"])
    assert len({row["window_start"] for row in result["density_metrics"]}) >= 8
    assert len(events["telemetry"]) == 20 * 24
    assert any(row["risk_alert"] for row in result["risk_events"])


def test_scenario_has_congestion():
    events = generate_scenario(ScenarioConfig())
    result = process_events(events["telemetry"], events["weather_alerts"])
    assert max(row["unique_vessel_count"] for row in result["density_metrics"]) >= 3
    assert all(row["speed_knots"] == 6 for row in events["telemetry"] if row["vessel_id"] < "vessel-00008")


def test_events_are_time_ordered():
    events = generate_scenario(ScenarioConfig())
    ordered = list(iter_events_by_time({key: reversed(rows) for key, rows in events.items()}))
    keys = [(row["event_time"], stream != "weather_alerts", row["event_id"]) for stream, row in ordered]
    assert keys == sorted(keys)
    published = []
    replay_events(events, lambda stream, row: published.append(row))
    assert published == [row for _, row in ordered]


def test_scenario_is_deterministic_and_supports_no_hazards():
    config = ScenarioConfig(vessel_count=3, hazards=())
    assert generate_scenario(config) == generate_scenario(config)
    assert generate_scenario(config)["weather_alerts"] == []


@pytest.mark.parametrize("changes", [{"interval_seconds": 0}, {"duration_minutes": 0},
                                      {"vessel_count": -1}, {"congestion_vessels": -1},
                                      {"interval_seconds": 10000}])
def test_scenario_rejects_invalid_settings(changes):
    with pytest.raises(ValueError):
        replace(ScenarioConfig(), **changes)

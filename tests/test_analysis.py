import json

import h3

from georisk.analysis import resolution_comparison
from georisk.analysis.resolution_comparison import compare_resolutions
from georisk.ingestion.generator import generate_events
from georisk.ingestion.scenario import ScenarioConfig, generate_scenario
from georisk.processing.h3_index import polygon_to_cells
from georisk.processing.stream_job import process_events


def test_comparison_returns_one_row_per_resolution(monkeypatch):
    events = generate_events(7, 10)
    ticks = iter([0.0, 0.5, 1.0, 1.75])
    monkeypatch.setattr(resolution_comparison, "perf_counter", lambda: next(ticks))
    rows = compare_resolutions(iter(events["telemetry"]), iter(events["weather_alerts"]))
    assert [row["resolution"] for row in rows] == [7, 8]
    assert [row["runtime_seconds"] for row in rows] == [0.5, 0.75]
    for row in rows:
        assert set(row) == {
            "resolution",
            "avg_cell_area_km2",
            "hazard_cell_count",
            "alerted_event_count",
            "exposed_vessel_count",
            "density_cell_count",
            "max_unique_vessels",
            "runtime_seconds",
        }
        result = process_events(
            events["telemetry"], events["weather_alerts"], resolution=row["resolution"]
        )
        alerted = [e for e in result["risk_events"] if e["risk_alert"]]
        assert row["avg_cell_area_km2"] == h3.average_hexagon_area(row["resolution"], "km^2")
        assert row["alerted_event_count"] == len(alerted)
        assert row["exposed_vessel_count"] == len({e["vessel_id"] for e in alerted})
        assert row["density_cell_count"] == len({e["h3_index"] for e in result["density_metrics"]})
        assert row["max_unique_vessels"] == max(
            e["unique_vessel_count"] for e in result["density_metrics"]
        )


def test_res8_has_more_hazard_cells_than_res7():
    events = generate_events(7, 10)
    coarse, fine = compare_resolutions(events["telemetry"], events["weather_alerts"])
    assert fine["hazard_cell_count"] > coarse["hazard_cell_count"] > 0
    assert fine["avg_cell_area_km2"] < coarse["avg_cell_area_km2"]


def test_comparison_hazard_count_uses_unique_latest_valid_coverage():
    alert = generate_events(7, 0)["weather_alerts"][0]
    revision = dict(alert, event_id="revision", event_time="2026-01-01T12:00:00Z")
    other = dict(alert, event_id="other", alert_id="other")
    invalid = dict(revision, event_time="2026-01-01T12:01:00Z", severity=6)
    row = compare_resolutions(
        [], [alert, revision, other, invalid], resolutions=(7,), coverage_mode="center"
    )[0]
    assert row["hazard_cell_count"] == len(polygon_to_cells(alert["geometry"], 7, "center"))
    assert row["alerted_event_count"] == row["exposed_vessel_count"] == 0
    assert row["density_cell_count"] == row["max_unique_vessels"] == 0


def test_comparison_cli_uses_configured_generator_and_report_directory(
    tmp_path, monkeypatch, capsys
):
    monkeypatch.setenv("GEORISK_REPORT_PATH", str(tmp_path / "reports" / "run.json"))
    monkeypatch.setenv("GEORISK_SEED", "11")
    monkeypatch.setenv("GEORISK_VESSEL_COUNT", "4")
    monkeypatch.setenv("GEORISK_SCENARIO_DURATION_MINUTES", "30")
    monkeypatch.setenv("GEORISK_TELEMETRY_INTERVAL_SECONDS", "600")
    monkeypatch.setenv("GEORISK_H3_COVERAGE_MODE", "center")
    monkeypatch.setattr(resolution_comparison, "configure_logging", lambda level: None)
    resolution_comparison.comparison_cli()
    rows = json.loads((tmp_path / "reports" / "resolution-comparison.json").read_text())
    assert json.loads(capsys.readouterr().out) == rows
    events = generate_scenario(
        ScenarioConfig(seed=11, vessel_count=4, duration_minutes=30, interval_seconds=600)
    )
    expected = compare_resolutions(
        events["telemetry"], events["weather_alerts"], coverage_mode="center"
    )
    assert [{k: v for k, v in row.items() if k != "runtime_seconds"} for row in rows] == [
        {k: v for k, v in row.items() if k != "runtime_seconds"} for row in expected
    ]

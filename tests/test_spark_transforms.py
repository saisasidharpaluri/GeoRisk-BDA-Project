from __future__ import annotations

import json

import pytest

pytest.importorskip("pyspark")
pytestmark = pytest.mark.spark

from georisk.ingestion.scenario import ScenarioConfig, generate_scenario
from georisk.processing.h3_index import point_to_cell
from georisk.processing.spark_transforms import (
    assign_h3,
    density,
    hazard_cells_df,
    match_risk,
    read_alerts,
    read_telemetry,
    split_valid,
)
from georisk.processing.stream_job import process_events


@pytest.fixture
def scenario():
    return generate_scenario(ScenarioConfig(vessel_count=4, duration_minutes=45))


def telemetry_df(spark, tmp_path, rows):
    path = tmp_path / "telemetry.jsonl"
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
    return read_telemetry(spark, path)


def test_spark_h3_matches_python(spark, tmp_path, scenario):
    valid, _ = split_valid(telemetry_df(spark, tmp_path, scenario["telemetry"]))
    rows = assign_h3(valid, 7).collect()
    assert len(rows) == len(scenario["telemetry"])
    assert all(row.h3_index == point_to_cell(row.latitude, row.longitude, 7) for row in rows)
    assert all(row.h3_resolution == 7 for row in rows)


@pytest.mark.parametrize(
    "update",
    [
        {"latitude": 91},
        {"longitude": -181},
        {"speed_knots": -1},
        {"speed_knots": 61},
        {"heading_deg": 360},
        {"heading_deg": None},
        {"event_id": ""},
        {"schema_version": 2},
        {"latitude": float("nan")},
        {"event_time": "2026-01-01T12:00:00+05:30"},
        {"event_time": "invalid"},
    ],
)
def test_spark_split_valid_flags_bad_latitude(spark, tmp_path, scenario, update):
    good = scenario["telemetry"][0]
    valid, invalid = split_valid(telemetry_df(spark, tmp_path, [good, dict(good, **update)]))
    assert valid.count() == 1
    assert [row.reason for row in invalid.collect()] == ["contract_violation"]


def test_spark_json_readers_preserve_geometry_and_malformed_input(spark, tmp_path, scenario):
    path = tmp_path / "alerts.jsonl"
    path.write_text("\n".join(json.dumps(row) for row in scenario["weather_alerts"]))
    assert [json.loads(row.geometry) for row in read_alerts(spark, path).collect()] == [
        row["geometry"] for row in scenario["weather_alerts"]
    ]
    broken = tmp_path / "broken.jsonl"
    broken.write_text('{"event_id": "broken", this is invalid}\n')
    frame = read_telemetry(spark, broken).cache()
    try:
        frame.count()
        valid, invalid = split_valid(frame)
        assert valid.count() == 0
        assert invalid.first()._corrupt_record
    finally:
        frame.unpersist()


@pytest.mark.parametrize("coverage_mode", ["overlap", "center"])
def test_spark_risk_matches_process_events(spark, tmp_path, scenario, coverage_mode):
    telemetry = scenario["telemetry"]
    alerts = scenario["weather_alerts"]
    valid, _ = split_valid(telemetry_df(spark, tmp_path, telemetry))
    result = match_risk(
        assign_h3(valid, 7), hazard_cells_df(spark, alerts, 7, coverage_mode), coverage_mode
    )
    expected = process_events(telemetry, alerts, coverage_mode=coverage_mode)["risk_events"]
    assert sorted(
        (row.asDict(recursive=True) for row in result.collect()), key=lambda row: row["event_id"]
    ) == sorted(expected, key=lambda row: row["event_id"])


def test_spark_risk_empty_hazards_and_revisions(spark, tmp_path, scenario):
    original = scenario["weather_alerts"][0]
    revision = dict(original, event_id="revision", event_time="2026-01-01T12:05:00Z", severity=5)
    ring = original["geometry"]["coordinates"][0]
    lon, lat = ring[0]
    telemetry = [
        dict(
            scenario["telemetry"][0],
            latitude=lat,
            longitude=lon,
            event_id=f"boundary-{i}",
            event_time=time,
        )
        for i, time in enumerate([original["valid_from"], original["valid_until"]])
    ]
    valid, _ = split_valid(telemetry_df(spark, tmp_path, telemetry))
    indexed = assign_h3(valid, 7)
    for alerts in ([], [original, revision]):
        rows = match_risk(
            indexed, hazard_cells_df(spark, alerts, 7, "overlap"), "overlap"
        ).collect()
        assert sorted(
            (row.asDict(recursive=True) for row in rows), key=lambda row: row["event_id"]
        ) == sorted(
            process_events(telemetry, alerts)["risk_events"], key=lambda row: row["event_id"]
        )


@pytest.mark.parametrize("minutes", [5, 15, 60, 120])
def test_spark_density_matches_process_events(spark, tmp_path, scenario, minutes):
    telemetry = scenario["telemetry"]
    valid, _ = split_valid(telemetry_df(spark, tmp_path, telemetry + telemetry[:1]))
    rows = density(assign_h3(valid, 7), minutes).collect()
    expected = process_events(telemetry, [], window_minutes=minutes)["density_metrics"]

    def key(row):
        return (row["window_start"], row["h3_index"])

    assert sorted((row.asDict() for row in rows), key=key) == sorted(expected, key=key)


def test_spark_density_window_boundary(spark, tmp_path, scenario):
    telemetry = [
        dict(scenario["telemetry"][0], event_id=f"boundary-{i}", event_time=time)
        for i, time in enumerate(["2026-01-01T12:14:59Z", "2026-01-01T12:15:00Z"])
    ]
    valid, _ = split_valid(telemetry_df(spark, tmp_path, telemetry))
    rows = density(assign_h3(valid, 7), 15).orderBy("window_start").collect()
    assert [(row.window_start, row.unique_vessel_count) for row in rows] == [
        ("2026-01-01T12:00:00Z", 1),
        ("2026-01-01T12:15:00Z", 1),
    ]

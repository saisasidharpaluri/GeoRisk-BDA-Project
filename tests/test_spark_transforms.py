from __future__ import annotations

import json

import pytest

pytest.importorskip("pyspark")
pytestmark = pytest.mark.spark

from georisk.ingestion.scenario import ScenarioConfig, generate_scenario
from georisk.processing.h3_index import point_to_cell
from georisk.processing.spark_transforms import (
    assign_h3, read_alerts, read_telemetry, split_valid,
)


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


@pytest.mark.parametrize("update", [
    {"latitude": 91}, {"longitude": -181}, {"speed_knots": -1},
    {"speed_knots": 61}, {"heading_deg": 360}, {"heading_deg": None},
    {"event_id": ""}, {"schema_version": 2}, {"latitude": float("nan")},
    {"event_time": "2026-01-01T12:00:00+05:30"}, {"event_time": "invalid"},
])
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

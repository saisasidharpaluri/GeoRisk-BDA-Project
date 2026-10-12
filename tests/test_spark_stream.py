from __future__ import annotations

import json

import pytest

pytest.importorskip("pyspark")
pytestmark = pytest.mark.spark

from georisk.contracts import validate_event
from georisk.ingestion.file_stream import FileStreamPublisher
from georisk.ingestion.scenario import ScenarioConfig, generate_scenario
from georisk.processing.spark_stream_job import run_stream
from georisk.processing.stream_job import process_events
from georisk.storage_analytics.lake import read_dataset


@pytest.fixture
def stream_data():
    return generate_scenario(ScenarioConfig(vessel_count=3, duration_minutes=30))


def publish(root, telemetry, alerts=()):
    with FileStreamPublisher(root, flush_every=100) as publisher:
        for alert in alerts:
            publisher("weather-alerts", alert)
        for event in telemetry:
            publisher("vessel-telemetry", event)


def run(spark, root, **kwargs):
    query = run_stream(spark, root / "input", root / "output", root / "checkpoint", **kwargs)
    try:
        assert query.awaitTermination(120), "Spark stream did not finish within 120 seconds"
    finally:
        if query.isActive:
            query.stop()


def without_date(rows):
    return [{key: value for key, value in row.items() if key != "event_date"} for row in rows]


def test_stream_processes_files_once(spark, tmp_path, stream_data):
    publish(tmp_path / "input", stream_data["telemetry"], stream_data["weather_alerts"])
    run(spark, tmp_path)
    original = read_dataset(tmp_path / "output", "risk_events")
    run(spark, tmp_path)
    assert read_dataset(tmp_path / "output", "risk_events") == original
    assert len(original) == len(stream_data["telemetry"])


def test_stream_skips_duplicate_event_ids_across_batches(spark, tmp_path, stream_data):
    first, second = stream_data["telemetry"][:3], stream_data["telemetry"][3:6]
    publish(tmp_path / "input", first + first[:1], stream_data["weather_alerts"])
    run(spark, tmp_path)
    publish(tmp_path / "input", first + second)
    run(spark, tmp_path)
    rows = read_dataset(tmp_path / "output", "risk_events")
    assert len(rows) == 6
    assert {row["event_id"] for row in rows} == {row["event_id"] for row in first + second}


def test_spark_output_readable_by_lake_read_dataset(spark, tmp_path, stream_data):
    publish(tmp_path / "input", stream_data["telemetry"], stream_data["weather_alerts"])
    run(spark, tmp_path)
    rows = read_dataset(tmp_path / "output", "risk_events")
    for row in without_date(rows):
        validate_event("risk_event", row)
    expected = process_events(stream_data["telemetry"], stream_data["weather_alerts"])["risk_events"]
    assert sorted(without_date(rows), key=lambda row: row["event_id"]) == \
        sorted(expected, key=lambda row: row["event_id"])


def test_stream_quarantines_invalid_telemetry_and_alerts(spark, tmp_path, stream_data):
    good = stream_data["telemetry"][0]
    bad_alert = dict(stream_data["weather_alerts"][0], severity=6)
    publish(tmp_path / "input", [good, dict(good, event_id="bad", latitude=99)], [bad_alert])
    run(spark, tmp_path)
    rows = read_dataset(tmp_path / "output", "risk_events")
    assert len(rows) == 1 and not rows[0]["risk_alert"]
    rejected = [json.loads(line) for path in (tmp_path / "output" / "quarantine").rglob("*")
                if path.suffix in (".json", ".jsonl") for line in path.read_text().splitlines()]
    assert len(rejected) == 2
    assert {row["reason"] for row in rejected} == {"contract_violation"}


def test_stream_no_files(spark, tmp_path):
    run(spark, tmp_path)
    assert read_dataset(tmp_path / "output", "risk_events") == []

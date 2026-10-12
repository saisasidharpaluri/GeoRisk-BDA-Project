from __future__ import annotations

import json
from pathlib import Path

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


def test_stream_density_equals_batch_density_after_two_batches(spark, tmp_path, stream_data):
    first, second = stream_data["telemetry"][:6], stream_data["telemetry"][6:]
    publish(tmp_path / "input", first)
    run(spark, tmp_path)
    publish(tmp_path / "input", second + second[:1])
    run(spark, tmp_path)
    rows = without_date(read_dataset(tmp_path / "output", "density_metrics"))
    expected = process_events(first + second, [])["density_metrics"]
    key = lambda row: (row["window_start"], row["h3_index"])
    assert sorted(rows, key=key) == sorted(expected, key=key)


def test_stream_density_preserves_untouched_dates(spark, tmp_path, stream_data):
    first = stream_data["telemetry"][:3]
    next_day = [dict(row, event_id=f"next-{i}", event_time="2026-01-02T12:00:00Z")
                for i, row in enumerate(first)]
    publish(tmp_path / "input", first)
    run(spark, tmp_path)
    publish(tmp_path / "input", next_day)
    run(spark, tmp_path)
    assert {row["event_date"] for row in read_dataset(tmp_path / "output", "density_metrics")} == {
        "2026-01-01", "2026-01-02",
    }


def test_stream_quarantines_late_events(spark, tmp_path, stream_data, monkeypatch):
    monkeypatch.setenv("GEORISK_ALLOWED_LATENESS_MINUTES", "10")
    event = stream_data["telemetry"][0]
    recent = dict(event, event_id="recent", event_time="2026-01-01T12:30:00Z")
    boundary = dict(event, event_id="boundary", event_time="2026-01-01T12:20:00Z")
    late = dict(event, event_id="late", event_time="2026-01-01T12:19:59Z")
    publish(tmp_path / "input", [recent])
    run(spark, tmp_path)
    publish(tmp_path / "input", [boundary, late])
    run(spark, tmp_path)
    rows = read_dataset(tmp_path / "output", "risk_events")
    expected = process_events([recent, boundary, late], [])
    assert {row["event_id"] for row in rows} == {row["event_id"] for row in expected["risk_events"]}
    rejected = [json.loads(line) for path in (tmp_path / "output" / "quarantine" / "late").rglob("*.json")
                for line in path.read_text().splitlines()]
    assert [(row["event_id"], row["reason"]) for row in rejected] == [("late", "late_event")]
    assert json.loads((tmp_path / "checkpoint" / "georisk-watermark.json").read_text()) == {
        "max_event_time": recent["event_time"],
    }


def test_stream_invalid_and_duplicate_rows_do_not_advance_watermark(spark, tmp_path, stream_data):
    event = stream_data["telemetry"][0]
    publish(tmp_path / "input", [event])
    run(spark, tmp_path)
    publish(tmp_path / "input", [dict(event, event_time="2026-01-01T23:00:00Z"),
                                  dict(event, event_id="invalid", latitude=100,
                                       event_time="2026-01-02T12:00:00Z")])
    run(spark, tmp_path)
    assert json.loads((tmp_path / "checkpoint" / "georisk-watermark.json").read_text()) == {
        "max_event_time": event["event_time"],
    }


def test_stream_recovers_after_sink_writes_before_checkpoint_commit(
    spark, tmp_path, stream_data, monkeypatch,
):
    from pyspark.errors import StreamingQueryException

    telemetry = stream_data["telemetry"][:3]
    publish(tmp_path / "input", telemetry)
    replace = Path.replace

    def fail_watermark_once(path, target):
        if Path(target).name == "georisk-watermark.json":
            raise OSError("simulated failure after sink writes")
        return replace(path, target)

    with monkeypatch.context() as patch:
        patch.setattr(Path, "replace", fail_watermark_once)
        with pytest.raises(StreamingQueryException, match="simulated failure"):
            run(spark, tmp_path)
    assert len(read_dataset(tmp_path / "output", "risk_events")) == len(telemetry)
    run(spark, tmp_path)
    assert len(read_dataset(tmp_path / "output", "risk_events")) == len(telemetry)
    key = lambda row: (row["window_start"], row["h3_index"])
    assert sorted(without_date(read_dataset(tmp_path / "output", "density_metrics")), key=key) == \
        sorted(process_events(telemetry, [])["density_metrics"], key=key)

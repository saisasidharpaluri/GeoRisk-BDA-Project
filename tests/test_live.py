from __future__ import annotations

import json
import threading
from datetime import datetime, timedelta, timezone

import pytest

from georisk import live as live_module
from georisk.config import GeoRiskConfig
from georisk.ingestion.file_stream import FileStreamPublisher
from georisk.ingestion.scenario import ScenarioConfig, generate_scenario
from georisk.live import _BatchReader, publish_ticks, run_live, scenario_ticks
from georisk.operations import validate_report
from georisk.processing.stream_job import StreamProcessor, process_events
from georisk.route_dashboard.views import LIVE_STALL_SECONDS, live_state

UTC = timezone.utc


@pytest.fixture(scope="module")
def scenario():
    return generate_scenario(ScenarioConfig(vessel_count=12))


def _config(root, **overrides):
    return GeoRiskConfig(
        input_dir=root / "data",
        processed_dir=root / "processed",
        lake_dir=root / "lake",
        stream_dir=root / "stream",
        dashboard_dir=root,
        report_path=root / "reports" / "run.json",
        vessel_count=12,
        live_tick_seconds=0,
        **overrides,
    )


def test_scenario_ticks_group_events_by_time_with_alerts_first(scenario):
    ticks = scenario_ticks(scenario)
    times = [tick_time for tick_time, _ in ticks]
    assert times == sorted(set(times))
    assert sum(len(records) for _, records in ticks) == len(scenario["telemetry"]) + len(
        scenario["weather_alerts"]
    )
    for _, records in ticks:
        streams = [stream for stream, _ in records]
        assert streams == sorted(streams, key=lambda stream: stream != "weather_alerts")
    # H2 appears at 12:30, inside the 12:30 telemetry batch.
    half_past = next(records for tick_time, records in ticks if tick_time.minute == 30)
    assert [event["alert_id"] for stream, event in half_past if stream == "weather_alerts"] == [
        "H2"
    ]


def test_incremental_processing_matches_one_batch(scenario):
    expected = process_events(scenario["telemetry"], scenario["weather_alerts"])
    processor = StreamProcessor()
    for _, records in scenario_ticks(scenario):
        processor.add_alerts([event for stream, event in records if stream == "weather_alerts"])
        processor.add_telemetry([event for stream, event in records if stream == "telemetry"])
    result = processor.result()
    assert result["risk_events"] == expected["risk_events"]
    assert result["density_metrics"] == expected["density_metrics"]
    assert result["stats"] == expected["stats"]
    assert any(event["risk_alert"] for event in result["risk_events"])


def test_incremental_state_persists_across_batches(scenario):
    event = scenario["telemetry"][-1]
    processor = StreamProcessor(allowed_lateness_minutes=0)
    assert len(processor.add_telemetry([event])) == 1
    assert processor.add_telemetry([dict(event)]) == []
    early = dict(scenario["telemetry"][0], event_id="early")
    assert processor.add_telemetry([early]) == []
    stats = processor.result()["stats"]
    assert stats["duplicates_dropped"] == 1
    assert stats["late_dropped"] == 1
    assert stats["telemetry_in"] == 3


def test_publisher_flushes_alerts_before_telemetry(tmp_path, scenario):
    order = []
    publisher = FileStreamPublisher(tmp_path, flush_every=100)
    original = publisher._flush_stream
    publisher._flush_stream = lambda name: (order.append(name), original(name))
    publisher("vessel-telemetry", scenario["telemetry"][0])
    publisher("weather-alerts", scenario["weather_alerts"][0])
    publisher.close()
    assert order == ["weather-alerts", "vessel-telemetry"]


def test_publish_ticks_writes_one_batch_per_stream_and_tick(tmp_path, scenario):
    ticks = scenario_ticks(scenario)[:3]
    progress = {"published": 0}
    publish_ticks(ticks, tmp_path, tick_seconds=0, stop=threading.Event(), progress=progress)
    assert progress["published"] == 3
    assert len(list((tmp_path / "vessel-telemetry").glob("part-*.jsonl"))) == 3
    assert len(list((tmp_path / "weather-alerts").glob("part-*.jsonl"))) == 1
    reader = _BatchReader(tmp_path)
    alerts, telemetry = reader.poll()
    assert (len(alerts), len(telemetry)) == (1, 3)
    assert reader.poll() == ([], [])


def test_publish_ticks_stops_when_asked(tmp_path, scenario):
    stop = threading.Event()
    stop.set()
    progress = {"published": 0}
    publish_ticks(scenario_ticks(scenario), tmp_path, tick_seconds=0, stop=stop, progress=progress)
    assert progress["published"] == 0


def test_run_live_streams_processes_and_finishes_like_run(tmp_path, scenario):
    config = _config(tmp_path)
    config.processed_dir.mkdir(parents=True)
    stale = config.processed_dir / "delay-estimates.jsonl"
    stale.write_text('{"stale": true}\n', encoding="utf-8")
    old_batch = config.stream_dir / "live" / "vessel-telemetry" / "part-000099.jsonl"
    old_batch.parent.mkdir(parents=True)
    old_batch.write_text("not json\n", encoding="utf-8")
    batches = []

    report = run_live(config, tick_seconds=0.01, poll_seconds=0.001, on_batch=batches.append)

    assert report["acceptance"]["passed"], report["acceptance"]
    assert validate_report(config.report_path)["passed"]
    assert report["live"]["status"] == "complete"
    assert report["live"]["events_processed"] == len(scenario["telemetry"]) + 2
    assert report["live"]["ticks_published"] == report["live"]["ticks_total"] == 24
    assert batches and batches[-1]["scenario_time"] == "2026-01-01T13:55:00Z"
    assert not old_batch.exists()
    expected = process_events(scenario["telemetry"], scenario["weather_alerts"])
    risk_events = [
        json.loads(line)
        for line in (config.processed_dir / "risk-events.jsonl").read_text().splitlines()
    ]
    assert risk_events == expected["risk_events"]
    metadata = json.loads((config.processed_dir / "run-metadata.json").read_text())
    assert metadata["mode"] == "live"
    assert metadata["live"]["status"] == "complete"
    assert metadata["counts"]["telemetry_in"] == len(scenario["telemetry"])
    assert stale.read_text() != '{"stale": true}\n'


def test_run_live_marks_an_interrupted_run_as_stopped(tmp_path):
    config = _config(tmp_path)

    def interrupt(_):
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        run_live(config, tick_seconds=0.01, poll_seconds=0.001, on_batch=interrupt)
    metadata = json.loads((config.processed_dir / "run-metadata.json").read_text())
    assert metadata["live"]["status"] == "stopped"
    assert not config.report_path.exists()


def test_run_live_reraises_producer_failures(tmp_path, monkeypatch):
    def broken(*args, **kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(live_module, "publish_ticks", broken)
    with pytest.raises(OSError, match="disk full"):
        run_live(_config(tmp_path), poll_seconds=0.001)


def test_live_tick_seconds_is_validated():
    assert GeoRiskConfig.from_env({"GEORISK_LIVE_TICK_SECONDS": "0.5"}).live_tick_seconds == 0.5
    for value in ("-1", "61", "nan"):
        with pytest.raises(ValueError, match="GEORISK_LIVE_TICK_SECONDS"):
            GeoRiskConfig.from_env({"GEORISK_LIVE_TICK_SECONDS": value})


def test_live_state_follows_only_recent_active_runs():
    now = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    recent = (now - timedelta(seconds=5)).isoformat().replace("+00:00", "Z")
    old = (now - timedelta(seconds=LIVE_STALL_SECONDS + 1)).isoformat().replace("+00:00", "Z")
    assert live_state(None, now) is None
    assert live_state({"counts": {}}, now) is None
    running = live_state({"live": {"status": "running", "updated_at": recent}}, now)
    assert running["following"] and not running["stalled"]
    finishing = live_state({"live": {"status": "finishing", "updated_at": recent}}, now)
    assert finishing["following"]
    stalled = live_state({"live": {"status": "running", "updated_at": old}}, now)
    assert not stalled["following"] and stalled["stalled"]
    complete = live_state({"live": {"status": "complete", "updated_at": old}}, now)
    assert not complete["following"] and not complete["stalled"]
    broken = live_state({"live": {"status": "running", "updated_at": "yesterday"}}, now)
    assert broken["stalled"]

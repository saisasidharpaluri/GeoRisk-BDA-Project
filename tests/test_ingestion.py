import json
from datetime import datetime, timezone

import pytest

from georisk.contracts import ContractValidationError
from georisk.ingestion.generator import (
    generate_events,
    iter_events,
    replay_events,
    write_jsonl,
)
from georisk.ingestion.kinesis_producer import publish_event


def test_generation_is_deterministic_and_schema_valid():
    first = generate_events(seed=7, vessel_count=4)
    second = generate_events(seed=7, vessel_count=4)

    assert first == second
    assert len(first["telemetry"]) == 12
    assert len(first["weather_alerts"]) == 2
    assert all(event["event_time"].endswith("Z") for event in first["telemetry"])


def test_generation_changes_with_seed():
    assert generate_events(7, 2) != generate_events(8, 2)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"vessel_count": -1},
        {"vessel_count": 10_001},
        {"vessel_count": 1, "telemetry_points": 0},
        {"vessel_count": 1, "interval_seconds": 0},
    ],
)
def test_generation_rejects_invalid_bounds(kwargs):
    with pytest.raises((TypeError, ValueError)):
        generate_events(seed=1, **kwargs)


def test_generation_requires_utc_start_time():
    with pytest.raises(ValueError, match="UTC"):
        generate_events(
            1,
            1,
            start_time=datetime(2026, 1, 1, tzinfo=timezone.utc)
            .astimezone(timezone.utc)
            .replace(tzinfo=None),
        )


def test_replay_is_bounded_and_uses_expected_streams():
    events = generate_events(4, 2)
    published = []
    stats = replay_events(
        events, lambda stream, event: published.append((stream, event)), max_events=3
    )

    assert stats.generated == 3
    assert stats.published == 3
    assert stats.rejected == 0
    assert [stream for stream, _ in published] == [
        "weather-alerts",
        "weather-alerts",
        "vessel-telemetry",
    ]


def test_replay_surfaces_publish_failure():
    events = generate_events(4, 1)

    def fail(_stream, _event):
        raise OSError("network unavailable")

    with pytest.raises(RuntimeError, match="Replay failed"):
        replay_events(events, fail)


def test_jsonl_sink_writes_valid_separate_stream_files(tmp_path):
    events = generate_events(5, 1)
    stats = write_jsonl(events, tmp_path)

    assert stats.generated == 5
    assert stats.published == 5
    telemetry_lines = (tmp_path / "vessel-telemetry.jsonl").read_text().splitlines()
    alert_lines = (tmp_path / "weather-alerts.jsonl").read_text().splitlines()
    assert len(telemetry_lines) == 3
    assert len(alert_lines) == 2
    assert json.loads(telemetry_lines[0])["vessel_id"] == "vessel-00000"


@pytest.mark.parametrize(
    "stream_key,stream_name",
    [
        ("telemetry", "vessel-telemetry"),
        ("weather_alerts", "weather-alerts"),
    ],
)
def test_replay_counts_invalid_event_without_raising(stream_key, stream_name):
    events = generate_events(5, 1)
    invalid = dict(events[stream_key][0], event_time="invalid")
    events[stream_key].insert(0, invalid)
    published, rejected = [], []
    stats = replay_events(
        events,
        lambda stream, event: published.append((stream, event)),
        on_invalid=lambda stream, event, exc: rejected.append((stream, event, exc)),
    )
    assert (stats.generated, stats.published, stats.rejected) == (6, 5, 1)
    assert stats.generated == stats.published + stats.rejected
    assert len(published) == 5
    assert rejected[0][:2] == (stream_name, invalid)
    assert isinstance(rejected[0][2], ContractValidationError)


@pytest.mark.parametrize("invalid", [{}, None, []])
def test_write_jsonl_quarantines_invalid_event(tmp_path, invalid):
    events = generate_events(5, 1)
    events["telemetry"].insert(0, invalid)
    stats = write_jsonl(events, tmp_path)
    assert (stats.generated, stats.published, stats.rejected) == (6, 5, 1)
    entry = json.loads((tmp_path / "quarantine.jsonl").read_text(encoding="utf-8"))
    assert entry["stream"] == "vessel-telemetry"
    assert entry["record"] == invalid
    assert entry["errors"]
    assert len((tmp_path / "vessel-telemetry.jsonl").read_text().splitlines()) == 3


def test_iter_events_validation_can_be_disabled_but_stream_keys_are_required():
    events = {"weather_alerts": [{}], "telemetry": []}
    with pytest.raises(ContractValidationError):
        list(iter_events(events))
    assert list(iter_events(events, validate=False)) == [("weather_alerts", {})]
    with pytest.raises(ValueError, match="telemetry"):
        list(iter_events({"weather_alerts": []}, validate=False))


def test_replay_continues_after_publisher_failure_and_invalid_event():
    calls = []
    events = generate_events(5, 1)
    events["telemetry"].insert(0, {})

    def publish(stream, event):
        calls.append(event)
        if len(calls) == 1:
            raise OSError("failed")

    with pytest.raises(RuntimeError, match="2 of 6") as error:
        replay_events(events, publish)
    assert isinstance(error.value.__cause__, OSError)
    assert len(calls) == 5


class FakeKinesis:
    def __init__(self):
        self.calls = []

    def put_record(self, **kwargs):
        self.calls.append(kwargs)
        return {"ShardId": "shard-1", "SequenceNumber": "1"}


def test_kinesis_publisher_validates_and_sets_partition_key():
    client = FakeKinesis()
    event = generate_events(3, 1)["telemetry"][0]

    response = publish_event("vessel-telemetry", event, client=client)

    assert response["ShardId"] == "shard-1"
    assert client.calls[0]["PartitionKey"] == "vessel-00000"
    assert json.loads(client.calls[0]["Data"]) == event


def test_kinesis_publisher_rejects_invalid_stream_and_event():
    client = FakeKinesis()
    event = generate_events(3, 1)["telemetry"][0]
    event["latitude"] = 100

    with pytest.raises(ValueError, match="Unsupported stream"):
        publish_event("unknown", event, client=client)
    with pytest.raises(ContractValidationError):
        publish_event("vessel-telemetry", event, client=client)
    assert client.calls == []


def test_generator_telemetry_is_time_ordered():
    events = generate_events(7, 5, telemetry_points=6)["telemetry"]
    keys = [(event["event_time"], event["vessel_id"]) for event in events]
    assert keys == sorted(keys)
    assert events[0]["vessel_id"] == "vessel-00000"


def test_displacement_matches_speed():
    from georisk.geo_demo import haversine_nm

    telemetry = generate_events(7, 8, telemetry_points=4)["telemetry"]
    for index in range(8):
        track = [row for row in telemetry if row["vessel_id"] == f"vessel-{index:05d}"]
        for a, b in zip(track, track[1:], strict=False):
            distance = haversine_nm(a["latitude"], a["longitude"], b["latitude"], b["longitude"])
            assert distance == pytest.approx(a["speed_knots"] * 300 / 3600, rel=0.1)


def test_heading_points_toward_waypoint():
    from georisk.geo_demo import DEMO_NODES, initial_bearing_deg

    row = generate_events(7, 1)["telemetry"][0]
    target = DEMO_NODES["chokepoint" if row["longitude"] < 81.18 else "port_b"]
    expected = initial_bearing_deg(row["latitude"], row["longitude"], *target)
    assert abs((row["heading_deg"] - expected + 180) % 360 - 180) < 15


def test_motion_is_deterministic():
    assert generate_events(19, 4, telemetry_points=24) == generate_events(
        19, 4, telemetry_points=24
    )


def test_vessel_stops_at_end_of_lane():
    from georisk.geo_demo import DEMO_NODES

    track = generate_events(7, 1, telemetry_points=100)["telemetry"]
    assert track[-1]["speed_knots"] == 0
    assert (track[-1]["latitude"], track[-1]["longitude"]) == DEMO_NODES["port_b"]
    assert track[-2]["latitude"] == track[-1]["latitude"]
    assert track[-2]["longitude"] == track[-1]["longitude"]


def test_replay_pacing_uses_injected_sleep():
    events = generate_events(7, 1)
    events["weather_alerts"] = []
    sleeps = []
    replay_events(events, lambda *_: None, speed_factor=60, sleep=sleeps.append)
    assert sleeps == [5.0, 5.0]
    sleeps.clear()
    replay_events(events, lambda *_: None, speed_factor=120, sleep=sleeps.append)
    assert sleeps == [2.5, 2.5]
    sleeps.clear()
    replay_events(events, lambda *_: None, speed_factor=0, sleep=sleeps.append)
    assert sleeps == []


@pytest.mark.parametrize("speed", [-1, float("nan"), float("inf"), True])
def test_replay_rejects_invalid_speed(speed):
    with pytest.raises(ValueError, match="speed_factor"):
        replay_events(generate_events(7, 1), lambda *_: None, speed_factor=speed)


def test_file_stream_publisher_writes_complete_files_only(tmp_path, monkeypatch):
    from georisk.ingestion import file_stream

    events = generate_events(7, 1)
    original_replace = file_stream.os.replace
    replacements = []

    def observe_replace(source, destination):
        assert source.name.startswith(".part-")
        assert source.suffix == ".tmp"
        assert not destination.exists()
        records = [json.loads(line) for line in source.read_text().splitlines()]
        assert 1 <= len(records) <= 2
        replacements.append(destination)
        original_replace(source, destination)

    monkeypatch.setattr(file_stream.os, "replace", observe_replace)
    with file_stream.FileStreamPublisher(tmp_path, flush_every=2) as publisher:
        stats = replay_events(events, publisher)
        assert len(list((tmp_path / "vessel-telemetry").glob("*.jsonl"))) == 1
    assert stats.published == 5
    assert len(replacements) == 3
    assert not list(tmp_path.rglob("*.tmp"))
    telemetry_files = sorted((tmp_path / "vessel-telemetry").glob("*.jsonl"))
    assert [path.name for path in telemetry_files] == ["part-000000.jsonl", "part-000001.jsonl"]
    assert [
        json.loads(line) for path in telemetry_files for line in path.read_text().splitlines()
    ] == events["telemetry"]
    with file_stream.FileStreamPublisher(tmp_path, flush_every=2) as restarted:
        restarted("vessel-telemetry", events["telemetry"][0])
    assert (tmp_path / "vessel-telemetry" / "part-000002.jsonl").exists()
    with pytest.raises(ValueError, match="closed"):
        publisher("vessel-telemetry", events["telemetry"][0])


def test_file_stream_failed_rename_leaves_only_hidden_batch(tmp_path, monkeypatch):
    from georisk.ingestion import file_stream

    def fail(*_):
        raise OSError("rename failed")

    monkeypatch.setattr(file_stream.os, "replace", fail)
    publisher = file_stream.FileStreamPublisher(tmp_path, flush_every=1)
    with pytest.raises(OSError, match="rename failed"):
        publisher("vessel-telemetry", generate_events(7, 1)["telemetry"][0])
    assert not list(tmp_path.rglob("*.jsonl"))
    assert len(list(tmp_path.rglob(".part-*.tmp"))) == 1


def test_replay_settings_from_env(tmp_path):
    from georisk.config import GeoRiskConfig

    config = GeoRiskConfig.from_env(
        {"GEORISK_STREAM_DIR": str(tmp_path), "GEORISK_REPLAY_SPEED_FACTOR": "60"}
    )
    assert config.stream_dir == tmp_path
    assert config.replay_speed_factor == 60
    for value in ("-1", "nan", "inf"):
        with pytest.raises(ValueError, match="GEORISK_REPLAY_SPEED_FACTOR"):
            GeoRiskConfig.from_env({"GEORISK_REPLAY_SPEED_FACTOR": value})


@pytest.mark.parametrize("flush_every", [0, -1, True, 1.5])
def test_file_stream_rejects_invalid_batch_size(tmp_path, flush_every):
    from georisk.ingestion.file_stream import FileStreamPublisher

    with pytest.raises(ValueError, match="flush_every"):
        FileStreamPublisher(tmp_path, flush_every=flush_every)
    assert not list(tmp_path.iterdir())


def test_file_stream_rejects_unknown_stream_and_reopening(tmp_path):
    from georisk.ingestion.file_stream import FileStreamPublisher

    publisher = FileStreamPublisher(tmp_path)
    with pytest.raises(ValueError, match="stream_name"):
        publisher("../outside", {})
    publisher.close()
    with pytest.raises(ValueError, match="closed"):
        with publisher:
            pytest.fail("closed publisher must not be reopened")
    assert not list(tmp_path.iterdir())

import json
from datetime import datetime, timezone

import pytest

from georisk.contracts import ContractValidationError
from georisk.ingestion.generator import (
    generate_events,
    replay_events,
    write_jsonl,
    iter_events,
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
            start_time=datetime(2026, 1, 1, tzinfo=timezone.utc).astimezone(
                timezone.utc
            ).replace(tzinfo=None),
        )


def test_replay_is_bounded_and_uses_expected_streams():
    events = generate_events(4, 2)
    published = []
    stats = replay_events(events, lambda stream, event: published.append((stream, event)), max_events=3)

    assert stats.generated == 3
    assert stats.published == 3
    assert stats.rejected == 0
    assert [stream for stream, _ in published] == ["weather-alerts", "weather-alerts", "vessel-telemetry"]


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


@pytest.mark.parametrize("stream_key,stream_name", [
    ("telemetry", "vessel-telemetry"), ("weather_alerts", "weather-alerts"),
])
def test_replay_counts_invalid_event_without_raising(stream_key, stream_name):
    events = generate_events(5, 1)
    invalid = dict(events[stream_key][0], event_time="invalid")
    events[stream_key].insert(0, invalid)
    published, rejected = [], []
    stats = replay_events(
        events, lambda stream, event: published.append((stream, event)),
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

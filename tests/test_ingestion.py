import json
from datetime import datetime, timezone

import pytest

from georisk.contracts import ContractValidationError
from georisk.ingestion.generator import (
    generate_events,
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

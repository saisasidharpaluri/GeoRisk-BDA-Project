"""Validated Amazon Kinesis publishing for GeoRisk events."""

from __future__ import annotations

import json
from typing import Any

from georisk.contracts import validate_event

STREAM_CONTRACTS = {
    "vessel-telemetry": "vessel_telemetry",
    "weather-alerts": "weather_alert",
}


def publish_event(
    stream_name: str,
    event: dict[str, Any],
    *,
    client: Any | None = None,
    partition_key: str | None = None,
) -> dict[str, Any]:
    """Validate and publish one event to Kinesis.

    ``client`` is injectable for local tests. If omitted, boto3 creates the
    default Kinesis client using the active AWS profile or environment.
    """
    contract = STREAM_CONTRACTS.get(stream_name)
    if contract is None:
        supported = ", ".join(sorted(STREAM_CONTRACTS))
        raise ValueError(f"Unsupported stream '{stream_name}'. Expected: {supported}")
    validate_event(contract, event)

    if client is None:
        try:
            import boto3
        except ImportError as exc:
            raise RuntimeError(
                "boto3 is required for AWS publishing; inject a client for local use"
            ) from exc
        client = boto3.client("kinesis")

    key = partition_key or _partition_key(contract, event)
    if not key:
        raise ValueError("partition_key must not be empty")
    return client.put_record(
        StreamName=stream_name,
        Data=(json.dumps(event, separators=(",", ":")) + "\n").encode("utf-8"),
        PartitionKey=key,
    )


def _partition_key(contract: str, event: dict[str, Any]) -> str:
    if contract == "vessel_telemetry":
        return event["vessel_id"]
    return event["alert_id"]

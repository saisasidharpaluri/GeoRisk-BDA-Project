"""Partitioned Parquet lake helpers for local and S3-backed workflows."""

from __future__ import annotations

import io
import json
from datetime import date
from pathlib import Path
from typing import Any, Iterable, Mapping
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from georisk.contracts import validate_event

CONTRACT_BY_DATASET = {
    "vessel_telemetry": "vessel_telemetry",
    "weather_alerts": "weather_alert",
    "risk_events": "risk_event",
    "density_metrics": "density_metric",
}


def _validate_event_date(event_date: str) -> str:
    try:
        parsed = date.fromisoformat(event_date)
    except ValueError as exc:
        raise ValueError("event_date must use YYYY-MM-DD format") from exc
    return parsed.isoformat()


def _dataset_name(records: list[Mapping[str, Any]], explicit: str | None) -> str:
    if explicit:
        if explicit not in CONTRACT_BY_DATASET:
            raise ValueError(f"Unsupported dataset '{explicit}'")
        return explicit
    if not records:
        raise ValueError("records cannot be empty when dataset_name is omitted")
    keys = set(records[0])
    if {"matching_hazards", "risk_alert"} <= keys:
        return "risk_events"
    if {"window_start", "unique_vessel_count"} <= keys:
        return "density_metrics"
    if {"alert_id", "valid_until", "geometry"} <= keys:
        return "weather_alerts"
    if {"vessel_id", "latitude", "longitude"} <= keys:
        return "vessel_telemetry"
    raise ValueError("unable to infer dataset name from record fields")


def _validate_records(records: list[Mapping[str, Any]], dataset_name: str) -> None:
    contract = CONTRACT_BY_DATASET[dataset_name]
    for record in records:
        validate_event(contract, record)


def _local_partition(root: Path, dataset_name: str, event_date: str) -> Path:
    return root / dataset_name / f"event_date={event_date}"


def write_dataset(
    records: Iterable[Mapping[str, Any]],
    s3_uri: str | Path,
    event_date: str,
    *,
    dataset_name: str | None = None,
) -> str:
    """Write validated records to a date-partitioned Parquet dataset.

    Local destinations use a directory path or ``file://`` URI. S3 destinations
    upload one Parquet object and require an installed boto3 client.
    """
    if not isinstance(s3_uri, (str, Path)) or not str(s3_uri):
        raise ValueError("s3_uri must be a non-empty string")
    s3_uri = str(s3_uri)
    event_date = _validate_event_date(event_date)
    materialized = [dict(record) for record in records]
    name = _dataset_name(materialized, dataset_name)
    _validate_records(materialized, name)
    table = pa.Table.from_pylist(materialized)
    parsed = urlparse(s3_uri)

    if parsed.scheme == "s3":
        try:
            import boto3
        except ImportError as exc:
            raise RuntimeError("boto3 is required for s3:// destinations") from exc
        key = f"{parsed.path.lstrip('/')}/{name}/event_date={event_date}/part-00000.parquet"
        buffer = io.BytesIO()
        pq.write_table(table, buffer, compression="snappy")
        boto3.client("s3").put_object(
            Bucket=parsed.netloc, Key=key, Body=buffer.getvalue()
        )
        return f"s3://{parsed.netloc}/{key}"

    is_windows_path = (
        len(parsed.scheme) == 1
        and len(s3_uri) > 2
        and s3_uri[1] == ":"
        and s3_uri[2] in {"\\", "/"}
    )
    if parsed.scheme not in {"", "file"} and not is_windows_path:
        raise ValueError("destination must be a local path, file:// URI, or s3:// URI")
    root = Path(parsed.path if parsed.scheme == "file" else s3_uri)
    partition = _local_partition(root, name, event_date)
    partition.mkdir(parents=True, exist_ok=True)
    output = partition / "part-00000.parquet"
    pq.write_table(table, output, compression="snappy")
    return str(output)


def read_dataset(root: str | Path, dataset_name: str) -> list[dict[str, Any]]:
    """Read one local Parquet dataset, including its event-date partition."""
    if dataset_name not in CONTRACT_BY_DATASET:
        raise ValueError(f"Unsupported dataset '{dataset_name}'")
    path = Path(root) / dataset_name
    if not path.exists():
        return []
    table = ds.dataset(path, format="parquet", partitioning="hive").to_table()
    return table.to_pylist()


def risk_summary(root: str | Path) -> dict[str, int]:
    """Return simple dashboard-ready risk counts from curated Parquet data."""
    records = read_dataset(root, "risk_events")
    return {
        "risk_event_count": len(records),
        "alerted_event_count": sum(bool(row["risk_alert"]) for row in records),
        "vessel_count": len({row["vessel_id"] for row in records}),
        "exposed_vessel_count": len(
            {row["vessel_id"] for row in records if row["risk_alert"]}
        ),
    }


def density_summary(root: str | Path) -> list[dict[str, Any]]:
    """Return density records sorted for historical and dashboard use."""
    return sorted(
        read_dataset(root, "density_metrics"),
        key=lambda row: (row["window_start"], row["h3_index"]),
    )

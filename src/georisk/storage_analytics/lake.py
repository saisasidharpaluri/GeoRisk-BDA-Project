"""Partitioned Parquet lake helpers for local and S3-backed workflows."""

from __future__ import annotations

import io
import re
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from georisk.contracts import validate_event
from georisk.storage_analytics.arrow_schemas import (
    ARROW_SCHEMAS, from_arrow_rows, to_arrow_rows,
)

CONTRACT_BY_DATASET = {
    "vessel_telemetry": "vessel_telemetry",
    "weather_alerts": "weather_alert",
    "risk_events": "risk_event",
    "density_metrics": "density_metric",
    "delay_estimates": "delay_estimate",
}

ZONE_BY_DATASET = {
    "vessel_telemetry": "raw",
    "weather_alerts": "raw",
    "risk_events": "curated",
    "density_metrics": "curated",
    "delay_estimates": "curated",
}


def _validate_batch_id(batch_id: str) -> None:
    if not isinstance(batch_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]+", batch_id):
        raise ValueError("batch_id must contain only letters, digits, underscores, or hyphens")


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
    if {"predicted_delay_hours", "model_name", "split"} <= keys:
        return "delay_estimates"
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
    return root / ZONE_BY_DATASET[dataset_name] / dataset_name / f"event_date={event_date}"


def write_dataset(
    records: Iterable[Mapping[str, Any]],
    s3_uri: str | Path,
    event_date: str,
    *,
    dataset_name: str | None = None,
    batch_id: str = "00000",
) -> str:
    """Write validated records to a date-partitioned Parquet dataset.

    Local destinations use a directory path or ``file://`` URI. S3 destinations
    upload one Parquet object and require an installed boto3 client.
    """
    if not isinstance(s3_uri, (str, Path)) or not str(s3_uri):
        raise ValueError("s3_uri must be a non-empty string")
    s3_uri = str(s3_uri)
    _validate_batch_id(batch_id)
    event_date = _validate_event_date(event_date)
    materialized = [dict(record) for record in records]
    name = _dataset_name(materialized, dataset_name)
    _validate_records(materialized, name)
    table = pa.Table.from_pylist(to_arrow_rows(name, materialized), schema=ARROW_SCHEMAS[name])
    parsed = urlparse(s3_uri)

    if parsed.scheme == "s3":
        try:
            import boto3
        except ImportError as exc:
            raise RuntimeError("boto3 is required for s3:// destinations") from exc
        key = f"{parsed.path.strip('/')}/{ZONE_BY_DATASET[name]}/{name}/event_date={event_date}/part-{batch_id}.parquet".lstrip("/")
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
    output = partition / f"part-{batch_id}.parquet"
    pq.write_table(table, output, compression="snappy")
    return str(output)


def write_partitioned(
    records: Iterable[Mapping[str, Any]],
    root: str | Path,
    *,
    dataset_name: str,
    batch_id: str = "00000",
) -> list[str]:
    """Write each row to its UTC date; replace the same batch in each partition.

    Empty input writes no files. Other batches and dates are left untouched.
    """
    if not isinstance(root, (str, Path)) or not str(root):
        raise ValueError("root must be a non-empty string or Path")
    _validate_batch_id(batch_id)
    materialized = [dict(record) for record in records]
    name = _dataset_name(materialized, dataset_name)
    _validate_records(materialized, name)
    timestamp_field = "window_start" if name == "density_metrics" else "event_time"
    partitions: dict[str, list[dict[str, Any]]] = {}
    for record in materialized:
        timestamp = datetime.fromisoformat(record[timestamp_field].replace("Z", "+00:00"))
        event_date = timestamp.astimezone(timezone.utc).date().isoformat()
        partitions.setdefault(event_date, []).append(record)
    return [
        write_dataset(rows, root, event_date, dataset_name=name, batch_id=batch_id)
        for event_date, rows in sorted(partitions.items())
    ]


def read_dataset(root: str | Path, dataset_name: str) -> list[dict[str, Any]]:
    """Read one local Parquet dataset, including its event-date partition."""
    if dataset_name not in CONTRACT_BY_DATASET:
        raise ValueError(f"Unsupported dataset '{dataset_name}'")
    path = Path(root) / ZONE_BY_DATASET[dataset_name] / dataset_name
    if not path.exists():
        return []
    table = ds.dataset(
        path,
        format="parquet",
        schema=ARROW_SCHEMAS[dataset_name].append(pa.field("event_date", pa.string())),
        partitioning=ds.partitioning(pa.schema([("event_date", pa.string())]), flavor="hive"),
    ).to_table()
    return from_arrow_rows(dataset_name, table.to_pylist())


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

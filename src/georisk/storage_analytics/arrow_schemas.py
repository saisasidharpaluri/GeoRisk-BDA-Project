"""Stable, nullable Parquet schemas and GeoJSON storage conversion."""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from typing import Any

import pyarrow as pa

ARROW_SCHEMAS: dict[str, pa.Schema] = {
    "delay_estimates": pa.schema(
        [
            ("schema_version", pa.int32()),
            ("event_id", pa.string()),
            ("event_time", pa.string()),
            ("vessel_id", pa.string()),
            ("split", pa.string()),
            ("model_name", pa.string()),
            ("predicted_delay_hours", pa.float64()),
            ("baseline_delay_hours", pa.float64()),
            ("synthetic_label_hours", pa.float64()),
            ("baseline_hours", pa.float64()),
            ("density", pa.int64()),
            ("hazard_severity", pa.int32()),
            ("speed_knots", pa.float64()),
            ("route_distance_nm", pa.float64()),
        ]
    ),
    "vessel_telemetry": pa.schema(
        [
            ("schema_version", pa.int32()),
            ("event_id", pa.string()),
            ("event_time", pa.string()),
            ("vessel_id", pa.string()),
            ("latitude", pa.float64()),
            ("longitude", pa.float64()),
            ("speed_knots", pa.float64()),
            ("heading_deg", pa.float64()),
        ]
    ),
    "weather_alerts": pa.schema(
        [
            ("schema_version", pa.int32()),
            ("event_id", pa.string()),
            ("alert_id", pa.string()),
            ("event_time", pa.string()),
            ("valid_from", pa.string()),
            ("valid_until", pa.string()),
            ("hazard_type", pa.string()),
            ("severity", pa.int32()),
            ("geometry_json", pa.string()),
        ]
    ),
    "risk_events": pa.schema(
        [
            ("schema_version", pa.int32()),
            ("event_time", pa.string()),
            ("event_id", pa.string()),
            ("latitude", pa.float64()),
            ("longitude", pa.float64()),
            ("coverage_mode", pa.string()),
            ("vessel_id", pa.string()),
            ("h3_index", pa.string()),
            ("h3_resolution", pa.int32()),
            ("risk_alert", pa.bool_()),
            (
                "matching_hazards",
                pa.list_(
                    pa.struct(
                        [
                            ("alert_id", pa.string()),
                            ("hazard_type", pa.string()),
                            ("severity", pa.int32()),
                            ("valid_from", pa.string()),
                            ("valid_until", pa.string()),
                        ]
                    )
                ),
            ),
        ]
    ),
    "density_metrics": pa.schema(
        [
            ("schema_version", pa.int32()),
            ("window_start", pa.string()),
            ("window_end", pa.string()),
            ("h3_index", pa.string()),
            ("h3_resolution", pa.int32()),
            ("unique_vessel_count", pa.int64()),
        ]
    ),
}


def to_arrow_rows(dataset: str, records: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Copy records, encoding mixed GeoJSON geometries as JSON strings."""
    rows = [dict(record) for record in records]
    if dataset == "weather_alerts":
        for row in rows:
            row["geometry_json"] = json.dumps(row.pop("geometry"), sort_keys=True)
    return rows


def from_arrow_rows(dataset: str, rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Copy stored rows and restore GeoJSON geometry objects."""
    records = [dict(row) for row in rows]
    if dataset == "weather_alerts":
        for record in records:
            record["geometry"] = json.loads(record.pop("geometry_json"))
    return records

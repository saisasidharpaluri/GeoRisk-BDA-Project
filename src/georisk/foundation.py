"""Small local H3 risk check used to establish the GeoRisk-Spark foundation."""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

import h3


def _read_json_lines(path: Path) -> Iterable[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, start=1):
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"{path}:{line_number}: invalid JSON: {exc.msg}") from exc
            if not isinstance(value, dict):
                raise ValueError(f"{path}:{line_number}: expected a JSON object")
            yield value


def _validate_telemetry(record: dict[str, Any], index: int) -> None:
    required = ("vessel_id", "event_time", "latitude", "longitude")
    missing = [field for field in required if field not in record]
    if missing:
        raise ValueError(f"telemetry record {index}: missing {', '.join(missing)}")
    try:
        latitude = float(record["latitude"])
        longitude = float(record["longitude"])
    except (TypeError, ValueError) as exc:
        raise ValueError(f"telemetry record {index}: coordinates must be numeric") from exc
    if not -90 <= latitude <= 90 or not -180 <= longitude <= 180:
        raise ValueError(f"telemetry record {index}: coordinates are out of range")
    timestamp = str(record["event_time"]).replace("Z", "+00:00")
    try:
        datetime.fromisoformat(timestamp)
    except ValueError as exc:
        raise ValueError(f"telemetry record {index}: event_time must be ISO 8601") from exc


def _polygon_cells(geometry: dict[str, Any], resolution: int) -> set[str]:
    """Return H3 cells whose centers are inside a GeoJSON Polygon/MultiPolygon."""
    kind = geometry.get("type")
    coordinates = geometry.get("coordinates")
    if kind not in {"Polygon", "MultiPolygon"} or not coordinates:
        raise ValueError("hazard geometry must be a non-empty GeoJSON Polygon or MultiPolygon")

    polygons = [coordinates] if kind == "Polygon" else coordinates
    cells: set[str] = set()
    for polygon in polygons:
        if not polygon or not polygon[0]:
            continue
        # GeoJSON order is [longitude, latitude]; H3 LatLngPoly expects [latitude, longitude].
        shell = [(float(lat), float(lon)) for lon, lat in polygon[0]]
        holes = [
            [(float(lat), float(lon)) for lon, lat in ring]
            for ring in polygon[1:]
        ]
        shape = h3.LatLngPoly(shell, *holes)
        cells.update(h3.polygon_to_cells(shape, resolution))
    return cells


def run(telemetry_path: Path, hazard_path: Path, output_path: Path, resolution: int) -> int:
    if not 0 <= resolution <= 15:
        raise ValueError("H3 resolution must be between 0 and 15")

    with hazard_path.open("r", encoding="utf-8") as stream:
        hazard_document = json.load(stream)
    if hazard_document.get("type") != "FeatureCollection":
        raise ValueError("hazard file must be a GeoJSON FeatureCollection")

    hazard_cells: dict[str, list[dict[str, Any]]] = {}
    for feature in hazard_document.get("features", []):
        properties = feature.get("properties") or {}
        hazard_id = properties.get("alert_id", "unknown-alert")
        covered_cells = _polygon_cells(feature.get("geometry") or {}, resolution)
        for cell in covered_cells:
            hazard_cells.setdefault(cell, []).append(
                {
                    "alert_id": hazard_id,
                    "hazard_type": properties.get("hazard_type", "unknown"),
                    "severity": properties.get("severity"),
                }
            )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    total = 0
    with output_path.open("w", encoding="utf-8", newline="\n") as output:
        for total, vessel in enumerate(_read_json_lines(telemetry_path), start=1):
            _validate_telemetry(vessel, total)
            latitude = float(vessel["latitude"])
            longitude = float(vessel["longitude"])
            cell = h3.latlng_to_cell(latitude, longitude, resolution)
            matches = hazard_cells.get(cell, [])
            result = {
                "schema_version": 1,
                "vessel_id": vessel["vessel_id"],
                "event_time": vessel["event_time"],
                "latitude": latitude,
                "longitude": longitude,
                "h3_index": cell,
                "h3_resolution": resolution,
                "risk_alert": bool(matches),
                "matching_hazards": matches,
                "risk_note": "H3 cell overlap is approximate; this is a demonstration result.",
            }
            output.write(json.dumps(result, separators=(",", ":")) + "\n")
    return total


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--telemetry", type=Path, required=True, help="Input vessel JSON Lines")
    parser.add_argument("--hazards", type=Path, required=True, help="Input GeoJSON FeatureCollection")
    parser.add_argument("--output", type=Path, required=True, help="Output risk-event JSON Lines")
    parser.add_argument("--resolution", type=int, default=7, help="H3 resolution (default: 7)")
    args = parser.parse_args()
    count = run(args.telemetry, args.hazards, args.output, args.resolution)
    print(f"Processed {count} vessel records; results written to {args.output}")


if __name__ == "__main__":
    main()

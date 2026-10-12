"""H3 indexing helpers used by local and streaming processors."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import h3

DEFAULT_RESOLUTION = 7
MIN_RESOLUTION = 0
MAX_RESOLUTION = 15
COVERAGE_MODES = ("center", "overlap")
DEFAULT_COVERAGE_MODE = "overlap"


def _validate_resolution(resolution: int) -> None:
    if not isinstance(resolution, int) or isinstance(resolution, bool):
        raise TypeError("resolution must be an integer")
    if not MIN_RESOLUTION <= resolution <= MAX_RESOLUTION:
        raise ValueError(
            f"resolution must be between {MIN_RESOLUTION} and {MAX_RESOLUTION}"
        )


def point_to_cell(latitude: float, longitude: float, resolution: int) -> str:
    """Return the H3 cell for a latitude/longitude point."""
    _validate_resolution(resolution)
    if not isinstance(latitude, (int, float)) or not -90 <= latitude <= 90:
        raise ValueError("latitude must be between -90 and 90")
    if not isinstance(longitude, (int, float)) or not -180 <= longitude <= 180:
        raise ValueError("longitude must be between -180 and 180")
    return h3.latlng_to_cell(float(latitude), float(longitude), resolution)


def _validate_geometry(geojson_geometry: Mapping[str, Any]) -> None:
    if not isinstance(geojson_geometry, Mapping):
        raise TypeError("geojson_geometry must be a mapping")
    if geojson_geometry.get("type") not in {"Polygon", "MultiPolygon"}:
        raise ValueError("geometry type must be Polygon or MultiPolygon")
    coordinates = geojson_geometry.get("coordinates")
    if not isinstance(coordinates, list) or not coordinates:
        raise ValueError("geometry coordinates must be a non-empty list")


def polygon_to_cells(
    geojson_geometry: dict, resolution: int,
    coverage_mode: str = DEFAULT_COVERAGE_MODE,
) -> set[str]:
    """Return candidate cells using center containment or polygon overlap."""
    _validate_resolution(resolution)
    if coverage_mode not in COVERAGE_MODES:
        raise ValueError("coverage_mode must be center or overlap")
    _validate_geometry(geojson_geometry)
    try:
        if coverage_mode == "center":
            return set(h3.geo_to_cells(geojson_geometry, resolution))
        return set(h3.h3shape_to_cells_experimental(
            h3.geo_to_h3shape(geojson_geometry), resolution, contain="overlap"
        ))
    except (TypeError, ValueError, h3.H3BaseException) as exc:
        raise ValueError(f"invalid GeoJSON geometry: {exc}") from exc

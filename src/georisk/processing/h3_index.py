"""H3 indexing helpers used by local and streaming processors."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import h3

DEFAULT_RESOLUTION = 7
MIN_RESOLUTION = 0
MAX_RESOLUTION = 15


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


def polygon_to_cells(geojson_geometry: dict, resolution: int) -> set[str]:
    """Return H3 cells covering a GeoJSON Polygon or MultiPolygon.

    H3 uses cell-center containment by default. The returned set is therefore
    a candidate cover and is not an exact polygon intersection result.
    """
    _validate_resolution(resolution)
    _validate_geometry(geojson_geometry)
    try:
        return set(h3.geo_to_cells(geojson_geometry, resolution))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid GeoJSON geometry: {exc}") from exc

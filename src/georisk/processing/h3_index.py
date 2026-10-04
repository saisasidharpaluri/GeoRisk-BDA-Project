"""TODO: implement H3 indexing helpers for vessel points and hazard polygons."""


def point_to_cell(latitude: float, longitude: float, resolution: int) -> str:
    """Return the H3 cell for a latitude/longitude point."""
    raise NotImplementedError("Member 2 owns H3 indexing")


def polygon_to_cells(geojson_geometry: dict, resolution: int) -> set[str]:
    """Return the selected H3 coverage cells for a GeoJSON hazard geometry."""
    raise NotImplementedError("Member 2 owns hazard coverage")

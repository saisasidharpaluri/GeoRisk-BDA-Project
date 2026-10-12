"""Shared offshore Chennai geography for the synthetic demonstration."""

from __future__ import annotations

import math

DEMO_NODES = {
    "port_a": (13.10, 80.45),
    "chokepoint": (13.10, 81.20),
    "outer_lane": (13.80, 81.40),
    "port_b": (13.10, 82.00),
}
DEMO_PATHS = {
    "inner": ["port_a", "chokepoint", "port_b"],
    "outer": ["port_a", "outer_lane", "port_b"],
}
DEMO_SPEED_KNOTS = 14.0
DEMO_BBOX = {"lat_min": 12.5, "lat_max": 14.0, "lon_min": 80.5, "lon_max": 82.2}


def haversine_nm(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Return great-circle distance in nautical miles."""
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    a = (
        math.sin((phi2 - phi1) / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(math.radians(lon2 - lon1) / 2) ** 2
    )
    return 2 * 3440.065 * math.asin(math.sqrt(min(1.0, max(0.0, a))))


def initial_bearing_deg(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Return the initial great-circle bearing in [0, 360)."""
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    delta = math.radians(lon2 - lon1)
    y = math.sin(delta) * math.cos(phi2)
    x = math.cos(phi1) * math.sin(phi2) - math.sin(phi1) * math.cos(phi2) * math.cos(delta)
    return math.degrees(math.atan2(y, x)) % 360

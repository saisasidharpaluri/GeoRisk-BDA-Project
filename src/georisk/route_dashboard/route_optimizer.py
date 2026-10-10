"""Deterministic risk-aware routing for the GeoRisk demonstration graph."""

from __future__ import annotations

import heapq
import math
from dataclasses import dataclass
from typing import Any, Mapping


@dataclass(frozen=True)
class RouteResult:
    """Comparable route output suitable for APIs and dashboard rendering."""

    path: tuple[str, ...]
    travel_hours: float
    risk_score: float
    total_cost: float
    blocked_edges: tuple[tuple[str, str], ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "path": list(self.path),
            "travel_hours": self.travel_hours,
            "risk_score": self.risk_score,
            "total_cost": self.total_cost,
            "blocked_edges": [list(edge) for edge in self.blocked_edges],
        }


def demonstration_graph() -> dict[str, list[dict[str, Any]]]:
    """Return the documented four-node synthetic sea-lane graph."""
    return {
        "port_a": [
            {"to": "chokepoint", "travel_hours": 8.0, "risk": 0.9},
            {"to": "outer_lane", "travel_hours": 11.0, "risk": 0.1},
        ],
        "chokepoint": [
            {"to": "port_b", "travel_hours": 8.0, "risk": 0.9},
        ],
        "outer_lane": [
            {"to": "port_b", "travel_hours": 13.0, "risk": 0.1},
        ],
        "port_b": [],
    }


def _validate_graph(graph: Mapping[str, list[Mapping[str, Any]]]) -> None:
    if not isinstance(graph, Mapping) or not graph:
        raise ValueError("graph must be a non-empty mapping")
    for source, edges in graph.items():
        if not isinstance(source, str) or not source:
            raise ValueError("graph node names must be non-empty strings")
        if not isinstance(edges, list):
            raise TypeError(f"edges for '{source}' must be a list")
        for edge in edges:
            if not isinstance(edge, Mapping):
                raise TypeError("each graph edge must be a mapping")
            destination = edge.get("to")
            if destination not in graph:
                raise ValueError(f"edge from '{source}' targets unknown node '{destination}'")
            for field in ("travel_hours", "risk"):
                value = edge.get(field)
                if not isinstance(value, (int, float)) or not math.isfinite(value):
                    raise ValueError(f"edge '{source}->{destination}' has invalid {field}")
                if value < 0:
                    raise ValueError(f"edge '{source}->{destination}' {field} cannot be negative")


def _shortest_path(
    graph: Mapping[str, list[Mapping[str, Any]]],
    origin: str,
    destination: str,
    risk_threshold: float | None,
    risk_penalty: float,
) -> RouteResult:
    queue: list[tuple[float, tuple[str, ...], float, float, tuple[tuple[str, str], ...]]] = [
        (0.0, (origin,), 0.0, 0.0, ())
    ]
    best: dict[str, float] = {origin: 0.0}
    while queue:
        cost, path, travel, risk, blocked = heapq.heappop(queue)
        current = path[-1]
        if current == destination:
            return RouteResult(path, travel, risk, cost, blocked)
        if cost > best.get(current, math.inf) + 1e-12:
            continue
        for edge in graph[current]:
            target = edge["to"]
            edge_risk = float(edge["risk"])
            is_blocked = risk_threshold is not None and edge_risk > risk_threshold
            if is_blocked:
                continue
            next_cost = cost + float(edge["travel_hours"]) + risk_penalty * edge_risk
            if next_cost < best.get(target, math.inf) - 1e-12:
                best[target] = next_cost
                heapq.heappush(
                    queue,
                    (
                        next_cost,
                        path + (target,),
                        travel + float(edge["travel_hours"]),
                        risk + edge_risk,
                        blocked,
                    ),
                )
    raise ValueError(f"no eligible route from '{origin}' to '{destination}'")


def find_alternative_route(
    graph: Mapping[str, list[Mapping[str, Any]]],
    origin: str,
    destination: str,
    risk_threshold: float,
    *,
    risk_penalty: float = 4.0,
) -> dict[str, Any]:
    """Compare the fastest baseline with the safest eligible alternative.

    Edges above ``risk_threshold`` are unavailable to the alternative route.
    The baseline remains the fastest unrestricted route, making the comparison
    explicit rather than silently changing the user's original route.
    """
    _validate_graph(graph)
    if origin not in graph or destination not in graph:
        raise ValueError("origin and destination must be graph nodes")
    if not isinstance(risk_threshold, (int, float)) or not math.isfinite(risk_threshold):
        raise ValueError("risk_threshold must be a finite number")
    if risk_penalty < 0 or not math.isfinite(risk_penalty):
        raise ValueError("risk_penalty must be a non-negative finite number")
    baseline = _shortest_path(graph, origin, destination, None, 0.0)
    alternative = _shortest_path(
        graph, origin, destination, float(risk_threshold), float(risk_penalty)
    )
    return {
        "origin": origin,
        "destination": destination,
        "risk_threshold": float(risk_threshold),
        "risk_penalty": float(risk_penalty),
        "baseline": baseline.as_dict(),
        "alternative": alternative.as_dict(),
        "route_changed": baseline.path != alternative.path,
        "travel_hour_delta": alternative.travel_hours - baseline.travel_hours,
        "risk_delta": alternative.risk_score - baseline.risk_score,
        "synthetic_demo": True,
    }

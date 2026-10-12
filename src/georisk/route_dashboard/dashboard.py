"""Local Streamlit dashboard and dashboard data-loading helpers."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from georisk.config import GeoRiskConfig
from georisk.route_dashboard.edge_risk import score_graph
from georisk.route_dashboard.route_optimizer import (
    demonstration_graph, demonstration_nodes, find_alternative_route,
)


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows = []
    with path.open(encoding="utf-8") as source:
        for line_number, line in enumerate(source, 1):
            if not line.strip():
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"invalid JSON in {path}:{line_number}") from exc
            if not isinstance(record, dict):
                raise ValueError(f"expected an object in {path}:{line_number}")
            rows.append(record)
    return rows


def load_dashboard_data(
    root: str | Path | None = None, *, input_dir: str | Path | None = None,
    processed_dir: str | Path | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """Load Phase 2/3 local files without hiding missing-data states."""
    base = Path(root) if root is not None else Path("output")
    inputs = Path(input_dir) if input_dir is not None else base / "data"
    processed = Path(processed_dir) if processed_dir is not None else base / "processed"
    return {
        "telemetry": _read_jsonl(inputs / "vessel-telemetry.jsonl"),
        "weather_alerts": _read_jsonl(inputs / "weather-alerts.jsonl"),
        "risk_events": _read_jsonl(processed / "risk-events.jsonl"),
        "density_metrics": _read_jsonl(processed / "density-metrics.jsonl"),
    }


def dashboard_summary(data: dict[str, list[dict[str, Any]]], *, resolution: int = 7) -> dict[str, Any]:
    """Create metric values and freshness information for UI or API use."""
    risk_events = data.get("risk_events", [])
    latest_values = [
        row.get("event_time")
        for row in (*data.get("telemetry", []), *risk_events)
        if row.get("event_time")
    ]
    latest = max(latest_values) if latest_values else None
    return {
        "simulated": True,
        "h3_resolution": resolution,
        "units": {"delay": "hours", "speed": "knots", "distance": "nm"},
        "telemetry_count": len(data.get("telemetry", [])),
        "active_alert_count": len(data.get("weather_alerts", [])),
        "risk_event_count": len(risk_events),
        "exposed_vessel_count": len(
            {row["vessel_id"] for row in risk_events if row.get("risk_alert")}
        ),
        "density_cell_count": len(
            {row["h3_index"] for row in data.get("density_metrics", [])}
        ),
        "latest_event_time": latest,
        "has_data": bool(latest_values),
    }


def dashboard_route(
    data: dict[str, list[dict[str, Any]]], *, resolution: int, coverage_mode: str,
    risk_threshold: float, risk_penalty: float,
) -> dict[str, Any] | None:
    """Score the demo graph at the latest telemetry time for the UI and report."""
    if not data["telemetry"]:
        return None
    at_time = max(event["event_time"] for event in data["telemetry"])
    graph = score_graph(
        demonstration_graph(), demonstration_nodes(), data["weather_alerts"],
        data["density_metrics"], at_time=at_time, resolution=resolution,
        coverage_mode=coverage_mode,
    )
    route = find_alternative_route(graph, "port_a", "port_b", risk_threshold,
                                   risk_penalty=risk_penalty)
    return {**route, "at_time": at_time, "edge_scores": graph}


def _render_map(st: Any, telemetry: list[dict[str, Any]]) -> None:
    points = [
        {"lat": row["latitude"], "lon": row["longitude"], "vessel_id": row["vessel_id"]}
        for row in telemetry
        if "latitude" in row and "longitude" in row
    ]
    if points:
        st.map(points, latitude="lat", longitude="lon", size=20)
    else:
        st.info("No vessel telemetry is available for the map.")


def main() -> None:
    """Render the local dashboard; cloud query adapters can replace the loader."""
    try:
        import streamlit as st
    except ImportError as exc:
        raise RuntimeError(
            "Streamlit is required to run the dashboard. Install the dashboard extra."
        ) from exc

    settings = GeoRiskConfig.from_env()
    st.set_page_config(page_title="GeoRisk-Spark", layout="wide")
    st.title("GeoRisk-Spark")
    st.caption("Synthetic demonstration data | H3 candidate risk screen")
    input_dir = st.sidebar.text_input("Input directory", str(settings.input_dir))
    processed_dir = st.sidebar.text_input("Processed directory", str(settings.processed_dir))
    threshold = st.sidebar.slider("Route risk threshold", 0.0, 1.0, settings.route_risk_threshold, 0.05)
    penalty = st.sidebar.number_input("Route risk penalty", min_value=0.0, value=settings.route_risk_penalty)
    data = load_dashboard_data(input_dir=input_dir, processed_dir=processed_dir)
    summary = dashboard_summary(data, resolution=settings.resolution)

    columns = st.columns(5)
    columns[0].metric("Telemetry events", summary["telemetry_count"])
    columns[1].metric("Weather alerts", summary["active_alert_count"])
    columns[2].metric("Risk events", summary["risk_event_count"])
    columns[3].metric("Exposed vessels", summary["exposed_vessel_count"])
    columns[4].metric("Density cells", summary["density_cell_count"])
    if summary["latest_event_time"]:
        st.caption(f"Latest event time: {summary['latest_event_time']} (UTC)")
    else:
        st.warning("No current data is available. Start the Phase 2 replay first.")

    st.subheader("Vessel positions")
    _render_map(st, data["telemetry"])
    st.subheader("Risk events")
    if data["risk_events"]:
        st.dataframe(data["risk_events"], use_container_width=True)
    else:
        st.info("No risk events are available.")

    st.subheader("Route comparison")
    try:
        route = dashboard_route(data, resolution=settings.resolution,
                                coverage_mode=settings.coverage_mode,
                                risk_threshold=threshold, risk_penalty=penalty)
    except ValueError as exc:
        st.warning(str(exc))
        return
    if route is None:
        st.info("No vessel telemetry is available for route scoring.")
        return
    st.write(
        {
            "baseline_path": " → ".join(route["baseline"]["path"]),
            "alternative_path": " → ".join(route["alternative"]["path"]),
            "baseline_hours": route["baseline"]["travel_hours"],
            "alternative_hours": route["alternative"]["travel_hours"],
            "risk_difference": route["risk_delta"],
            "synthetic_demo": route["synthetic_demo"],
        }
    )
    st.write(route["explanation"])


if __name__ == "__main__":
    main()

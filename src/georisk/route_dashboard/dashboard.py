"""Local Streamlit dashboard and dashboard data-loading helpers."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from georisk.route_dashboard.route_optimizer import demonstration_graph, find_alternative_route


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


def load_dashboard_data(root: str | Path = "output") -> dict[str, list[dict[str, Any]]]:
    """Load Phase 2/3 local files without hiding missing-data states."""
    base = Path(root)
    return {
        "telemetry": _read_jsonl(base / "data" / "vessel-telemetry.jsonl"),
        "weather_alerts": _read_jsonl(base / "data" / "weather-alerts.jsonl"),
        "risk_events": _read_jsonl(base / "processed" / "risk-events.jsonl"),
        "density_metrics": _read_jsonl(base / "processed" / "density-metrics.jsonl"),
    }


def dashboard_summary(data: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    """Create metric values and freshness information for UI or API use."""
    risk_events = data.get("risk_events", [])
    latest_values = [
        row.get("event_time")
        for row in (*data.get("telemetry", []), *risk_events)
        if row.get("event_time")
    ]
    latest = max(latest_values) if latest_values else None
    return {
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

    st.set_page_config(page_title="GeoRisk-Spark", layout="wide")
    st.title("GeoRisk-Spark")
    st.caption("Synthetic demonstration data | H3 candidate risk screen")
    data_root = st.sidebar.text_input("Data directory", "output")
    threshold = st.sidebar.slider("Route risk threshold", 0.0, 1.0, 0.5, 0.05)
    data = load_dashboard_data(data_root)
    summary = dashboard_summary(data)

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
    route = find_alternative_route(
        demonstration_graph(), "port_a", "port_b", threshold
    )
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


if __name__ == "__main__":
    main()

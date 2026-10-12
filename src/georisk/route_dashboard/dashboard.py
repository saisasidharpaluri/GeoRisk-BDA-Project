"""Local Streamlit dashboard and dashboard data-loading helpers."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from georisk.config import GeoRiskConfig
from georisk.processing.h3_index import polygon_to_cells
from georisk.route_dashboard.edge_risk import score_graph
from georisk.route_dashboard.route_optimizer import (
    demonstration_graph, demonstration_nodes, find_alternative_route,
)
from georisk.route_dashboard.views import (
    ROUTE_SCENARIOS, active_alerts, compare_adjacent_windows, delay_mae, density_for_window,
    density_windows, exposure_series, filter_risk_events, freshness, lane_status,
    latest_delay_estimates, latest_positions, latest_risk_by_vessel, risk_table_rows,
    route_scenario_alerts, severity_distribution,
)

MAP_LAYERS = (
    ("vessels", "Vessels"), ("hazards", "Hazard zones"), ("risk_cells", "Risk cells"),
    ("density", "Corridor density"), ("route", "Routes"),
)

_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Archivo:wdth,wght@62..125,500..800&family=IBM+Plex+Mono:wght@400;500;600&family=Public+Sans:wght@400;500;600;700&display=swap');
:root { --gr-ink:#14232E; --gr-paper:#F2F5F4; --gr-magenta:#A8166F; --gr-green:#0E7C66;
        --gr-slate:#5B6B78; --gr-sea:#1D5C80; --gr-rule:#C9D6DA; --gr-red:#E03A1E; }
.block-container { padding-top: 1.5rem; max-width: 1480px; }
[data-testid="stMain"] [data-testid="stMetricLabel"] p { font-size: .74rem; letter-spacing: .07em;
  text-transform: uppercase; color: var(--gr-slate); }
[data-testid="stMetricValue"] { font-family: 'IBM Plex Mono', monospace; font-weight: 500; }
.gr-legend { display: flex; flex-wrap: wrap; gap: .3rem 1.1rem; margin: .45rem 0 0;
  font: 500 .74rem/1.4 'IBM Plex Mono', monospace; color: var(--gr-ink); }
.gr-legend span { white-space: nowrap; }
.gr-key { display: inline-block; width: 12px; height: 12px; margin-right: 6px; vertical-align: -1px; }
.gr-key.dot { border-radius: 50%; box-shadow: 0 0 0 1.5px var(--gr-paper) inset; }
.gr-key.zone { background: rgba(168,22,111,.16); border: 2px solid var(--gr-magenta); }
.gr-key.hex { clip-path: polygon(25% 4%,75% 4%,100% 50%,75% 96%,25% 96%,0 50%); }
.gr-key.line { height: 4px; width: 20px; vertical-align: 3px; border-radius: 2px; }
.gr-hint { font: .78rem/1.45 'Public Sans', sans-serif; color: var(--gr-slate); margin: 0 0 .4rem; }
.gr-cartouche { padding: .2rem 0 0; }
.gr-eyebrow { font: 600 .72rem/1.3 'IBM Plex Mono', monospace; letter-spacing: .16em;
  text-transform: uppercase; color: var(--gr-slate); margin: 0; }
.gr-title { font-family: 'Archivo', sans-serif; font-stretch: 72%; font-weight: 800;
  font-size: clamp(2.2rem, 4vw, 3.1rem); line-height: .95; letter-spacing: .005em;
  text-transform: uppercase; color: var(--gr-ink); margin: .25rem 0 .55rem; padding: 0; }
.gr-badges { display: flex; flex-wrap: wrap; align-items: center; gap: .35rem .7rem;
  font: 500 .78rem/1.4 'IBM Plex Mono', monospace; color: var(--gr-ink); }
.gr-badges .sep { color: var(--gr-rule); }
.gr-stamp { border: 1.5px solid var(--gr-magenta); color: var(--gr-magenta); padding: 0 .45rem;
  font-weight: 600; letter-spacing: .1em; }
.gr-neatline { height: 7px; margin: 1rem 0 .3rem; border: 1px solid var(--gr-ink);
  background: repeating-linear-gradient(90deg, var(--gr-ink) 0 32px, transparent 32px 64px); }
.gr-scale { display: flex; justify-content: space-between; margin-bottom: 1.1rem;
  font: 500 .68rem/1 'IBM Plex Mono', monospace; letter-spacing: .08em; color: var(--gr-slate); }
.gr-kv { display: grid; grid-template-columns: auto 1fr; gap: .2rem .9rem; margin: .3rem 0 0;
  font: .76rem/1.4 'IBM Plex Mono', monospace; color: var(--gr-ink); }
.gr-kv dt { color: var(--gr-slate); }
.gr-kv dd { margin: 0; overflow-wrap: anywhere; }
.st-key-freshness h3 { font-size: .8rem; letter-spacing: .12em; text-transform: uppercase;
  font-family: 'IBM Plex Mono', monospace; font-weight: 600; padding-top: 0; }
.stTabs [data-baseweb="tab"] p { font-family: 'Archivo', sans-serif; font-stretch: 87.5%;
  font-weight: 650; font-size: .98rem; letter-spacing: .02em; }
@media (prefers-reduced-motion: reduce) { * { animation: none !important; transition: none !important; } }
</style>
"""

_LEGEND = {
    "vessels": '<span><i class="gr-key dot" style="background:#14232E"></i>Vessel</span>'
               '<span><i class="gr-key dot" style="background:#E03A1E"></i>Vessel in hazard</span>',
    "hazards": '<span><i class="gr-key zone"></i>Active hazard zone</span>',
    "risk_cells": '<span><i class="gr-key hex" style="background:#A8166F"></i>Alerted H3 cell</span>',
    "density": '<span><i class="gr-key hex" style="background:linear-gradient(90deg,#9CC9DB,#1D5C80)">'
               '</i>Vessels per cell (height)</span>',
    "route": '<span><i class="gr-key line" style="background:#5B6B78"></i>Baseline (fastest)</span>'
             '<span><i class="gr-key line" style="background:#0E7C66"></i>Alternative (risk-aware)</span>',
}


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
    risk_threshold: float, risk_penalty: float, alerts: list[dict[str, Any]] | None = None,
) -> dict[str, Any] | None:
    """Score the demo graph at the latest telemetry time for the UI and report.

    ``alerts`` replaces the loaded weather alerts, for what-if scenarios.
    """
    if not data["telemetry"]:
        return None
    at_time = max(event["event_time"] for event in data["telemetry"])
    graph = score_graph(
        demonstration_graph(), demonstration_nodes(),
        data["weather_alerts"] if alerts is None else alerts,
        data["density_metrics"], at_time=at_time, resolution=resolution,
        coverage_mode=coverage_mode,
    )
    route = find_alternative_route(graph, "port_a", "port_b", risk_threshold,
                                   risk_penalty=risk_penalty)
    return {**route, "at_time": at_time, "edge_scores": graph}


def load_run_metadata(processed_dir: str | Path) -> dict[str, Any] | None:
    """Load ``run-metadata.json``; a missing or unreadable file returns ``None``."""
    path = Path(processed_dir) / "run-metadata.json"
    try:
        metadata = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return metadata if isinstance(metadata, dict) else None


def _dms(value: float, positive: str, negative: str) -> str:
    degrees, minutes = divmod(round(abs(value) * 60), 60)
    return f"{degrees}°{minutes:02d}′{positive if value >= 0 else negative}"


def _render_header(st: Any, metadata: dict[str, Any] | None, settings: GeoRiskConfig) -> None:
    from html import escape

    from georisk.geo_demo import DEMO_BBOX

    metadata = metadata or {}
    resolution = metadata.get("h3_resolution", settings.resolution)
    coverage = metadata.get("coverage_mode", settings.coverage_mode)
    window = metadata.get("window_minutes", settings.density_window_minutes)
    west = f"{_dms(DEMO_BBOX['lat_min'], 'N', 'S')} {_dms(DEMO_BBOX['lon_min'], 'E', 'W')}"
    east = f"{_dms(DEMO_BBOX['lat_max'], 'N', 'S')} {_dms(DEMO_BBOX['lon_max'], 'E', 'W')}"
    badges = [f"H3 res {resolution}", f"coverage: {coverage}", f"window: {window} min"]
    st.html(
        '<div class="gr-cartouche">'
        '<p class="gr-eyebrow">Bay of Bengal · offshore Chennai · maritime risk screen</p>'
        '<h1 class="gr-title">GeoRisk-Spark</h1>'
        '<div class="gr-badges"><span class="gr-stamp">SIMULATED DATA</span>'
        + "".join(f'<span class="sep">·</span><span>{escape(str(badge))}</span>' for badge in badges)
        + "</div></div>"
        f'<div class="gr-neatline" aria-hidden="true"></div>'
        f'<div class="gr-scale"><span>{west}</span><span>{east}</span></div>'
    )


def _render_freshness(st: Any, metadata: dict[str, Any] | None, settings: GeoRiskConfig) -> None:
    from datetime import datetime, timezone
    from html import escape

    status = freshness(metadata, datetime.now(timezone.utc), settings.stale_after_minutes)
    with st.container(border=True, key="freshness"):
        st.subheader("Data freshness")
        age = status["age_minutes"]
        if status["state"] == "fresh":
            st.success(f"Fresh · processed {age:g} min ago", icon=":material/check_circle:")
        elif status["state"] == "stale":
            st.warning(f"Stale · last processed {age:g} min ago (limit "
                       f"{settings.stale_after_minutes} min). Rerun `georisk run` or the replay.",
                       icon=":material/schedule:")
        else:
            st.error("Unavailable · no run-metadata.json in the processed directory. "
                     "Run `georisk run` to create it.", icon=":material/error:")
        rows = (("Processed at", status["processed_at"]),
                ("Latest event", status["latest_event_time"]),
                ("Watermark", status["watermark"]),
                ("Age", f"{age:g} min" if age is not None else None))
        st.html('<dl class="gr-kv">' + "".join(
            f"<dt>{label}</dt><dd>{escape(str(value)) if value else '—'}</dd>"
            for label, value in rows) + "</dl>")


def _map_selection(st: Any) -> tuple[str, str] | None:
    """Return the (layer id, object id) last clicked on the map, if any."""
    state = st.session_state.get("live_map")
    try:
        objects = state["selection"]["objects"]
    except (KeyError, TypeError):
        return None
    for layer, field in (("vessels", "vessel_id"), ("hazards", "alert_id")):
        if objects.get(layer):
            return layer, objects[layer][0][field]
    return None


def _render_inspector(
    st: Any, positions: list[dict[str, Any]], latest_risk: dict[str, dict[str, Any]],
    alerts: list[dict[str, Any]], at_time: str | None, settings: GeoRiskConfig,
) -> None:
    st.subheader("Inspect")
    st.html('<p class="gr-hint">Click a vessel or hazard zone on the map, '
            'or choose one below.</p>')
    selected = _map_selection(st)
    if selected and selected != st.session_state.get("_map_selection_seen"):
        st.session_state["_map_selection_seen"] = selected
        st.session_state["inspect_vessel" if selected[0] == "vessels" else "inspect_alert"] = selected[1]

    vessel_ids = [row["vessel_id"] for row in positions]
    if vessel_ids:
        if st.session_state.get("inspect_vessel") not in vessel_ids:
            st.session_state.pop("inspect_vessel", None)
        exposed = {vessel for vessel, row in latest_risk.items() if row.get("risk_alert")}
        vessel = st.selectbox(
            "Inspect vessel", vessel_ids, key="inspect_vessel",
            format_func=lambda v: f"{v}  · in hazard" if v in exposed else v,
        )
        position = next(row for row in positions if row["vessel_id"] == vessel)
        risk = latest_risk.get(vessel, {})
        st.json({
            "vessel_id": vessel,
            "latest_position": {
                "event_time": position["event_time"], "latitude": position["latitude"],
                "longitude": position["longitude"], "speed_knots": position.get("speed_knots"),
                "heading_deg": position.get("heading_deg"), "h3_index": risk.get("h3_index"),
            },
            "risk_alert": bool(risk.get("risk_alert")),
            "hazards": [
                {key: hazard.get(key) for key in ("alert_id", "hazard_type", "severity")}
                for hazard in risk.get("matching_hazards") or []
            ],
        }, expanded=2)
    else:
        st.info("No vessels to inspect yet.")

    alert_ids = [alert["alert_id"] for alert in alerts]
    if alert_ids:
        if st.session_state.get("inspect_alert") not in alert_ids:
            st.session_state.pop("inspect_alert", None)
        alert_id = st.selectbox("Inspect alert", alert_ids, key="inspect_alert")
        alert = next(row for row in alerts if row["alert_id"] == alert_id)
        st.json({
            "alert_id": alert_id, "hazard_type": alert["hazard_type"],
            "severity": alert["severity"],
            "validity": {"valid_from": alert["valid_from"], "valid_until": alert["valid_until"],
                         "active_at": at_time},
            "cell_count": len(polygon_to_cells(alert["geometry"], settings.resolution,
                                               settings.coverage_mode)),
            "coverage": f"H3 res {settings.resolution}, {settings.coverage_mode}",
        }, expanded=2)
    else:
        st.info("No hazard alerts are active at the latest event time.")


def _render_map(
    st: Any, layers: dict[str, bool], *, positions: list[dict[str, Any]],
    exposed: set[str], alerts: list[dict[str, Any]], risk_events: list[dict[str, Any]],
    density_rows: list[dict[str, Any]], route: dict[str, Any] | None,
) -> None:
    from georisk.route_dashboard import map_layers

    st.subheader("Live map")
    if not positions and not alerts:
        st.info("No vessel telemetry or hazard alerts are available for the map.")
        return
    built = map_layers.node_layers(demonstration_nodes())
    if layers["density"]:
        built.append(map_layers.density_layer(density_rows))
    if layers["hazards"]:
        built.append(map_layers.hazards_layer(alerts))
    if layers["risk_cells"]:
        built.append(map_layers.risk_cells_layer(risk_events))
    if layers["route"]:
        built.append(map_layers.route_layer(route, demonstration_nodes()))
    if layers["vessels"]:
        built.append(map_layers.vessels_layer(positions, exposed))
    st.pydeck_chart(map_layers.build_deck(built), height=560, on_select="rerun",
                    selection_mode="single-object", key="live_map")
    legend = "".join(_LEGEND[name] for name, enabled in layers.items() if enabled)
    st.html(f'<div class="gr-legend">{legend}</div>' if legend else
            '<p class="gr-hint">All map layers are hidden. Turn them on in the sidebar.</p>')


def _short_time(value: str) -> str:
    return value[11:16] if len(value) >= 16 else value


def _render_kpis(
    st: Any, *, alerts: list[dict[str, Any]], exposed: set[str], positions: list[dict[str, Any]],
    data: dict[str, list[dict[str, Any]]], latest: str | None,
) -> None:
    risk_events = data["risk_events"]
    ever_exposed = {row["vessel_id"] for row in risk_events if row.get("risk_alert")}
    alerted = sum(bool(row.get("risk_alert")) for row in risk_events)
    series = [row["exposed_vessels"] for row in exposure_series(risk_events)]
    columns = st.columns(4)
    columns[0].metric("Active alerts", len(alerts), border=True, height="stretch",
                      help="Hazard alerts valid at the latest event time (latest revision).")
    columns[1].metric(
        "Vessels exposed now", f"{len(exposed)} / {len(positions)}", border=True, height="stretch",
        chart_data=series or None, chart_type="area",
        help=f"Latest position inside an active hazard. {len(ever_exposed)} vessels were "
             "exposed at some point; the sparkline tracks exposure over time.",
    )
    columns[2].metric("Alerted positions", f"{alerted} / {len(risk_events)}", border=True, height="stretch",
                      help="Telemetry positions matched to an active hazard cell.")
    columns[3].metric("Latest event (UTC)", _short_time(latest) if latest else "—", border=True, height="stretch",
                      help=f"{latest} UTC" if latest else "No events loaded yet.")


def _render_risk_panel(st: Any, risk_events: list[dict[str, Any]], settings: GeoRiskConfig) -> None:
    st.subheader("Risk summary")
    st.caption(f"Positions matched to active hazards on the H3 grid (res {settings.resolution}, "
               f"{settings.coverage_mode} coverage). Severity runs from 1 (minor) to 5 (severe).")
    if not risk_events:
        st.info("No risk events yet. Run `georisk run` to process telemetry against hazard alerts.")
        return
    distribution = severity_distribution(risk_events)
    chart_column, table_column = st.columns([1, 2.4], gap="large")
    with chart_column:
        st.bar_chart(
            {"Severity": [str(level) for level in range(1, 6)],
             "Alerted positions": [distribution.get(level, 0) for level in range(1, 6)]},
            x="Severity", y="Alerted positions", color="#A8166F", height=280, sort=False,
        )
    with table_column:
        hazard_types = sorted({hazard["hazard_type"] for event in risk_events
                               for hazard in event.get("matching_hazards") or []})
        times = sorted({event["event_time"] for event in risk_events})
        type_column, severity_column, time_column = st.columns([1.3, 1, 1.5])
        chosen_types = type_column.multiselect(
            "Hazard type", hazard_types, placeholder="All hazard types", key="risk_types",
            format_func=lambda kind: kind.replace("_", " "),
        )
        min_severity = severity_column.slider("Minimum severity", 1, 5, 1, key="risk_severity")
        start, end = (times[0], times[-1]) if times else (None, None)
        if len(times) > 1:
            start, end = time_column.select_slider(
                "Event time (UTC)", times, value=(times[0], times[-1]), key="risk_time",
                format_func=_short_time,
            )
        filtered = filter_risk_events(risk_events, hazard_types=chosen_types or None,
                                      min_severity=min_severity, start=start, end=end)
        alerted_total = sum(bool(event.get("risk_alert")) for event in risk_events)
        if filtered:
            st.dataframe(
                risk_table_rows(filtered), width="stretch", hide_index=True, height=320,
                column_order=("event_time", "vessel_id", "hazards", "max_severity", "h3_index",
                              "coverage_mode", "h3_resolution", "latitude", "longitude"),
                column_config={
                    "event_time": st.column_config.TextColumn("Event time (UTC)"),
                    "vessel_id": st.column_config.TextColumn("Vessel"),
                    "hazards": st.column_config.TextColumn("Matched hazards"),
                    "max_severity": st.column_config.ProgressColumn(
                        "Severity", min_value=0, max_value=5, format="%d"),
                    "h3_index": st.column_config.TextColumn("H3 cell"),
                    "coverage_mode": st.column_config.TextColumn("Coverage"),
                    "h3_resolution": st.column_config.NumberColumn("Res", format="%d"),
                    "latitude": st.column_config.NumberColumn("Lat (°)", format="%.4f"),
                    "longitude": st.column_config.NumberColumn("Lon (°)", format="%.4f"),
                },
            )
            st.caption(f"Showing {len(filtered)} of {alerted_total} alerted positions.")
        else:
            st.info("No alerted positions match these filters. Lower the minimum severity, "
                    "widen the time range, or clear the hazard types.")


def _window_label(rows: list[dict[str, Any]], window_start: str) -> str:
    end = next((row["window_end"] for row in rows if row["window_start"] == window_start), "")
    return f"{_short_time(window_start)}–{_short_time(end)}" if end else _short_time(window_start)


def _render_density_panel(
    st: Any, density_metrics: list[dict[str, Any]], settings: GeoRiskConfig,
) -> str | None:
    """Render the density tables and return the window the map should show."""
    st.subheader("Corridor density")
    st.caption(f"Unique vessels per H3 cell (res {settings.resolution}) per "
               f"{settings.density_window_minutes}-minute window. The map's density layer "
               "follows the window selected here.")
    windows = density_windows(density_metrics)
    if not windows:
        st.info("No density metrics yet. Run `georisk run` to aggregate vessel positions "
                "into H3 windows.")
        return None
    window = windows[-1]
    if len(windows) > 1:
        window = st.select_slider("Window (UTC)", windows, value=windows[-1], key="density_window",
                                  format_func=lambda start: _window_label(density_metrics, start))
    rows = density_for_window(density_metrics, window)
    peak = max(row["unique_vessel_count"] for row in rows)
    current_column, compare_column = st.columns([1, 1.4], gap="large")
    with current_column:
        st.markdown(f"**Busiest cells · {_window_label(density_metrics, window)} UTC**")
        st.dataframe(
            rows, width="stretch", hide_index=True, height=300,
            column_order=("h3_index", "unique_vessel_count"),
            column_config={
                "h3_index": st.column_config.TextColumn("H3 cell"),
                "unique_vessel_count": st.column_config.ProgressColumn(
                    "Unique vessels", min_value=0, max_value=max(peak, 1), format="%d"),
            },
        )
    with compare_column:
        st.markdown("**Compared with neighbouring windows**")
        comparison = [
            {**row, "change": None if row["previous"] is None else row["current"] - row["previous"]}
            for row in compare_adjacent_windows(density_metrics, window)
        ]
        st.dataframe(
            comparison, width="stretch", hide_index=True, height=300,
            column_order=("h3_index", "previous", "current", "next", "change"),
            column_config={
                "h3_index": st.column_config.TextColumn("H3 cell"),
                "previous": st.column_config.NumberColumn("Previous", format="%d"),
                "current": st.column_config.NumberColumn("Selected", format="%d"),
                "next": st.column_config.NumberColumn("Next", format="%d"),
                "change": st.column_config.NumberColumn("Change vs previous", format="%+d"),
            },
        )
        st.caption("Blank neighbour columns mean the selected window is the first or last one.")
    return window


def load_delay_estimates(processed_dir: str | Path) -> list[dict[str, Any]]:
    """Load persisted delay estimates; a missing file returns an empty list."""
    return _read_jsonl(Path(processed_dir) / "delay-estimates.jsonl")


def _render_delay_panel(st: Any, estimates: list[dict[str, Any]]) -> None:
    st.subheader("Delay estimates")
    st.caption("Synthetic demonstration, not a real-world forecast.")
    if not estimates:
        st.info("No delay estimates yet. Run `georisk run` to train the delay model and write "
                "processed/delay-estimates.jsonl.")
        return
    mae = delay_mae(estimates)
    model_name = estimates[-1].get("model_name", "model")
    columns = st.columns(3)
    if mae["model_mae_hours"] is None:
        columns[0].metric("Model MAE (h)", "—", border=True)
        columns[1].metric("Baseline MAE (h)", "—", border=True)
    else:
        columns[0].metric(
            f"Model MAE (h) · {model_name}", f"{mae['model_mae_hours']:.2f}", border=True,
            delta=f"{mae['model_mae_hours'] - mae['baseline_mae_hours']:+.2f} h vs baseline",
            delta_color="inverse", help="Mean absolute error on held-out rows (lower is better).",
        )
        columns[1].metric("Baseline MAE (h)", f"{mae['baseline_mae_hours']:.2f}", border=True,
                          help="Predicting the training-set mean delay for every row.")
    columns[2].metric("Held-out rows", mae["evaluation_count"], border=True)
    show_truth = st.toggle("Demo mode: show synthetic ground truth", key="delay_truth",
                           help="Adds the synthetic label the model was trained to predict.")
    columns_shown = ["vessel_id", "event_time", "predicted_delay_hours", "baseline_delay_hours",
                     "error_hours"] + (["synthetic_label_hours"] if show_truth else [])
    st.dataframe(
        latest_delay_estimates(estimates), width="stretch", hide_index=True,
        column_order=columns_shown,
        column_config={
            "vessel_id": st.column_config.TextColumn("Vessel"),
            "event_time": st.column_config.TextColumn("Latest estimate (UTC)"),
            "predicted_delay_hours": st.column_config.NumberColumn("Predicted delay (h)", format="%.2f"),
            "baseline_delay_hours": st.column_config.NumberColumn("Baseline (h)", format="%.2f"),
            "error_hours": st.column_config.NumberColumn("Error (h)", format="%+.2f",
                                                         help="Predicted minus synthetic label."),
            "synthetic_label_hours": st.column_config.NumberColumn("Synthetic label (h)", format="%.2f"),
        },
    )


def _path_label(path: list[str]) -> str:
    from georisk.route_dashboard.map_layers import NODE_LABELS

    return " → ".join(NODE_LABELS.get(node, node) for node in path)


def _render_route_panel(
    st: Any, route: dict[str, Any] | None, route_error: str | None, scenario: str,
) -> None:
    st.subheader("Route comparison")
    st.caption(f"Scenario: {scenario}. Lanes riskier than the threshold are closed to the "
               "alternative; total cost = hours + penalty × risk. Synthetic demonstration.")
    if route_error:
        st.warning(f"{route_error}. Raise the risk threshold or choose another scenario.")
        return
    if route is None:
        st.info("No vessel telemetry is available for route scoring. Run `georisk run` first.")
        return
    if route["route_changed"]:
        st.warning(route["explanation"], icon=":material/alt_route:")
    else:
        st.success(route["explanation"], icon=":material/check_circle:")
    penalty = route["risk_penalty"]
    legs = {name: route[name] for name in ("baseline", "alternative")}
    st.table({
        "": ["Path", "Travel time (h)", "Total risk", f"Total cost (h + {penalty:g} × risk)"],
        "Baseline · fastest": [
            _path_label(legs["baseline"]["path"]), f"{legs['baseline']['travel_hours']:.2f}",
            f"{legs['baseline']['risk_score']:.2f}",
            f"{legs['baseline']['travel_hours'] + penalty * legs['baseline']['risk_score']:.2f}",
        ],
        "Alternative · risk-aware": [
            _path_label(legs["alternative"]["path"]), f"{legs['alternative']['travel_hours']:.2f}",
            f"{legs['alternative']['risk_score']:.2f}",
            f"{legs['alternative']['travel_hours'] + penalty * legs['alternative']['risk_score']:.2f}",
        ],
    }, hide_index=True)
    st.markdown(f"**Lane status at {route['at_time']} UTC**")
    st.dataframe(
        lane_status(route["edge_scores"], route), width="stretch", hide_index=True,
        column_config={
            "lane": st.column_config.TextColumn("Lane"),
            "distance_nm": st.column_config.NumberColumn("Distance (nm)", format="%.1f"),
            "travel_hours": st.column_config.NumberColumn("Hours", format="%.2f"),
            "risk": st.column_config.ProgressColumn("Risk", min_value=0.0, max_value=1.0,
                                                    format="%.2f"),
            "risk_detail": st.column_config.TextColumn(
                "Risk drivers", help="Max hazard severity (alert IDs) and max vessel density."),
            "status": st.column_config.TextColumn("Status"),
            "used_by": st.column_config.TextColumn("Used by"),
        },
    )


def _render_body(st: Any, settings: GeoRiskConfig, controls: dict[str, Any]) -> None:
    """Render everything that depends on loaded data; reruns alone when auto-refreshing."""
    processed_dir = controls["processed_dir"]
    data = load_dashboard_data(input_dir=controls["input_dir"], processed_dir=processed_dir)
    metadata = load_run_metadata(processed_dir)
    summary = dashboard_summary(data, resolution=settings.resolution)

    at_time = summary["latest_event_time"]
    alerts = active_alerts(data["weather_alerts"], at_time) if at_time else []
    positions = latest_positions(data["telemetry"])
    latest_risk = latest_risk_by_vessel(data["risk_events"])
    exposed = {vessel for vessel, row in latest_risk.items() if row.get("risk_alert")}

    header_column, freshness_column = st.columns([2.6, 1], gap="medium",
                                                 vertical_alignment="bottom")
    with header_column:
        _render_header(st, metadata, settings)
    with freshness_column:
        _render_freshness(st, metadata, settings)
    _render_kpis(st, alerts=alerts, exposed=exposed, positions=positions, data=data, latest=at_time)
    if not summary["has_data"]:
        st.warning("No vessel telemetry or risk events were found in the selected directories. "
                   "Run `georisk run` (or start a replay), then check the data sources in the "
                   "sidebar.", icon=":material/database:")

    scenario = controls["scenario"]
    route_time = max((row["event_time"] for row in data["telemetry"]), default=at_time)
    scenario_alerts = (route_scenario_alerts(scenario, data["weather_alerts"], route_time)
                       if route_time else [])
    map_alerts = active_alerts(scenario_alerts, route_time) if route_time else []
    try:
        route = dashboard_route(data, resolution=settings.resolution,
                                coverage_mode=settings.coverage_mode,
                                risk_threshold=controls["threshold"],
                                risk_penalty=controls["penalty"], alerts=scenario_alerts)
        route_error = None
    except ValueError as exc:
        route, route_error = None, str(exc)

    map_column, inspect_column = st.columns([2.6, 1], gap="medium")
    risk_tab, route_tab, density_tab, delay_tab = st.tabs(
        ["Risk", "Route", "Corridor density", "Delay"])
    with risk_tab:
        _render_risk_panel(st, data["risk_events"], settings)
    with route_tab:
        _render_route_panel(st, route, route_error, scenario)
    with density_tab:
        window = _render_density_panel(st, data["density_metrics"], settings)
    with delay_tab:
        _render_delay_panel(st, load_delay_estimates(processed_dir))
    density_rows = density_for_window(data["density_metrics"], window) if window else []
    with map_column:
        _render_map(st, controls["layers"], positions=positions, exposed=exposed,
                    alerts=map_alerts, risk_events=data["risk_events"],
                    density_rows=density_rows, route=route)
        if scenario != ROUTE_SCENARIOS[0]:
            st.caption(f"Route what-if “{scenario}” is active: hazard zones and routes on the "
                       "map reflect the scenario, not the loaded alerts.")
    with inspect_column:
        with st.container(height=640, border=True):
            _render_inspector(st, positions, latest_risk, map_alerts, route_time, settings)


def _sidebar_controls(st: Any, settings: GeoRiskConfig) -> dict[str, Any]:
    sidebar = st.sidebar
    sidebar.subheader("Route what-if")
    scenario = sidebar.selectbox(
        "Hazard scenario", ROUTE_SCENARIOS, key="route_scenario",
        help="As generated uses the loaded alerts. Close inner lane adds a synthetic "
             "severity-5 closure at the inner-lane chokepoint.",
    )
    threshold = sidebar.slider("Route risk threshold", 0.0, 1.0, settings.route_risk_threshold,
                               0.05, key="route_threshold",
                               help="Lanes with risk above this are closed to the alternative.")
    penalty = sidebar.number_input("Route risk penalty (h per unit risk)", min_value=0.0,
                                   value=settings.route_risk_penalty, key="route_penalty",
                                   help="Hours added per unit of lane risk when ranking routes.")
    sidebar.subheader("Map layers")
    layers = {name: sidebar.checkbox(label, value=True, key=f"layer_{name}")
              for name, label in MAP_LAYERS}
    sidebar.subheader("Live")
    auto_refresh = sidebar.toggle("Auto-refresh every 10 s", key="auto_refresh",
                                  help="Reload the files every 10 seconds while a replay runs.")
    with sidebar.expander("Data sources"):
        input_dir = st.text_input("Input directory", str(settings.input_dir), key="input_dir")
        processed_dir = st.text_input("Processed directory", str(settings.processed_dir),
                                      key="processed_dir")
    return {"scenario": scenario, "threshold": threshold, "penalty": penalty,
            "layers": layers, "auto_refresh": auto_refresh,
            "input_dir": input_dir, "processed_dir": processed_dir}


def main() -> None:
    """Render the local dashboard; cloud query adapters can replace the loader."""
    try:
        import streamlit as st
    except ImportError as exc:
        raise RuntimeError(
            "Streamlit is required to run the dashboard. Install the dashboard extra."
        ) from exc

    settings = GeoRiskConfig.from_env()
    st.set_page_config(page_title="GeoRisk-Spark", page_icon=":material/sailing:", layout="wide")
    st.html(_CSS)
    controls = _sidebar_controls(st, settings)
    if controls["auto_refresh"]:
        st.fragment(run_every=10)(_render_body)(st, settings, controls)
    else:
        _render_body(st, settings, controls)


if __name__ == "__main__":
    main()

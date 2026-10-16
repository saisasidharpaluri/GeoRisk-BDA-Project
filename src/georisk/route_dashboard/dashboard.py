"""Local Streamlit dashboard and dashboard data-loading helpers.

The data functions (``load_dashboard_data``, ``dashboard_summary``,
``dashboard_route``, ``load_run_metadata``, ``effective_settings``, …) are
also used by the pipeline report and have no Streamlit dependency. The
``_render_*`` functions draw one part of the page each; ``main`` wires them
together in presentation order: header and pipeline, current situation (KPIs,
map, inspector), then one tab per analytic product.
"""

from __future__ import annotations

import dataclasses
import json
from datetime import datetime, timezone
from html import escape
from pathlib import Path
from typing import Any

import h3

from georisk.config import GeoRiskConfig
from georisk.processing.h3_index import polygon_to_cells
from georisk.route_dashboard.edge_risk import score_graph
from georisk.route_dashboard.route_optimizer import (
    demonstration_graph,
    demonstration_nodes,
    find_alternative_route,
)
from georisk.route_dashboard.styles import CSS, LEGEND
from georisk.route_dashboard.views import (
    ROUTE_SCENARIOS,
    _utc,
    acceptance_summary,
    active_alerts,
    alert_status,
    chart_time_labels,
    compare_adjacent_windows,
    count_by,
    daily_history,
    delay_mae,
    density_for_window,
    density_trend,
    density_windows,
    exposure_series,
    filter_risk_events,
    format_datetime,
    format_time,
    freshness,
    humanize_minutes,
    lane_status,
    latest_delay_estimates,
    latest_positions,
    latest_risk_by_vessel,
    latest_timestamp,
    risk_table_rows,
    route_scenario_alerts,
    severity_distribution,
    time_span,
    vessel_exposure,
    vessels_inside,
)

MAP_LAYERS = (
    ("vessels", "Vessels"),
    ("hazards", "Hazard zones"),
    ("risk_cells", "Alerted H3 cells"),
    ("density", "Corridor density"),
    ("route", "Routes"),
)

TABS = ("Hazard exposure", "Corridor density", "Delay model", "Route decision", "Run quality")


# --- Data loading (no Streamlit dependency) --------------------------------


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
    root: str | Path | None = None,
    *,
    input_dir: str | Path | None = None,
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


def dashboard_summary(
    data: dict[str, list[dict[str, Any]]], *, resolution: int = 7
) -> dict[str, Any]:
    """Create metric values and freshness information for UI or API use.

    ``active_alert_count`` counts alerts valid at the latest event time, using
    the revision current at that time.
    """
    risk_events = data.get("risk_events", [])
    latest_values = [
        row["event_time"]
        for row in (*data.get("telemetry", []), *risk_events)
        if row.get("event_time")
    ]
    latest = latest_timestamp(latest_values)
    alerts = data.get("weather_alerts", [])
    return {
        "simulated": True,
        "h3_resolution": resolution,
        "units": {"delay": "hours", "speed": "knots", "distance": "nm"},
        "telemetry_count": len(data.get("telemetry", [])),
        "alert_record_count": len(alerts),
        "active_alert_count": len(active_alerts(alerts, latest)) if latest else 0,
        "risk_event_count": len(risk_events),
        "exposed_vessel_count": len(
            {row["vessel_id"] for row in risk_events if row.get("risk_alert")}
        ),
        "density_cell_count": len({row["h3_index"] for row in data.get("density_metrics", [])}),
        "latest_event_time": latest,
        "has_data": bool(latest_values),
    }


def dashboard_route(
    data: dict[str, list[dict[str, Any]]],
    *,
    resolution: int,
    coverage_mode: str,
    risk_threshold: float,
    risk_penalty: float,
    alerts: list[dict[str, Any]] | None = None,
) -> dict[str, Any] | None:
    """Score the demo graph at the latest telemetry time for the UI and report.

    ``alerts`` replaces the loaded weather alerts, for what-if scenarios.
    """
    at_time = latest_timestamp(event["event_time"] for event in data["telemetry"])
    if at_time is None:
        return None
    graph = score_graph(
        demonstration_graph(),
        demonstration_nodes(),
        data["weather_alerts"] if alerts is None else alerts,
        data["density_metrics"],
        at_time=at_time,
        resolution=resolution,
        coverage_mode=coverage_mode,
    )
    route = find_alternative_route(
        graph, "port_a", "port_b", risk_threshold, risk_penalty=risk_penalty
    )
    return {**route, "at_time": at_time, "edge_scores": graph}


def load_run_metadata(processed_dir: str | Path) -> dict[str, Any] | None:
    """Load ``run-metadata.json``; a missing or unreadable file returns ``None``."""
    path = Path(processed_dir) / "run-metadata.json"
    try:
        metadata = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return metadata if isinstance(metadata, dict) else None


def load_run_report(report_path: str | Path) -> dict[str, Any] | None:
    """Load the ``georisk run`` report; a missing or unreadable file returns ``None``."""
    try:
        report = json.loads(Path(report_path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return report if isinstance(report, dict) else None


def load_quarantine(processed_dir: str | Path) -> list[dict[str, Any]]:
    """Load quarantined records; a missing or unreadable file returns an empty list."""
    try:
        return _read_jsonl(Path(processed_dir) / "quarantine.jsonl")
    except (OSError, ValueError):
        return []


def load_delay_estimates(processed_dir: str | Path) -> list[dict[str, Any]]:
    """Load persisted delay estimates; a missing file returns an empty list."""
    return _read_jsonl(Path(processed_dir) / "delay-estimates.jsonl")


def effective_settings(settings: GeoRiskConfig, metadata: dict[str, Any] | None) -> GeoRiskConfig:
    """Prefer the processed run's H3 resolution, coverage mode, and window.

    Outputs must be interpreted with the settings that produced them; using the
    current environment instead would, for example, score routes against
    density cells of another resolution. Invalid metadata falls back to
    ``settings``.
    """
    if not metadata:
        return settings
    try:
        return dataclasses.replace(
            settings,
            resolution=int(metadata.get("h3_resolution", settings.resolution)),
            coverage_mode=str(metadata.get("coverage_mode", settings.coverage_mode)),
            density_window_minutes=int(
                metadata.get("window_minutes", settings.density_window_minutes)
            ),
        )
    except (TypeError, ValueError):
        return settings


def route_decision(route: dict[str, Any] | None) -> dict[str, str]:
    """Summarise a route comparison as a short verdict and an explanatory line."""
    if route is None:
        return {"verdict": "—", "detail": "No telemetry to score routes yet."}
    alternative = route.get("alternative")
    if alternative is None:
        return {
            "verdict": "No safe route",
            "detail": "Every path uses a lane above the risk threshold.",
        }
    if not route["route_changed"]:
        return {"verdict": "Keep baseline", "detail": "The fastest route is within the threshold."}
    return {
        "verdict": "Detour",
        "detail": f"{route['travel_hour_delta']:+.1f} h sailing, "
        f"{route['risk_delta']:+.2f} total risk",
    }


# --- Small rendering helpers -----------------------------------------------


def _kv(rows: list[tuple[str, Any]]) -> str:
    """Render label/value pairs as an HTML definition list; empty values show a dash."""
    return (
        '<dl class="gr-kv">'
        + "".join(
            f"<dt>{escape(label)}</dt>"
            f"<dd>{escape(str(value)) if value not in (None, '') else '—'}</dd>"
            for label, value in rows
        )
        + "</dl>"
    )


def _severity_bar(severity: int) -> str:
    cells = "".join(f'<i class="{"on" if level <= severity else ""}"></i>' for level in range(1, 6))
    return f'<span class="gr-sev" aria-label="severity {severity} of 5">{cells}</span>'


def _hazard_names(alerts: list[dict[str, Any]]) -> str:
    return (
        ", ".join(
            f"{alert['alert_id']} {alert['hazard_type'].replace('_', ' ')} "
            f"(sev {alert['severity']})"
            for alert in alerts
        )
        or "None active"
    )


def _friendly_nodes(text: str) -> str:
    """Replace graph node IDs (``port_a``) with their map labels (``Port A``)."""
    from georisk.route_dashboard.map_layers import NODE_LABELS

    for node, label in NODE_LABELS.items():
        text = text.replace(node, label)
    return text.replace(" → ", "→").replace("→", " → ")


def _path_label(path: list[str]) -> str:
    from georisk.route_dashboard.map_layers import NODE_LABELS

    return " → ".join(NODE_LABELS.get(node, node) for node in path)


# --- Header, freshness, and pipeline ----------------------------------------


def _render_header(
    st: Any,
    settings: GeoRiskConfig,
    data: dict[str, list[dict[str, Any]]],
) -> None:
    span = time_span(data["telemetry"])
    scenario = (
        f"Scenario {_utc(span[0]).day} {_utc(span[0]):%b %Y}, "
        f"{format_time(span[0])}–{format_time(span[1])} UTC"
        if span
        else "No scenario loaded"
    )
    area = h3.average_hexagon_area(settings.resolution, unit="km^2")
    badges = [
        ("stamp", "SIMULATED DATA"),
        ("", scenario),
        ("", f"H3 resolution {settings.resolution} · cells ≈ {area:.1f} km²"),
        ("", f"{settings.coverage_mode} coverage"),
        ("", f"{settings.density_window_minutes}-minute density windows"),
    ]
    st.html(
        '<p class="gr-eyebrow">Big Data Analytics · Bay of Bengal, offshore Chennai</p>'
        '<h1 class="gr-title">GeoRisk-Spark</h1>'
        '<p class="gr-lede">Synthetic vessel positions and marine hazard alerts are indexed on '
        "an H3 hexagon grid, matched in event time to flag exposed vessels, aggregated into "
        "corridor congestion, and turned into delay estimates and a risk-aware route choice.</p>"
        '<div class="gr-badges">'
        + "".join(f'<span class="gr-badge {kind}">{escape(text)}</span>' for kind, text in badges)
        + "</div>"
    )


def _render_freshness(st: Any, metadata: dict[str, Any] | None, settings: GeoRiskConfig) -> None:
    status = freshness(metadata, datetime.now(timezone.utc), settings.stale_after_minutes)
    with st.container(border=True, key="freshness"):
        st.subheader("Data freshness")
        age = humanize_minutes(status["age_minutes"])
        if status["state"] == "fresh":
            st.success(f"Fresh · processed {age} ago", icon=":material/check_circle:")
        elif status["state"] == "stale":
            st.warning(
                f"Stale · last processed {age} ago (limit {settings.stale_after_minutes} min). "
                "Rerun `georisk run` or the replay.",
                icon=":material/schedule:",
            )
        else:
            st.error(
                "Unavailable · no run-metadata.json in the processed directory. "
                "Run `georisk run` to create it.",
                icon=":material/error:",
            )
        lag = status["processing_lag_minutes"]
        note = (
            '<p class="gr-note">Event times come from a seeded historical scenario, so the '
            "processing lag reflects replayed history rather than pipeline delay.</p>"
            if lag is not None and lag > 24 * 60
            else ""
        )
        st.html(
            _kv(
                [
                    ("Processed", format_datetime(status["processed_at"])),
                    ("Latest event", format_datetime(status["latest_event_time"])),
                    ("Watermark", format_datetime(status["watermark"])),
                    ("Processing lag", humanize_minutes(lag) if lag is not None else None),
                ]
            )
            + note
        )


def _render_pipeline(
    st: Any,
    *,
    data: dict[str, list[dict[str, Any]]],
    metadata: dict[str, Any] | None,
    settings: GeoRiskConfig,
    estimates: list[dict[str, Any]],
    route: dict[str, Any] | None,
) -> None:
    """One card per processing stage with the counts this run actually produced."""
    telemetry, risk_events = data["telemetry"], data["risk_events"]
    density = data["density_metrics"]
    counts = (metadata or {}).get("counts") or {}
    vessels = len({row["vessel_id"] for row in telemetry})
    hazard_cells: set[str] = set()
    for alert in data["weather_alerts"]:
        try:
            hazard_cells |= polygon_to_cells(
                alert["geometry"], settings.resolution, settings.coverage_mode
            )
        except (KeyError, ValueError):
            continue
    alerted = sum(bool(event.get("risk_alert")) for event in risk_events)
    exposed = len({event["vessel_id"] for event in risk_events if event.get("risk_alert")})
    peak = max((row["unique_vessel_count"] for row in density), default=0)
    mae = delay_mae(estimates)
    decision = route_decision(route)
    model = (
        f"MAE {mae['model_mae_hours']:.2f} h vs {mae['baseline_mae_hours']:.2f} h"
        if mae["model_mae_hours"] is not None
        else "No model yet"
    )
    steps = [
        (
            "",
            "1 · Ingest",
            f"{len(telemetry)} positions · {len(data['weather_alerts'])} alerts",
            f"{vessels} vessels; {counts.get('quarantined', 0)} records quarantined by the "
            "contract checks",
        ),
        (
            "",
            "2 · H3 index",
            f"{len({event['h3_index'] for event in risk_events})} cells visited",
            f"{len(hazard_cells)} cells covered by hazards at resolution {settings.resolution}",
        ),
        (
            "risk",
            "3 · Risk match",
            f"{alerted} alerted positions",
            f"{exposed} of {vessels} vessels were inside an active hazard at some point",
        ),
        (
            "",
            "4 · Corridor density",
            f"{len(density)} cell-windows",
            f"Busiest cell held {peak} vessels in one {settings.density_window_minutes}-min window",
        ),
        (
            "out",
            "5 · Analytics",
            model,
            "Delay model vs mean baseline (held-out MAE). "
            f"Route: {decision['verdict'].lower()}"
            + (f", {decision['detail']}" if decision["verdict"] == "Detour" else ""),
        ),
    ]
    st.html(
        '<p class="gr-flow-title">How this run flowed through the pipeline</p><div class="gr-flow">'
        + "".join(
            f'<div class="gr-step {kind}"><p class="gr-step-name">{escape(name)}</p>'
            f'<p class="gr-step-value">{escape(value)}</p>'
            f'<p class="gr-step-detail">{escape(detail)}</p></div>'
            for kind, name, value, detail in steps
        )
        + "</div>"
    )


def _render_kpis(
    st: Any,
    *,
    alerts: list[dict[str, Any]],
    exposed: set[str],
    positions: list[dict[str, Any]],
    risk_events: list[dict[str, Any]],
    route: dict[str, Any] | None,
    latest: str | None,
) -> None:
    ever_exposed = {row["vessel_id"] for row in risk_events if row.get("risk_alert")}
    alerted = sum(bool(row.get("risk_alert")) for row in risk_events)
    share = f"{alerted / len(risk_events):.0%}" if risk_events else "0%"
    now = f"At {format_time(latest)} UTC" if latest else "At the latest report"
    decision = route_decision(route)
    cards = (
        (
            "Active alerts",
            len(alerts),
            f"{now}: {_hazard_names(alerts)}",
            "Alerts whose validity window contains the latest report time, using the "
            "revision current at that time.",
        ),
        (
            "Vessels in a hazard",
            f"{len(exposed)} of {len(positions)}",
            f"{now}. {len(ever_exposed)} were inside a hazard at some point.",
            "A vessel counts when its latest position matched an active hazard cell. "
            "See Hazard exposure for the count over time.",
        ),
        (
            "Alerted positions",
            f"{alerted} of {len(risk_events)}",
            f"Whole scenario: {share} of position reports matched a hazard.",
            "Position reports whose H3 cell matched an active hazard.",
        ),
        (
            "Route decision",
            decision["verdict"],
            decision["detail"],
            "Baseline = fastest path. A detour is chosen when the baseline uses a lane "
            "above the risk threshold (see Route decision).",
        ),
    )
    for column, (label, value, caption, help_text) in zip(st.columns(4), cards, strict=True):
        with column.container(border=True, height="stretch"):
            st.metric(label, value, help=help_text)
            st.caption(caption)


# --- Map and inspector -------------------------------------------------------


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


def _render_map(
    st: Any,
    layers: dict[str, bool],
    *,
    positions: list[dict[str, Any]],
    exposed: set[str],
    alerts: list[dict[str, Any]],
    risk_events: list[dict[str, Any]],
    density_rows: list[dict[str, Any]],
    route: dict[str, Any] | None,
    at_time: str | None,
    window: str | None,
) -> None:
    from georisk.route_dashboard import map_layers

    st.subheader("Live map")
    if not positions and not alerts:
        st.info("No vessel telemetry or hazard alerts are available for the map.")
        return
    st.html(
        '<p class="gr-hint">'
        f"Situation at <b>{escape(format_time(at_time))} UTC</b>, the latest position report: "
        "vessel colours and hazard zones show that moment. Magenta hexagons summarise the "
        "whole scenario; blue columns show the density window "
        f"<b>{escape(format_time(window))} UTC</b> chosen in Corridor density. "
        "Hover for details, click a vessel or zone to inspect it.</p>"
    )
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
    st.pydeck_chart(
        map_layers.build_deck(built),
        height=560,
        on_select="rerun",
        selection_mode="single-object",
        key="live_map",
    )
    legend = "".join(LEGEND[name] for name, enabled in layers.items() if enabled)
    st.html(
        f'<div class="gr-legend">{legend}</div>'
        if legend
        else '<p class="gr-hint">All map layers are hidden. Turn them on in the sidebar.</p>'
    )


def _render_inspector(
    st: Any,
    positions: list[dict[str, Any]],
    latest_risk: dict[str, dict[str, Any]],
    risk_events: list[dict[str, Any]],
    alerts: list[dict[str, Any]],
    at_time: str | None,
    settings: GeoRiskConfig,
) -> None:
    st.subheader("Inspect")
    st.html('<p class="gr-hint">Click a vessel or hazard zone on the map, or choose one below.</p>')
    selected = _map_selection(st)
    if selected and selected != st.session_state.get("_map_selection_seen"):
        st.session_state["_map_selection_seen"] = selected
        st.session_state["inspect_vessel" if selected[0] == "vessels" else "inspect_alert"] = (
            selected[1]
        )

    vessel_ids = [row["vessel_id"] for row in positions]
    if vessel_ids:
        if st.session_state.get("inspect_vessel") not in vessel_ids:
            st.session_state.pop("inspect_vessel", None)
        exposed = {vessel for vessel, row in latest_risk.items() if row.get("risk_alert")}
        vessel = st.selectbox(
            "Inspect vessel",
            vessel_ids,
            key="inspect_vessel",
            format_func=lambda v: f"{v} · inside a hazard" if v in exposed else v,
        )
        position = next(row for row in positions if row["vessel_id"] == vessel)
        risk = latest_risk.get(vessel, {})
        history = vessel_exposure(risk_events, vessel)
        hazards = risk.get("matching_hazards") or []
        pill = (
            '<span class="gr-pill alert">Inside a hazard</span>'
            if risk.get("risk_alert")
            else '<span class="gr-pill ok">Clear of hazards</span>'
        )
        exposure = (
            f"{history['alerted_positions']} of {history['positions']} reports "
            f"({format_time(history['first_alerted'])}–{format_time(history['last_alerted'])} UTC)"
            if history["alerted_positions"]
            else f"Never, across {history['positions']} reports"
        )
        st.html(
            pill
            + _kv(
                [
                    ("Last report", f"{format_time(position['event_time'])} UTC"),
                    ("Position", f"{position['latitude']:.4f}° N, {position['longitude']:.4f}° E"),
                    ("Speed", f"{position.get('speed_knots', 0):.1f} kn"),
                    ("Heading", f"{position.get('heading_deg', 0):.0f}°"),
                    ("H3 cell", risk.get("h3_index")),
                    (
                        "Matched now",
                        ", ".join(
                            f"{h['alert_id']} {h['hazard_type'].replace('_', ' ')} "
                            f"(sev {h['severity']})"
                            for h in hazards
                        )
                        or "None",
                    ),
                    ("In a hazard", exposure),
                ]
            )
        )
    else:
        st.info("No vessels to inspect yet.")

    st.divider()
    alert_ids = [alert["alert_id"] for alert in alerts]
    if alert_ids and at_time:
        if st.session_state.get("inspect_alert") not in alert_ids:
            st.session_state.pop("inspect_alert", None)
        alert_id = st.selectbox("Inspect alert", alert_ids, key="inspect_alert")
        alert = next(row for row in alerts if row["alert_id"] == alert_id)
        status = alert_status(alert, at_time)
        inside = vessels_inside(alert_id, latest_risk)
        cells = len(
            polygon_to_cells(alert["geometry"], settings.resolution, settings.coverage_mode)
        )
        state = {
            "active": ("alert", f"Active · ends in {humanize_minutes(status['minutes'])}"),
            "upcoming": ("warn", f"Starts in {humanize_minutes(status['minutes'])}"),
            "expired": ("muted", f"Expired {humanize_minutes(status['minutes'])} ago"),
        }[status["state"]]
        st.html(
            f'<span class="gr-pill {state[0]}">{escape(state[1])}</span>'
            f'<p class="gr-card-title">{escape(alert["hazard_type"].replace("_", " ").title())}'
            f" · {_severity_bar(alert['severity'])} {alert['severity']}/5</p>"
            + _kv(
                [
                    (
                        "Valid",
                        f"{format_time(alert['valid_from'])}–"
                        f"{format_time(alert['valid_until'])} UTC",
                    ),
                    (
                        "Coverage",
                        f"{cells} H3 cells (res {settings.resolution}, {settings.coverage_mode})",
                    ),
                    ("Vessels inside", ", ".join(inside) or "None at the latest report"),
                ]
            )
        )
    else:
        st.info("No hazard alerts are active at the latest event time.")


# --- Tabs -------------------------------------------------------------------


def _render_risk_panel(st: Any, risk_events: list[dict[str, Any]], settings: GeoRiskConfig) -> None:
    st.subheader("Risk summary")
    st.caption(
        f"Every position report is mapped to an H3 cell (res {settings.resolution}, "
        f"{settings.coverage_mode} coverage) and flagged when that cell is covered by a hazard "
        "that is valid at the report's time. This is a fast candidate screen, not exact "
        "polygon containment. Severity runs from 1 (minor) to 5 (severe)."
    )
    if not risk_events:
        st.info("No risk events yet. Run `georisk run` to process telemetry against hazard alerts.")
        return
    timeline = exposure_series(risk_events)
    hazard_types = sorted(
        {
            hazard["hazard_type"]
            for event in risk_events
            for hazard in event.get("matching_hazards") or []
        }
    )
    times = sorted({event["event_time"] for event in risk_events}, key=_utc)
    type_column, severity_column, time_column = st.columns([1.3, 1, 1.6])
    chosen_types = type_column.multiselect(
        "Hazard type",
        hazard_types,
        placeholder="All hazard types",
        key="risk_types",
        format_func=lambda kind: kind.replace("_", " "),
    )
    min_severity = severity_column.slider("Minimum severity", 1, 5, 1, key="risk_severity")
    start, end = (times[0], times[-1]) if times else (None, None)
    if len(times) > 1:
        start, end = time_column.select_slider(
            "Event time (UTC)",
            times,
            value=(times[0], times[-1]),
            key="risk_time",
            format_func=format_time,
        )
    filtered = filter_risk_events(
        risk_events,
        hazard_types=chosen_types or None,
        min_severity=min_severity,
        start=start,
        end=end,
    )
    alerted_total = sum(bool(event.get("risk_alert")) for event in risk_events)

    timeline_column, severity_chart_column = st.columns([1.6, 1], gap="large")
    with timeline_column:
        st.markdown("**Vessels inside a hazard over time** (all hazards)")
        st.area_chart(
            {
                "Time (UTC)": chart_time_labels(row["event_time"] for row in timeline),
                "Vessels": [row["exposed_vessels"] for row in timeline],
            },
            x="Time (UTC)",
            y="Vessels",
            color="#E03A1E",
            height=230,
        )
    with severity_chart_column:
        st.markdown("**Alerted positions by severity** (follows the filters)")
        distribution = severity_distribution(filtered)
        st.bar_chart(
            {
                "Severity": [str(level) for level in range(1, 6)],
                "Alerted positions": [distribution.get(level, 0) for level in range(1, 6)],
            },
            x="Severity",
            y="Alerted positions",
            color="#A8166F",
            height=230,
            sort=False,
        )

    if filtered:
        rows = [
            {**row, "time": format_time(row["event_time"])} for row in risk_table_rows(filtered)
        ]
        st.dataframe(
            rows,
            width="stretch",
            hide_index=True,
            height=320,
            column_order=(
                "time",
                "vessel_id",
                "hazards",
                "max_severity",
                "h3_index",
                "latitude",
                "longitude",
            ),
            column_config={
                "time": st.column_config.TextColumn("Time (UTC)"),
                "vessel_id": st.column_config.TextColumn("Vessel"),
                "hazards": st.column_config.TextColumn("Matched hazards"),
                "max_severity": st.column_config.ProgressColumn(
                    "Severity", min_value=0, max_value=5, format="%d"
                ),
                "h3_index": st.column_config.TextColumn("H3 cell"),
                "latitude": st.column_config.NumberColumn("Lat (°N)", format="%.4f"),
                "longitude": st.column_config.NumberColumn("Lon (°E)", format="%.4f"),
            },
        )
        st.caption(f"Showing {len(filtered)} of {alerted_total} alerted positions, newest first.")
    else:
        st.info(
            "No alerted positions match these filters. Lower the minimum severity, "
            "widen the time range, or clear the hazard types."
        )


def _window_label(rows: list[dict[str, Any]], window_start: str) -> str:
    """Label a window ``12:00–12:15``; values that are not window starts pass through."""
    end = next((row["window_end"] for row in rows if row["window_start"] == window_start), None)
    return f"{format_time(window_start)}–{format_time(end)}" if end else window_start


def _render_density_panel(
    st: Any,
    density_metrics: list[dict[str, Any]],
    settings: GeoRiskConfig,
) -> str | None:
    """Render the density view and return the window the map should show."""
    st.subheader("Corridor density")
    st.caption(
        f"Congestion = the number of different vessels seen in one H3 cell during one "
        f"{settings.density_window_minutes}-minute window. Repeated reports from the same "
        "vessel count once, so this measures vessels, not message volume."
    )
    windows = density_windows(density_metrics)
    if not windows:
        st.info(
            "No density metrics yet. Run `georisk run` to aggregate vessel positions "
            "into H3 windows."
        )
        return None
    trend = density_trend(density_metrics)
    st.markdown("**Congestion across the scenario**")
    st.line_chart(
        {
            "Window start (UTC)": chart_time_labels(row["window_start"] for row in trend),
            "Busiest cell (vessels)": [row["peak"] for row in trend],
            "Occupied cells": [row["cells"] for row in trend],
        },
        x="Window start (UTC)",
        y=["Busiest cell (vessels)", "Occupied cells"],
        color=["#1D5C80", "#9CC9DB"],
        height=220,
    )
    window = windows[-1]
    if len(windows) > 1:
        window = st.select_slider(
            "Window (UTC) — also drives the map's density columns",
            windows,
            value=windows[-1],
            key="density_window",
            format_func=lambda start: _window_label(density_metrics, start),
        )
    rows = density_for_window(density_metrics, window)
    peak = max(row["unique_vessel_count"] for row in rows)
    current_column, compare_column = st.columns([1, 1.4], gap="large")
    with current_column:
        st.markdown(f"**Busiest cells · {_window_label(density_metrics, window)} UTC**")
        st.dataframe(
            rows,
            width="stretch",
            hide_index=True,
            height=300,
            column_order=("h3_index", "unique_vessel_count"),
            column_config={
                "h3_index": st.column_config.TextColumn("H3 cell"),
                "unique_vessel_count": st.column_config.ProgressColumn(
                    "Unique vessels", min_value=0, max_value=max(peak, 1), format="%d"
                ),
            },
        )
    with compare_column:
        st.markdown("**Same cells in the previous and next window**")
        comparison = [
            {**row, "change": None if row["previous"] is None else row["current"] - row["previous"]}
            for row in compare_adjacent_windows(density_metrics, window)
        ]
        st.dataframe(
            comparison,
            width="stretch",
            hide_index=True,
            height=300,
            column_order=("h3_index", "previous", "current", "next", "change"),
            column_config={
                "h3_index": st.column_config.TextColumn("H3 cell"),
                "previous": st.column_config.NumberColumn("Previous", format="%d"),
                "current": st.column_config.NumberColumn("Selected", format="%d"),
                "next": st.column_config.NumberColumn("Next", format="%d"),
                "change": st.column_config.NumberColumn("Change vs previous", format="%+d"),
            },
        )
        st.caption(
            "A quiet window shows 0; a blank means that window is outside the processed time range."
        )
    return window


def _prediction_chart(rows: list[dict[str, Any]]) -> Any:
    """Predicted vs synthetic label with a dashed y = x reference line."""
    import altair as alt

    points = [
        {"label": row["synthetic_label_hours"], "predicted": row["predicted_delay_hours"]}
        for row in rows
    ]
    top = max(max(point["label"], point["predicted"]) for point in points) * 1.05 or 1.0
    bottom = min(0.0, *(point["predicted"] for point in points))
    scale = alt.Scale(domain=[bottom, top])
    scatter = (
        alt.Chart(alt.Data(values=points))
        .mark_circle(size=60, opacity=0.75, color="#1D5C80")
        .encode(
            x=alt.X("label:Q", title="Synthetic label (h)", scale=scale),
            y=alt.Y("predicted:Q", title="Predicted delay (h)", scale=scale),
            tooltip=[
                alt.Tooltip("label:Q", title="Label (h)", format=".2f"),
                alt.Tooltip("predicted:Q", title="Predicted (h)", format=".2f"),
            ],
        )
    )
    diagonal = (
        alt.Chart(alt.Data(values=[{"v": bottom}, {"v": top}]))
        .mark_line(strokeDash=[5, 4], color="#5B6B78")
        .encode(x="v:Q", y="v:Q")
    )
    return (diagonal + scatter).properties(height=260)


def _render_delay_panel(st: Any, estimates: list[dict[str, Any]]) -> None:
    st.subheader("Delay estimates")
    st.caption("Synthetic demonstration, not a real-world forecast.")
    if not estimates:
        st.info(
            "No delay estimates yet. Run `georisk run` to train the delay model and write "
            "processed/delay-estimates.jsonl."
        )
        return
    mae = delay_mae(estimates)
    model_name = estimates[-1].get("model_name", "model")
    columns = st.columns(4)
    if mae["model_mae_hours"] is None:
        columns[0].metric("Model MAE (h)", "—", border=True)
        columns[1].metric("Baseline MAE (h)", "—", border=True)
        columns[3].metric("Error reduction", "—", border=True)
    else:
        columns[0].metric(
            f"Model MAE (h) · {model_name}",
            f"{mae['model_mae_hours']:.2f}",
            border=True,
            delta=f"{mae['model_mae_hours'] - mae['baseline_mae_hours']:+.2f} h vs baseline",
            delta_color="inverse",
            help="Mean absolute error on held-out rows (lower is better).",
        )
        columns[1].metric(
            "Baseline MAE (h)",
            f"{mae['baseline_mae_hours']:.2f}",
            border=True,
            help="Predicting the training-set mean delay for every row.",
        )
        reduction = (
            1 - mae["model_mae_hours"] / mae["baseline_mae_hours"]
            if mae["baseline_mae_hours"]
            else None
        )
        columns[3].metric(
            "Error reduction",
            f"{reduction:.0%}" if reduction is not None else "—",
            border=True,
            help="How much lower the model's MAE is than the mean baseline's.",
        )
    columns[2].metric(
        "Held-out rows",
        mae["evaluation_count"],
        border=True,
        help="The latest rows by event time, never seen in training (chronological split).",
    )

    held_out = [row for row in estimates if row.get("split") == "evaluation"]
    chart_column, explain_column = st.columns([1.4, 1], gap="large")
    with chart_column:
        st.markdown("**Held-out rows: prediction vs synthetic label**")
        if held_out:
            st.altair_chart(_prediction_chart(held_out), width="stretch")
            st.caption(
                "Points on the dashed diagonal are perfect predictions. The horizontal spread "
                "is the random noise in the synthetic label (σ = 0.25 h), which no model can "
                "predict."
            )
        else:
            st.info("No held-out rows in this run.")
    with explain_column:
        st.markdown("**How the label is built**")
        st.code(
            "delay_h = max(0, 0.05·sailing_h + 0.015·density\n"
            "              + 0.45·severity + 0.002·distance_nm\n"
            "              − 0.01·speed_kn + noise)",
            language=None,
        )
        st.caption(
            "Rows are sorted by event time; the last 20% are held out. The label is derived "
            "from the same features, so low error shows the workflow, not real predictive skill."
        )

    show_truth = st.toggle(
        "Demo mode: show synthetic ground truth",
        key="delay_truth",
        help="Adds the synthetic label the model was trained to predict.",
    )
    columns_shown = [
        "vessel_id",
        "time",
        "predicted_delay_hours",
        "baseline_delay_hours",
        *(["synthetic_label_hours"] if show_truth else []),
        "error_hours",
        "hazard_severity",
        "density",
        "speed_knots",
        "route_distance_nm",
        "baseline_hours",
    ]
    st.markdown("**Each vessel's latest estimate**")
    st.dataframe(
        [
            {**row, "time": format_time(row["event_time"])}
            for row in latest_delay_estimates(estimates)
        ],
        width="stretch",
        hide_index=True,
        column_order=columns_shown,
        column_config={
            "vessel_id": st.column_config.TextColumn("Vessel"),
            "time": st.column_config.TextColumn("Report (UTC)"),
            "predicted_delay_hours": st.column_config.NumberColumn(
                "Predicted delay (h)", format="%.2f"
            ),
            "baseline_delay_hours": st.column_config.NumberColumn("Baseline (h)", format="%.2f"),
            "error_hours": st.column_config.NumberColumn(
                "Error (h)", format="%+.2f", help="Predicted minus synthetic label."
            ),
            "synthetic_label_hours": st.column_config.NumberColumn(
                "Synthetic label (h)", format="%.2f"
            ),
            "hazard_severity": st.column_config.NumberColumn("Hazard severity", format="%d"),
            "density": st.column_config.NumberColumn("Cell density (vessels)", format="%d"),
            "speed_knots": st.column_config.NumberColumn("Speed (kn)", format="%.1f"),
            "route_distance_nm": st.column_config.NumberColumn(
                "Distance to Port B (nm)", format="%.1f"
            ),
            "baseline_hours": st.column_config.NumberColumn(
                "Sailing time (h)", format="%.2f", help="Distance ÷ speed."
            ),
        },
    )
    st.caption(
        "Feature columns are the model inputs. Distance is straight-line to Port B, "
        "not the selected route."
    )


def _render_route_panel(
    st: Any,
    route: dict[str, Any] | None,
    route_error: str | None,
    scenario: str,
) -> None:
    st.subheader("Route comparison")
    st.caption(
        f"Scenario: {scenario}. Each lane's risk = 0.7 × (hazard severity ÷ 5) + 0.3 × "
        "(congestion ÷ 5 vessels), capped at 1. The baseline is the fastest path; lanes above "
        "the threshold are closed to the alternative, which minimises hours + penalty × risk. "
        "Synthetic demonstration."
    )
    if route_error:
        st.warning(f"{route_error}. Raise the risk threshold or choose another scenario.")
        return
    if route is None:
        st.info("No vessel telemetry is available for route scoring. Run `georisk run` first.")
        return
    explanation = _friendly_nodes(route["explanation"])
    if route.get("alternative") is None:
        st.error(explanation, icon=":material/block:")
    elif route["route_changed"]:
        st.warning(explanation, icon=":material/alt_route:")
    else:
        st.success(explanation, icon=":material/check_circle:")

    penalty = route["risk_penalty"]
    baseline, alternative = route["baseline"], route.get("alternative")
    baseline_column, alternative_column = st.columns(2, gap="medium")
    with baseline_column.container(border=True):
        st.markdown(f"**Baseline · fastest**  \n{_path_label(baseline['path'])}")
        hours, risk, cost = st.columns(3)
        hours.metric("Sailing time (h)", f"{baseline['travel_hours']:.2f}")
        risk.metric("Total risk", f"{baseline['risk_score']:.2f}")
        cost.metric(f"Cost (h + {penalty:g}×risk)", f"{baseline['total_cost']:.2f}")
    with alternative_column.container(border=True):
        if alternative is None:
            st.markdown("**Alternative · risk-aware**  \nNo eligible route")
            st.caption("Every path uses at least one lane above the threshold.")
        else:
            st.markdown(f"**Alternative · risk-aware**  \n{_path_label(alternative['path'])}")
            hours, risk, cost = st.columns(3)
            hours.metric(
                "Sailing time (h)",
                f"{alternative['travel_hours']:.2f}",
                delta=f"{alternative['travel_hours'] - baseline['travel_hours']:+.2f} h",
                delta_color="inverse",
            )
            risk.metric(
                "Total risk",
                f"{alternative['risk_score']:.2f}",
                delta=f"{alternative['risk_score'] - baseline['risk_score']:+.2f}",
                delta_color="inverse",
            )
            cost.metric(
                f"Cost (h + {penalty:g}×risk)",
                f"{alternative['total_cost']:.2f}",
                delta=f"{alternative['total_cost'] - baseline['total_cost']:+.2f}",
                delta_color="inverse",
            )

    st.markdown(
        f"**Lane status at {format_time(route['at_time'])} UTC** · "
        f"threshold {route['risk_threshold']:.2f}"
    )
    st.dataframe(
        [
            {**row, "lane": _friendly_nodes(row["lane"])}
            for row in lane_status(route["edge_scores"], route)
        ],
        width="stretch",
        hide_index=True,
        column_config={
            "lane": st.column_config.TextColumn("Lane"),
            "distance_nm": st.column_config.NumberColumn("Distance (nm)", format="%.1f"),
            "travel_hours": st.column_config.NumberColumn("Hours", format="%.2f"),
            "risk": st.column_config.ProgressColumn(
                "Risk", min_value=0.0, max_value=1.0, format="%.2f"
            ),
            "risk_detail": st.column_config.TextColumn(
                "Risk drivers", help="Max hazard severity (alert IDs) and max vessel density."
            ),
            "status": st.column_config.TextColumn(
                "Status", help="blocked = risk above the threshold; closed to the alternative."
            ),
            "used_by": st.column_config.TextColumn("Used by"),
        },
    )


def _render_history(st: Any, lake_dir: str | Path) -> None:
    with st.expander("History", icon=":material/history:"):
        st.caption(f"Daily totals from the curated Parquet lake at {lake_dir}.")
        try:
            history = daily_history(lake_dir)
        except (OSError, ValueError) as exc:
            st.error(f"The lake could not be read: {exc}")
            return
        if not history:
            st.info(
                "The lake has no curated risk or density data yet. Run `georisk run` to "
                "write partitioned Parquet files."
            )
            return
        st.dataframe(
            history,
            width="stretch",
            hide_index=True,
            column_config={
                "event_date": st.column_config.TextColumn("Day (UTC)"),
                "risk_events": st.column_config.NumberColumn("Risk events", format="%d"),
                "alerted_events": st.column_config.NumberColumn("Alerted positions", format="%d"),
                "exposed_vessels": st.column_config.NumberColumn("Exposed vessels", format="%d"),
                "max_density": st.column_config.NumberColumn(
                    "Peak density (vessels/cell)", format="%d"
                ),
            },
        )


def _render_quality_panel(
    st: Any,
    *,
    metadata: dict[str, Any] | None,
    report: dict[str, Any] | None,
    quarantine: list[dict[str, Any]],
    settings: GeoRiskConfig,
    lake_dir: str | Path,
) -> None:
    st.subheader("Run quality")
    st.caption(
        "Evidence that this run is complete and consistent: acceptance checks from the run "
        "report, record accounting, and the settings that produced the outputs."
    )
    summary = acceptance_summary(report)
    if summary is None:
        st.info(
            f"No run report found at {settings.report_path}. Run `georisk run`, or set the "
            "report path under Data sources in the sidebar."
        )
    else:
        message = (
            f"{summary['passed_count']} of {summary['total']} acceptance checks passed "
            f"(checked {format_datetime(summary['checked_at'])})."
        )
        if summary["passed"]:
            st.success(message, icon=":material/verified:")
        else:
            st.error(message, icon=":material/error:")
        st.dataframe(
            [
                {**check, "result": "✓ pass" if check["passed"] else "✗ FAIL"}
                for check in summary["checks"]
            ],
            width="stretch",
            hide_index=True,
            height=min(38 + 35 * len(summary["checks"]), 420),
            column_order=("check", "result", "detail"),
            column_config={
                "check": st.column_config.TextColumn("Check"),
                "result": st.column_config.TextColumn("Result", width="small"),
                "detail": st.column_config.TextColumn("What it verifies"),
            },
        )

    counts = (metadata or {}).get("counts") or {}
    st.markdown("**Record accounting (processing stage)**")
    columns = st.columns(6)
    for column, (label, key, help_text) in zip(
        columns,
        (
            ("Positions in", "telemetry_in", "Telemetry records read."),
            ("Alerts in", "alerts_in", "Weather-alert records read, including revisions."),
            ("Quarantined", "quarantined", "Records that failed a contract or time check."),
            ("Duplicates", "duplicates_dropped", "Repeated event IDs, counted once."),
            ("Late", "late_dropped", "Older than the watermark (latest event − lateness)."),
            ("Future", "future_dropped", "Stamped beyond the allowed clock skew."),
        ),
        strict=True,
    ):
        column.metric(label, counts.get(key, "—"), border=True, help=help_text)
    reasons = count_by(quarantine, "reason")
    if reasons:
        st.caption(
            "Quarantine reasons: " + ", ".join(f"{reason} ({n})" for reason, n in reasons.items())
        )
    else:
        st.caption("No records were quarantined in this run.")

    metadata = metadata or {}
    st.markdown("**Settings used for these outputs**")
    st.html(
        _kv(
            [
                ("H3 resolution", metadata.get("h3_resolution", settings.resolution)),
                ("Coverage mode", metadata.get("coverage_mode", settings.coverage_mode)),
                (
                    "Density window",
                    f"{metadata.get('window_minutes', settings.density_window_minutes)} min",
                ),
                (
                    "Allowed lateness",
                    f"{metadata.get('allowed_lateness_minutes', settings.allowed_lateness_minutes)}"
                    " min",
                ),
                (
                    "Future-event limit",
                    f"{metadata.get('max_future_minutes', settings.max_future_minutes)} min",
                ),
            ]
        )
    )
    _render_history(st, lake_dir)


# --- Page assembly -------------------------------------------------------------


def _render_body(st: Any, settings: GeoRiskConfig, controls: dict[str, Any]) -> None:
    """Render everything that depends on loaded data; reruns alone when auto-refreshing."""
    processed_dir = controls["processed_dir"]
    data = load_dashboard_data(input_dir=controls["input_dir"], processed_dir=processed_dir)
    metadata = load_run_metadata(processed_dir)
    settings = effective_settings(settings, metadata)
    summary = dashboard_summary(data, resolution=settings.resolution)
    estimates = load_delay_estimates(processed_dir)

    at_time = summary["latest_event_time"]
    alerts = active_alerts(data["weather_alerts"], at_time) if at_time else []
    positions = latest_positions(data["telemetry"])
    latest_risk = latest_risk_by_vessel(data["risk_events"])
    exposed = {vessel for vessel, row in latest_risk.items() if row.get("risk_alert")}

    scenario = controls["scenario"]
    route_time = latest_timestamp(row["event_time"] for row in data["telemetry"]) or at_time
    scenario_alerts = (
        route_scenario_alerts(scenario, data["weather_alerts"], route_time) if route_time else []
    )
    map_alerts = active_alerts(scenario_alerts, route_time) if route_time else []
    try:
        route = dashboard_route(
            data,
            resolution=settings.resolution,
            coverage_mode=settings.coverage_mode,
            risk_threshold=controls["threshold"],
            risk_penalty=controls["penalty"],
            alerts=scenario_alerts,
        )
        route_error = None
    except ValueError as exc:
        route, route_error = None, str(exc)

    header_column, freshness_column = st.columns([2.2, 1], gap="large", vertical_alignment="top")
    with header_column:
        _render_header(st, settings, data)
    with freshness_column:
        _render_freshness(st, metadata, settings)
    _render_pipeline(
        st, data=data, metadata=metadata, settings=settings, estimates=estimates, route=route
    )
    _render_kpis(
        st,
        alerts=alerts,
        exposed=exposed,
        positions=positions,
        risk_events=data["risk_events"],
        route=route,
        latest=at_time,
    )
    if not summary["has_data"]:
        st.warning(
            "No vessel telemetry or risk events were found in the selected directories. "
            "Run `georisk run` (or start a replay), then check the data sources in the "
            "sidebar.",
            icon=":material/database:",
        )

    map_column, inspect_column = st.columns([2.6, 1], gap="medium")
    tabs = st.tabs(list(TABS))
    with tabs[0]:
        _render_risk_panel(st, data["risk_events"], settings)
    with tabs[1]:
        window = _render_density_panel(st, data["density_metrics"], settings)
    with tabs[2]:
        _render_delay_panel(st, estimates)
    with tabs[3]:
        _render_route_panel(st, route, route_error, scenario)
    with tabs[4]:
        _render_quality_panel(
            st,
            metadata=metadata,
            report=load_run_report(controls["report_path"]),
            quarantine=load_quarantine(processed_dir),
            settings=settings,
            lake_dir=controls["lake_dir"],
        )
    density_rows = density_for_window(data["density_metrics"], window) if window else []
    with map_column:
        _render_map(
            st,
            controls["layers"],
            positions=positions,
            exposed=exposed,
            alerts=map_alerts,
            risk_events=data["risk_events"],
            density_rows=density_rows,
            route=route,
            at_time=route_time,
            window=window,
        )
        if scenario != ROUTE_SCENARIOS[0]:
            st.caption(
                f"Route what-if “{scenario}” is active: hazard zones and routes on the "
                "map reflect the scenario, not the loaded alerts."
            )
    with inspect_column:
        with st.container(height=700, border=True):
            _render_inspector(
                st,
                positions,
                latest_risk,
                data["risk_events"],
                map_alerts,
                route_time,
                settings,
            )


PRESENTER_GUIDE = """
1. **Header and pipeline strip** – what the data is and how one run flows.
2. **KPIs and map** – the situation at the latest report; red vessels are inside an
   active hazard. Click a vessel or zone to inspect it.
3. **Hazard exposure** – filter by type, severity, and time.
4. **Corridor density** – move the window slider; the map follows.
5. **Delay model** – model vs mean baseline on held-out rows; toggle ground truth.
6. **Route decision** – try *Close inner lane*, or a threshold of 1.0.
7. **Run quality** – acceptance checks and record accounting.
"""


def _sidebar_controls(st: Any, settings: GeoRiskConfig) -> dict[str, Any]:
    sidebar = st.sidebar
    with sidebar.expander("Presenter guide", icon=":material/slideshow:"):
        st.markdown(PRESENTER_GUIDE)
    sidebar.subheader("Route what-if")
    scenario = sidebar.selectbox(
        "Hazard scenario",
        ROUTE_SCENARIOS,
        key="route_scenario",
        help="As generated uses the loaded alerts. No hazards removes them. Close inner lane "
        "adds a synthetic severity-5 closure at the chokepoint.",
    )
    threshold = sidebar.slider(
        "Route risk threshold",
        0.0,
        1.0,
        settings.route_risk_threshold,
        0.05,
        key="route_threshold",
        help="Lanes with risk above this are closed to the alternative route.",
    )
    penalty = sidebar.number_input(
        "Route risk penalty (h per unit risk)",
        min_value=0.0,
        value=settings.route_risk_penalty,
        key="route_penalty",
        help="Hours added per unit of lane risk when ranking the alternative route.",
    )
    sidebar.subheader("Map layers")
    layers = {
        name: sidebar.checkbox(label, value=True, key=f"layer_{name}") for name, label in MAP_LAYERS
    }
    sidebar.subheader("Live")
    auto_refresh = sidebar.toggle(
        "Auto-refresh every 10 s",
        key="auto_refresh",
        help="Reload the files every 10 seconds while a replay or rerun is writing them.",
    )
    with sidebar.expander("Data sources"):
        input_dir = st.text_input("Input directory", str(settings.input_dir), key="input_dir")
        processed_dir = st.text_input(
            "Processed directory", str(settings.processed_dir), key="processed_dir"
        )
        lake_dir = st.text_input("Lake directory", str(settings.lake_dir), key="lake_dir")
        report_path = st.text_input("Run report", str(settings.report_path), key="report_path")
    return {
        "scenario": scenario,
        "threshold": threshold,
        "penalty": penalty,
        "layers": layers,
        "auto_refresh": auto_refresh,
        "input_dir": input_dir,
        "processed_dir": processed_dir,
        "lake_dir": lake_dir,
        "report_path": report_path,
    }


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
    st.html(CSS)
    controls = _sidebar_controls(st, settings)
    if controls["auto_refresh"]:
        st.fragment(run_every=10)(_render_body)(st, settings, controls)
    else:
        _render_body(st, settings, controls)


if __name__ == "__main__":
    main()

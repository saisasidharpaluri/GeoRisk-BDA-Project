"""PyDeck layer builders for the dashboard map; no Streamlit dependency."""

from __future__ import annotations

from html import escape
from typing import Any, Iterable, Mapping

import pydeck as pdk

from georisk.geo_demo import DEMO_BBOX

# Nautical-chart palette shared with theme.toml: magenta marks hazards as on
# paper charts, navy ink marks traffic, buoy green marks the safe passage.
INK = [20, 35, 46]
CHART_MAGENTA = [168, 22, 111]
ALERT_RED = [224, 58, 30]
BASELINE_SLATE = [91, 107, 120]
ALTERNATIVE_GREEN = [14, 124, 102]
SHOAL_BLUE = [156, 201, 219]
DEEP_BLUE = [29, 92, 128]
PAPER = [242, 245, 244]

NODE_LABELS = {
    "port_a": "Port A", "port_b": "Port B",
    "chokepoint": "Inner lane", "outer_lane": "Outer lane",
}


def _tooltip(title: str, *lines: str) -> str:
    body = "".join(f"<div>{escape(line)}</div>" for line in lines)
    return f"<b>{escape(title)}</b>{body}"


def _ramp(fraction: float, alpha: int) -> list[int]:
    fraction = min(1.0, max(0.0, fraction))
    return [round(a + (b - a) * fraction) for a, b in zip(SHOAL_BLUE, DEEP_BLUE)] + [alpha]


def vessels_layer(
    positions: Iterable[Mapping[str, Any]], alerted_vessel_ids: Iterable[str],
) -> pdk.Layer:
    """Latest vessel positions; vessels inside an active hazard are red."""
    alerted = set(alerted_vessel_ids)
    data = []
    for row in positions:
        is_alerted = row["vessel_id"] in alerted
        data.append({
            "vessel_id": row["vessel_id"],
            "position": [row["longitude"], row["latitude"]],
            "alerted": is_alerted,
            "color": (ALERT_RED if is_alerted else INK) + [235],
            "tooltip": _tooltip(
                row["vessel_id"],
                "In active hazard" if is_alerted else "Clear of hazards",
                f"{row.get('speed_knots', '?')} kn · heading {row.get('heading_deg', '?')}°",
                f"{row['event_time']} UTC",
            ),
        })
    return pdk.Layer(
        "ScatterplotLayer", data, id="vessels", pickable=True, stroked=True, filled=True,
        get_position="position", get_fill_color="color", get_line_color=PAPER + [255],
        get_radius=6, radius_units="'pixels'", line_width_min_pixels=1.5,
    )


def hazards_layer(active_alerts: Iterable[Mapping[str, Any]]) -> pdk.Layer:
    """Hazard polygons; rings keep the GeoJSON [lon, lat] order deck.gl expects."""
    data = []
    for alert in active_alerts:
        geometry = alert["geometry"]
        polygons = ([geometry["coordinates"]] if geometry["type"] == "Polygon"
                    else geometry["coordinates"])
        for rings in polygons:
            data.append({
                "alert_id": alert["alert_id"],
                "polygon": [[float(lon), float(lat)] for lon, lat in rings[0]],
                "severity": alert["severity"],
                "fill": CHART_MAGENTA + [18 + 10 * alert["severity"]],
                "tooltip": _tooltip(
                    f"{alert['alert_id']} · {alert['hazard_type'].replace('_', ' ')}",
                    f"Severity {alert['severity']} of 5",
                    f"Valid {alert['valid_from']} → {alert['valid_until']}",
                ),
            })
    return pdk.Layer(
        "PolygonLayer", data, id="hazards", pickable=True, stroked=True, filled=True,
        get_polygon="polygon", get_fill_color="fill", get_line_color=CHART_MAGENTA + [230],
        line_width_min_pixels=2,
    )


def risk_cells_layer(risk_events: Iterable[Mapping[str, Any]]) -> pdk.Layer:
    """H3 cells where at least one position matched an active hazard."""
    cells: dict[str, dict[str, Any]] = {}
    for event in risk_events:
        if not event.get("risk_alert"):
            continue
        cell = cells.setdefault(event["h3_index"], {"events": 0, "vessels": set(), "severity": 0})
        cell["events"] += 1
        cell["vessels"].add(event["vessel_id"])
        cell["severity"] = max([cell["severity"], *(
            hazard.get("severity", 0) for hazard in event.get("matching_hazards") or [])])
    data = [
        {
            "hex": h3_index, "events": cell["events"], "severity": cell["severity"],
            "fill": CHART_MAGENTA + [60 + 25 * cell["severity"]],
            "tooltip": _tooltip(
                f"Risk cell {h3_index}", f"{cell['events']} alerted positions",
                f"{len(cell['vessels'])} vessels · max severity {cell['severity']}",
            ),
        }
        for h3_index, cell in sorted(cells.items())
    ]
    return pdk.Layer(
        "H3HexagonLayer", data, id="risk_cells", pickable=True, filled=True, extruded=False,
        stroked=True, get_hexagon="hex", get_fill_color="fill",
        get_line_color=CHART_MAGENTA + [200], line_width_min_pixels=1,
    )


def density_layer(rows: Iterable[Mapping[str, Any]]) -> pdk.Layer:
    """Extruded corridor density; elevation is 200 m per unique vessel."""
    rows = list(rows)
    peak = max((row["unique_vessel_count"] for row in rows), default=1) or 1
    data = [
        {
            "hex": row["h3_index"], "count": row["unique_vessel_count"],
            "elevation": row["unique_vessel_count"] * 200,
            "fill": _ramp(row["unique_vessel_count"] / peak, 170),
            "tooltip": _tooltip(
                f"Density cell {row['h3_index']}",
                f"{row['unique_vessel_count']} unique vessels",
                f"Window {row['window_start']} → {row.get('window_end', '?')}",
            ),
        }
        for row in rows
    ]
    return pdk.Layer(
        "H3HexagonLayer", data, id="density", pickable=True, filled=True, extruded=True,
        get_hexagon="hex", get_fill_color="fill", get_elevation="elevation",
        elevation_scale=1, coverage=0.85,
    )


def route_layer(
    route: Mapping[str, Any] | None, nodes: Mapping[str, tuple[float, float]],
) -> pdk.Layer:
    """Baseline (slate) under the risk-aware alternative (green)."""
    data = []
    if route is not None:
        for name, color, width in (("baseline", BASELINE_SLATE, 7), ("alternative", ALTERNATIVE_GREEN, 4)):
            leg = route[name]
            data.append({
                "name": name,
                "path": [[nodes[node][1], nodes[node][0]] for node in leg["path"]],
                "color": color + [230], "width": width,
                "tooltip": _tooltip(
                    f"{name.title()} route", " → ".join(NODE_LABELS.get(n, n) for n in leg["path"]),
                    f"{leg['travel_hours']:.2f} h · risk {leg['risk_score']:.2f}",
                ),
            })
    return pdk.Layer(
        "PathLayer", data, id="route", pickable=True, get_path="path", get_color="color",
        get_width="width", width_units="'pixels'", cap_rounded=True, joint_rounded=True,
    )


def node_layers(nodes: Mapping[str, tuple[float, float]]) -> list[pdk.Layer]:
    """Port and waypoint markers with labels, so routes read against the coast."""
    data = [
        {"node": name, "label": NODE_LABELS.get(name, name), "position": [lon, lat],
         "tooltip": _tooltip(NODE_LABELS.get(name, name), f"{lat:.2f}° N, {lon:.2f}° E")}
        for name, (lat, lon) in nodes.items()
    ]
    return [
        pdk.Layer("ScatterplotLayer", data, id="nodes", pickable=True, stroked=True,
                  get_position="position", get_fill_color=PAPER + [255],
                  get_line_color=INK + [255], get_radius=7, radius_units="'pixels'",
                  line_width_min_pixels=2),
        pdk.Layer("TextLayer", data, id="node_labels", get_position="position",
                  get_text="label", get_size=13, get_color=INK + [255],
                  get_alignment_baseline="'bottom'", get_pixel_offset=[0, -12],
                  font_family="'IBM Plex Mono, monospace'", font_weight=600,
                  outline_width=3, outline_color=PAPER + [255],
                  font_settings={"sdf": True}),
    ]


def build_deck(layers: Iterable[pdk.Layer], *, pitch: float = 30) -> pdk.Deck:
    """Frame the demo bounding box off Chennai with one shared HTML tooltip."""
    view = pdk.ViewState(
        latitude=(DEMO_BBOX["lat_min"] + DEMO_BBOX["lat_max"]) / 2,
        longitude=(DEMO_BBOX["lon_min"] + DEMO_BBOX["lon_max"]) / 2,
        zoom=7.9, pitch=pitch, bearing=0,
    )
    return pdk.Deck(
        layers=list(layers), initial_view_state=view, map_provider="carto", map_style="light",
        tooltip={"html": "{tooltip}", "style": {
            "backgroundColor": "#14232E", "color": "#F2F5F4", "fontSize": "12px",
            "fontFamily": "'IBM Plex Mono', monospace", "padding": "8px 10px",
            "borderRadius": "4px",
        }},
    )

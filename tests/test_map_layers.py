import pytest

pytest.importorskip("pydeck")

from georisk.geo_demo import DEMO_BBOX, DEMO_NODES  # noqa: E402
from georisk.route_dashboard.map_layers import (  # noqa: E402
    ALERT_RED, INK, build_deck, density_layer, hazards_layer, node_layers, risk_cells_layer,
    route_layer, vessels_layer,
)
from georisk.route_dashboard.route_optimizer import (  # noqa: E402
    demonstration_graph, find_alternative_route,
)

ALERT = {
    "alert_id": "H1", "hazard_type": "storm", "severity": 4,
    "valid_from": "2026-01-01T12:00:00Z", "valid_until": "2026-01-01T14:00:00Z",
    "geometry": {"type": "Polygon", "coordinates": [[
        [81.08, 12.98], [81.32, 12.98], [81.32, 13.22], [81.08, 13.22], [81.08, 12.98]]]},
}


def _position(vessel, lat=13.1, lon=81.2):
    return {"vessel_id": vessel, "latitude": lat, "longitude": lon,
            "event_time": "2026-01-01T12:00:00Z", "speed_knots": 12, "heading_deg": 90}


def test_vessels_layer_colors_alerted_red():
    layer = vessels_layer([_position("a"), _position("b", 13.5, 81.0)], ["a"])
    assert layer.type == "ScatterplotLayer" and len(layer.data) == 2
    by_id = {row["vessel_id"]: row for row in layer.data}
    assert by_id["a"]["color"][:3] == ALERT_RED and by_id["b"]["color"][:3] == INK
    assert by_id["b"]["position"] == [81.0, 13.5]
    assert len(vessels_layer([], []).data) == 0


def test_hazards_layer_builds_polygons():
    layer = hazards_layer([ALERT])
    assert layer.type == "PolygonLayer" and len(layer.data) == 1
    assert "storm" in layer.data[0]["tooltip"]


def test_hazard_polygon_uses_lon_lat_order():
    ring = hazards_layer([ALERT]).data[0]["polygon"]
    assert ring[0] == [81.08, 12.98]
    assert all(DEMO_BBOX["lon_min"] <= lon <= DEMO_BBOX["lon_max"] for lon, _ in ring)
    assert all(DEMO_BBOX["lat_min"] <= lat <= DEMO_BBOX["lat_max"] for _, lat in ring)


def test_risk_cells_layer_keeps_alerted_cells():
    events = [
        {"vessel_id": "a", "h3_index": "c1", "risk_alert": True,
         "matching_hazards": [{"severity": 4}]},
        {"vessel_id": "b", "h3_index": "c1", "risk_alert": True,
         "matching_hazards": [{"severity": 2}]},
        {"vessel_id": "c", "h3_index": "c2", "risk_alert": False, "matching_hazards": []},
    ]
    layer = risk_cells_layer(events)
    assert layer.type == "H3HexagonLayer" and len(layer.data) == 1
    assert layer.data[0]["events"] == 2 and layer.data[0]["severity"] == 4


def test_density_layer_extrudes_by_count():
    rows = [{"h3_index": "c1", "unique_vessel_count": 3, "window_start": "w"},
            {"h3_index": "c2", "unique_vessel_count": 1, "window_start": "w"}]
    layer = density_layer(rows)
    assert layer.type == "H3HexagonLayer" and len(layer.data) == 2
    assert layer.extruded is True
    assert [row["elevation"] for row in layer.data] == [600, 200]
    assert len(density_layer([]).data) == 0


def test_route_layer_draws_baseline_and_alternative():
    route = find_alternative_route(demonstration_graph(), "port_a", "port_b", 0.5)
    layer = route_layer(route, DEMO_NODES)
    assert layer.type == "PathLayer" and len(layer.data) == 2
    baseline, alternative = layer.data
    assert baseline["color"] != alternative["color"]
    lat, lon = DEMO_NODES["port_a"]
    assert baseline["path"][0] == [lon, lat]
    assert len(route_layer(None, DEMO_NODES).data) == 0


def test_node_layers_label_every_node():
    markers, labels = node_layers(DEMO_NODES)
    assert markers.type == "ScatterplotLayer" and labels.type == "TextLayer"
    assert len(labels.data) == len(DEMO_NODES)


def test_build_deck_centres_on_demo_bbox():
    deck = build_deck([hazards_layer([ALERT])])
    view = deck.initial_view_state
    assert DEMO_BBOX["lat_min"] < view.latitude < DEMO_BBOX["lat_max"]
    assert DEMO_BBOX["lon_min"] < view.longitude < DEMO_BBOX["lon_max"]
    assert deck._tooltip["html"] == "{tooltip}"
    assert deck.map_provider == "carto"
    assert len(deck.layers) == 1


def test_literal_props_are_not_serialized_as_accessors():
    import json

    route = find_alternative_route(demonstration_graph(), "port_a", "port_b", 0.5)
    deck = build_deck([vessels_layer([_position("a")], []), route_layer(route, DEMO_NODES),
                       *node_layers(DEMO_NODES)])
    layers = {layer["id"]: layer for layer in json.loads(deck.to_json())["layers"]}
    assert layers["vessels"]["radiusUnits"] == "pixels"
    assert layers["route"]["widthUnits"] == "pixels"
    assert layers["node_labels"]["getAlignmentBaseline"] == "bottom"
    assert not layers["node_labels"]["fontFamily"].startswith("@@=")

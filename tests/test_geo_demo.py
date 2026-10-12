import pytest

from georisk.geo_demo import DEMO_BBOX, haversine_nm, initial_bearing_deg
from georisk.ingestion.generator import generate_events


def test_haversine_one_degree_latitude_is_about_60nm():
    assert haversine_nm(0, 0, 1, 0) == pytest.approx(60.04, abs=0.1)


def test_bearing_due_east_is_90():
    assert initial_bearing_deg(0, 0, 0, 1) == pytest.approx(90)


def test_generated_points_inside_demo_bbox():
    events = generate_events(7, 20)
    points = [(event["latitude"], event["longitude"]) for event in events["telemetry"]]
    points += [(lat, lon) for alert in events["weather_alerts"]
               for lon, lat in alert["geometry"]["coordinates"][0]]
    for lat, lon in points:
        assert DEMO_BBOX["lat_min"] <= lat <= DEMO_BBOX["lat_max"]
        assert DEMO_BBOX["lon_min"] <= lon <= DEMO_BBOX["lon_max"]

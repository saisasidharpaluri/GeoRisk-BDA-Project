from datetime import datetime, timedelta, timezone
import json

import h3
import pytest

from georisk.contracts import ContractValidationError
from georisk.processing.h3_index import point_to_cell, polygon_to_cells
from georisk.processing.stream_job import process_events, run_streaming_job

UTC = timezone.utc


def _timestamp(minutes=0):
    return (datetime(2026, 1, 1, 12, 0, tzinfo=UTC) + timedelta(minutes=minutes)).isoformat().replace("+00:00", "Z")


def _telemetry(vessel_id, latitude, longitude, minutes=0):
    return {
        "schema_version": 1,
        "event_id": f"{vessel_id}-{minutes}",
        "event_time": _timestamp(minutes),
        "vessel_id": vessel_id,
        "latitude": latitude,
        "longitude": longitude,
        "speed_knots": 10,
        "heading_deg": 90,
    }


def _alert(center_latitude=12.0, center_longitude=78.0, until_minutes=30):
    size = 0.1
    return {
        "schema_version": 1,
        "event_id": "alert-event",
        "alert_id": "alert-1",
        "event_time": _timestamp(-1),
        "valid_from": _timestamp(-1),
        "valid_until": _timestamp(until_minutes),
        "hazard_type": "storm",
        "severity": 5,
        "geometry": {
            "type": "Polygon",
            "coordinates": [[
                [center_longitude - size, center_latitude - size],
                [center_longitude + size, center_latitude - size],
                [center_longitude + size, center_latitude + size],
                [center_longitude - size, center_latitude + size],
                [center_longitude - size, center_latitude - size],
            ]],
        },
    }


def test_point_index_is_valid_and_resolution_is_preserved():
    cell = point_to_cell(12, 78, 7)
    assert h3.is_valid_cell(cell)
    assert h3.get_resolution(cell) == 7


def test_h3_helpers_reject_invalid_inputs():
    with pytest.raises(ValueError):
        point_to_cell(91, 78, 7)
    with pytest.raises(ValueError):
        point_to_cell(12, 78, 16)
    with pytest.raises(ValueError):
        polygon_to_cells({"type": "Point", "coordinates": [78, 12]}, 7)


def test_polygon_and_multipolygon_coverage_include_expected_cells():
    polygon = _alert()["geometry"]
    point_cell = point_to_cell(12, 78, 7)
    assert point_cell in polygon_to_cells(polygon, 7)
    multipolygon = {
        "type": "MultiPolygon",
        "coordinates": [polygon["coordinates"], [
            [[80, 14], [80.1, 14], [80.1, 14.1], [80, 14.1], [80, 14]]
        ]],
    }
    assert point_cell in polygon_to_cells(multipolygon, 7)


def test_processing_matches_active_hazard_and_excludes_outside_event():
    results = process_events(
        [_telemetry("inside", 12, 78), _telemetry("outside", 14, 80)],
        [_alert()],
    )
    assert [event["risk_alert"] for event in results["risk_events"]] == [True, False]
    assert results["risk_events"][0]["matching_hazards"][0]["alert_id"] == "alert-1"


def test_expired_hazard_is_not_matched():
    results = process_events([_telemetry("vessel", 12, 78, 10)], [_alert(until_minutes=5)])
    assert results["risk_events"][0]["risk_alert"] is False
    assert results["risk_events"][0]["matching_hazards"] == []


def test_density_deduplicates_vessel_per_cell_and_window():
    telemetry = [
        _telemetry("vessel-1", 12, 78, 0),
        _telemetry("vessel-1", 12, 78, 5),
        _telemetry("vessel-2", 12, 78, 10),
    ]
    results = process_events(telemetry, [])
    assert len(results["density_metrics"]) == 1
    assert results["density_metrics"][0]["unique_vessel_count"] == 2
    assert results["density_metrics"][0]["window_start"] == "2026-01-01T12:00:00Z"


def test_processing_rejects_invalid_input():
    event = _telemetry("vessel", 12, 78)
    event["latitude"] = 100
    with pytest.raises(ContractValidationError):
        process_events([event], [])


def test_local_runner_reads_and_writes_jsonl(tmp_path):
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()
    (input_dir / "vessel-telemetry.jsonl").write_text(
        json.dumps(_telemetry("vessel", 12, 78)) + "\n", encoding="utf-8"
    )
    (input_dir / "weather-alerts.jsonl").write_text(
        json.dumps(_alert()) + "\n", encoding="utf-8"
    )
    result = run_streaming_job({"input_dir": input_dir, "output_dir": output_dir})
    assert result["risk_events"][0]["risk_alert"] is True
    assert (output_dir / "risk-events.jsonl").exists()
    assert (output_dir / "density-metrics.jsonl").exists()

from datetime import datetime, timedelta, timezone
import json

import h3
import pytest

import georisk.processing.stream_job as stream_job
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


def _alert(center_latitude=12.0, center_longitude=78.0, until_minutes=30, from_minutes=-1):
    size = 0.1
    return {
        "schema_version": 1,
        "event_id": "alert-event",
        "alert_id": "alert-1",
        "event_time": _timestamp(-1),
        "valid_from": _timestamp(from_minutes),
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


def test_duplicate_message_produces_one_risk_row():
    event = _telemetry("vessel", 12, 78)
    duplicate = dict(event, latitude=14, longitude=80)
    result = process_events([event, duplicate], [_alert()])
    assert len(result["risk_events"]) == 1
    assert result["risk_events"][0]["risk_alert"] is True
    assert result["stats"]["telemetry_in"] == 2
    assert result["stats"]["duplicates_dropped"] == 1
    assert result["quarantine"] == []
    assert len(result["density_metrics"]) == 1
    assert result["density_metrics"][0]["unique_vessel_count"] == 1


def test_risk_event_carries_event_id_and_coordinates():
    event = _telemetry("vessel", 12, 78)
    risk = process_events([event], [])["risk_events"][0]
    assert {key: risk[key] for key in ("event_id", "latitude", "longitude")} == {
        key: event[key] for key in ("event_id", "latitude", "longitude")
    }
    assert risk["coverage_mode"] == "center"


def test_invalid_record_does_not_reserve_event_id():
    valid = _telemetry("vessel", 12, 78)
    result = process_events([dict(valid, latitude=100), valid], [])
    assert len(result["risk_events"]) == 1
    assert result["stats"]["duplicates_dropped"] == 0
    assert result["stats"]["quarantined"] == 1


def test_processing_rejects_invalid_input():
    event = _telemetry("vessel", 12, 78)
    event["latitude"] = 100
    with pytest.raises(ContractValidationError):
        process_events([event], [], strict=True)


def test_invalid_telemetry_is_quarantined_and_rest_processed():
    invalid = _telemetry("bad", 100, 78)
    result = process_events([invalid, _telemetry("good", 12, 78)], [_alert()])
    assert [row["vessel_id"] for row in result["risk_events"]] == ["good"]
    assert result["density_metrics"][0]["unique_vessel_count"] == 1
    assert result["stats"]["telemetry_in"] == 2
    assert result["stats"]["alerts_in"] == 1
    assert result["stats"]["quarantined"] == 1
    entry = result["quarantine"][0]
    assert entry["contract"] == "vessel_telemetry"
    assert entry["reason"] == "contract_violation"
    assert entry["record"] == invalid
    assert entry["errors"] and "latitude" in entry["errors"][0]
    assert entry["quarantined_at"].endswith("Z")


@pytest.mark.parametrize("until_minutes", [-2, -1])
def test_invalid_validity_alert_is_quarantined(until_minutes):
    result = process_events([_telemetry("good", 12, 78)], [_alert(until_minutes=until_minutes)])
    assert not result["risk_events"][0]["risk_alert"]
    assert result["quarantine"][0]["reason"] == "invalid_validity_range"
    assert result["stats"]["quarantined"] == 1
    with pytest.raises(ValueError):
        process_events([], [_alert(until_minutes=until_minutes)], strict=True)


@pytest.mark.parametrize("invalid", [{}, None, []])
def test_invalid_alert_and_non_mapping_telemetry_are_quarantined(invalid):
    result = process_events([invalid, _telemetry("good", 12, 78)], [invalid, _alert()])
    assert result["risk_events"][0]["risk_alert"]
    assert result["stats"]["quarantined"] == 2
    assert all(entry["reason"] == "contract_violation" for entry in result["quarantine"])


@pytest.mark.parametrize("bad_line", ["{bad json}", "[]", "null", '"' + "x" * 1200 + '"'])
def test_runner_writes_quarantine_for_bad_json_line(tmp_path, bad_line):
    (tmp_path / "vessel-telemetry.jsonl").write_text(
        bad_line + "\n" + json.dumps(_telemetry("good", 12, 78)) + "\n", encoding="utf-8"
    )
    (tmp_path / "weather-alerts.jsonl").write_text("", encoding="utf-8")
    output = tmp_path / "processed"
    config = {"input_dir": tmp_path, "output_dir": output}
    result = run_streaming_job(config)
    assert len(result["risk_events"]) == 1
    assert result["stats"]["telemetry_in"] == 2
    assert result["stats"]["quarantined"] == 1
    entry = json.loads((output / "quarantine.jsonl").read_text(encoding="utf-8"))
    assert entry == result["quarantine"][0]
    assert entry["reason"] == "invalid_json"
    assert entry["record"] == {"line": 1, "raw": (bad_line + "\n")[:1000]}
    with pytest.raises(ValueError, match="invalid JSON"):
        run_streaming_job({**config, "strict": True})


def test_runner_always_writes_empty_quarantine(tmp_path):
    result = run_streaming_job({"telemetry": [], "output_dir": tmp_path})
    assert result["quarantine"] == []
    assert (tmp_path / "quarantine.jsonl").read_text(encoding="utf-8") == ""


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


def test_polygon_coverage_computed_once_per_alert(monkeypatch):
    calls = 0
    original_polygon_to_cells = stream_job.polygon_to_cells

    def counting_polygon_to_cells(geometry, resolution):
        nonlocal calls
        calls += 1
        return original_polygon_to_cells(geometry, resolution)

    monkeypatch.setattr(stream_job, "polygon_to_cells", counting_polygon_to_cells)
    second_alert = _alert(center_latitude=14, center_longitude=80)
    second_alert["alert_id"] = "alert-2"
    second_alert["event_id"] = "alert-event-2"
    telemetry = [_telemetry(f"vessel-{index}", 12, 78) for index in range(20)]
    results = process_events(telemetry, [_alert(), second_alert])

    assert calls == 2
    assert len(results["risk_events"]) == 20
    assert all(
        [hazard["alert_id"] for hazard in event["matching_hazards"]] == ["alert-1"]
        for event in results["risk_events"]
    )


def test_event_on_window_boundary_starts_new_window():
    results = process_events([_telemetry("vessel", 12, 78, minutes=15)], [])

    assert len(results["density_metrics"]) == 1
    metric = results["density_metrics"][0]
    assert metric["window_start"] == "2026-01-01T12:15:00Z"
    assert metric["window_end"] == "2026-01-01T12:30:00Z"
    assert metric["unique_vessel_count"] == 1


def test_alert_not_yet_active_does_not_match():
    results = process_events([_telemetry("vessel", 12, 78)], [_alert(from_minutes=5)])

    event = results["risk_events"][0]
    assert event["risk_alert"] is False
    assert event["matching_hazards"] == []


def test_validity_interval_is_half_open():
    results = process_events(
        [_telemetry("vessel", 12, 78, minutes=0), _telemetry("vessel", 12, 78, minutes=15)],
        [_alert(from_minutes=0, until_minutes=15)],
    )

    start_event, end_event = results["risk_events"]
    assert start_event["risk_alert"] is True
    assert [hazard["alert_id"] for hazard in start_event["matching_hazards"]] == ["alert-1"]
    assert end_event["risk_alert"] is False
    assert end_event["matching_hazards"] == []


def test_overlapping_alerts_both_reported_sorted():
    first_alert = _alert()
    second_alert = _alert()
    second_alert["alert_id"] = "alert-2"
    second_alert["event_id"] = "alert-event-2"
    results = process_events(
        [_telemetry("vessel", 12, 78)], [second_alert, first_alert]
    )

    event = results["risk_events"][0]
    assert event["risk_alert"] is True
    assert [hazard["alert_id"] for hazard in event["matching_hazards"]] == [
        "alert-1", "alert-2"
    ]


def test_multipolygon_alert_matches_second_polygon():
    alert = _alert()
    first_polygon = alert["geometry"]
    second_polygon = _alert(center_latitude=14, center_longitude=80)["geometry"]
    cell = point_to_cell(14, 80, 7)
    assert cell not in polygon_to_cells(first_polygon, 7)
    assert cell in polygon_to_cells(second_polygon, 7)
    alert["geometry"] = {
        "type": "MultiPolygon",
        "coordinates": [first_polygon["coordinates"], second_polygon["coordinates"]],
    }
    results = process_events([_telemetry("vessel", 14, 80)], [alert])

    event = results["risk_events"][0]
    assert event["risk_alert"] is True
    assert [hazard["alert_id"] for hazard in event["matching_hazards"]] == ["alert-1"]

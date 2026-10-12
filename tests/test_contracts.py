from datetime import datetime, timezone

import pytest

import georisk.contracts as contracts
from georisk.contracts import (
    ContractValidationError,
    is_valid_event,
    load_schema,
    validate_event,
)


def _timestamp() -> str:
    return datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc).isoformat()


def _telemetry() -> dict:
    return {
        "schema_version": 1,
        "event_id": "telemetry-1",
        "event_time": _timestamp(),
        "vessel_id": "vessel-1",
        "latitude": 12.5,
        "longitude": 78.25,
        "speed_knots": 14.2,
        "heading_deg": 90,
    }


def _alert() -> dict:
    return {
        "schema_version": 1,
        "event_id": "alert-event-1",
        "alert_id": "alert-1",
        "event_time": _timestamp(),
        "valid_from": _timestamp(),
        "valid_until": "2026-01-01T14:00:00+00:00",
        "hazard_type": "high_waves",
        "severity": 4,
        "geometry": {
            "type": "Polygon",
            "coordinates": [
                [[78.0, 12.0], [79.0, 12.0], [79.0, 13.0], [78.0, 12.0]]
            ],
        },
    }


def _risk_event() -> dict:
    return {
        "schema_version": 1,
        "event_time": _timestamp(),
        "vessel_id": "vessel-1",
        "h3_index": "87754e649ffffff",
        "h3_resolution": 7,
        "risk_alert": True,
        "matching_hazards": [{"alert_id": "alert-1", "severity": 4}],
    }


def _density_metric() -> dict:
    return {
        "schema_version": 1,
        "window_start": _timestamp(),
        "window_end": "2026-01-01T12:15:00+00:00",
        "h3_index": "87754e649ffffff",
        "h3_resolution": 7,
        "unique_vessel_count": 2,
    }


@pytest.mark.parametrize(
    ("contract", "event_factory"),
    [
        ("vessel_telemetry", _telemetry),
        ("weather_alert", _alert),
        ("risk_event", _risk_event),
        ("density_metric", _density_metric),
    ],
)
def test_valid_events_satisfy_contracts(contract, event_factory):
    validate_event(contract, event_factory())
    assert is_valid_event(contract, event_factory())


@pytest.mark.parametrize(
    ("contract", "event_factory", "field", "value"),
    [
        ("vessel_telemetry", _telemetry, "latitude", 91),
        ("vessel_telemetry", _telemetry, "event_time", "not-a-timestamp"),
        ("vessel_telemetry", _telemetry, "event_time", "2026-01-01T17:30:00+05:30"),
        ("weather_alert", _alert, "severity", 6),
        ("weather_alert", _alert, "geometry", {"type": "Point", "coordinates": []}),
        ("risk_event", _risk_event, "h3_resolution", 16),
        ("density_metric", _density_metric, "unique_vessel_count", -1),
    ],
)
def test_invalid_values_are_rejected(contract, event_factory, field, value):
    event = event_factory()
    event[field] = value

    with pytest.raises(ContractValidationError) as error:
        validate_event(contract, event)

    assert field in str(error.value)
    assert not is_valid_event(contract, event)


def test_unknown_fields_are_rejected():
    event = _telemetry()
    event["unexpected"] = True

    with pytest.raises(ContractValidationError):
        validate_event("vessel_telemetry", event)


def test_risk_event_accepts_new_optional_fields():
    event = dict(_risk_event(), event_id="telemetry-1", latitude=12.5,
                 longitude=78.25, coverage_mode="center")
    validate_event("risk_event", event)
    event["coverage_mode"] = "overlap"
    validate_event("risk_event", event)


def test_risk_event_rejects_unknown_coverage_mode():
    with pytest.raises(ContractValidationError, match="coverage_mode"):
        validate_event("risk_event", dict(_risk_event(), coverage_mode="unknown"))


def test_matching_hazard_requires_alert_id():
    event = _risk_event()
    del event["matching_hazards"][0]["alert_id"]
    with pytest.raises(ContractValidationError, match="alert_id"):
        validate_event("risk_event", event)


@pytest.mark.parametrize("hazard", [
    {"alert_id": "a"}, {"alert_id": "", "severity": 4},
    {"alert_id": "a", "severity": 0}, {"alert_id": "a", "severity": 6},
    {"alert_id": "a", "severity": 4, "unknown": 1},
    {"alert_id": "a", "severity": 4, "valid_from": "bad"},
])
def test_matching_hazard_fields_are_validated(hazard):
    with pytest.raises(ContractValidationError):
        validate_event("risk_event", dict(_risk_event(), matching_hazards=[hazard]))


@pytest.mark.parametrize("position", [[500, 500], [181, 12], [78, 91], [-181, 12], [78, -91]])
@pytest.mark.parametrize("multipolygon", [False, True])
def test_alert_rejects_out_of_range_coordinates(position, multipolygon):
    event = _alert()
    event["geometry"]["coordinates"][0][1] = position
    if multipolygon:
        event["geometry"] = {"type": "MultiPolygon", "coordinates": [event["geometry"]["coordinates"]]}
    with pytest.raises(ContractValidationError, match="geometry"):
        validate_event("weather_alert", event)


@pytest.mark.parametrize("multipolygon", [False, True])
def test_alert_rejects_unclosed_ring(multipolygon):
    event = _alert()
    ring = [[78.2, 12.2], [78.4, 12.2], [78.4, 12.4], [78.3, 12.3]]
    event["geometry"]["coordinates"].append(ring)
    path = "geometry.coordinates[1]"
    if multipolygon:
        event["geometry"] = {"type": "MultiPolygon", "coordinates": [event["geometry"]["coordinates"]]}
        path = "geometry.coordinates[0][1]"
    with pytest.raises(ContractValidationError) as error:
        validate_event("weather_alert", event)
    assert error.value.errors == [f"{path}: ring is not closed"]


@pytest.mark.parametrize("valid_until", ["2026-01-01T11:00:00Z", "2026-01-01T12:00:00Z"])
def test_alert_rejects_inverted_validity(valid_until):
    with pytest.raises(ContractValidationError) as error:
        validate_event("weather_alert", dict(_alert(), valid_until=valid_until))
    assert error.value.errors == ["valid_until: must be after valid_from"]


def test_semantic_checks_wait_for_valid_structure(monkeypatch):
    def unexpected(*args):
        pytest.fail("semantic checks ran on malformed input")

    monkeypatch.setattr(contracts, "_semantic_errors", unexpected)
    with pytest.raises(ContractValidationError):
        validate_event("weather_alert", dict(_alert(), geometry={}))


@pytest.mark.parametrize("field", ["speed_knots", "heading_deg"])
def test_telemetry_requires_speed_and_heading(field):
    event = _telemetry()
    del event[field]
    with pytest.raises(ContractValidationError, match=field):
        validate_event("vessel_telemetry", event)


def test_telemetry_rejects_implausible_speed():
    with pytest.raises(ContractValidationError, match="speed_knots"):
        validate_event("vessel_telemetry", dict(_telemetry(), speed_knots=80))


@pytest.mark.parametrize("speed", [0, 60])
def test_telemetry_accepts_speed_boundaries(speed):
    validate_event("vessel_telemetry", dict(_telemetry(), speed_knots=speed))


def test_missing_required_fields_are_rejected():
    event = _alert()
    del event["valid_until"]

    with pytest.raises(ContractValidationError) as error:
        validate_event("weather_alert", event)

    assert "valid_until" in str(error.value)


def test_invalid_event_type_is_rejected():
    with pytest.raises(TypeError, match="mapping"):
        validate_event("vessel_telemetry", [])


def test_unknown_contract_is_rejected():
    with pytest.raises(ValueError, match="Unsupported contract"):
        load_schema("unknown")


def test_validator_is_built_once_per_contract(monkeypatch):
    contracts._validator.cache_clear()
    calls = 0
    original_load_schema = contracts.load_schema

    def counting_load_schema(contract):
        nonlocal calls
        calls += 1
        return original_load_schema(contract)

    monkeypatch.setattr(contracts, "load_schema", counting_load_schema)
    try:
        for _ in range(3):
            validate_event("vessel_telemetry", _telemetry())
        assert calls == 1
    finally:
        contracts._validator.cache_clear()

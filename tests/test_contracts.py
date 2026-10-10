from datetime import datetime, timezone

import pytest

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

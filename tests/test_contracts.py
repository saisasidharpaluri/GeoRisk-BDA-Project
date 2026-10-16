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
            "coordinates": [[[78.0, 12.0], [79.0, 12.0], [79.0, 13.0], [78.0, 12.0]]],
        },
    }


def _risk_event() -> dict:
    return {
        "schema_version": 1,
        "event_id": "telemetry-1",
        "event_time": _timestamp(),
        "vessel_id": "vessel-1",
        "latitude": 12.5,
        "longitude": 78.25,
        "coverage_mode": "center",
        "h3_index": "87754e649ffffff",
        "h3_resolution": 7,
        "risk_alert": True,
        "matching_hazards": [
            {
                "alert_id": "alert-1",
                "hazard_type": "storm",
                "severity": 4,
                "valid_from": "2026-01-01T11:00:00Z",
                "valid_until": "2026-01-01T13:00:00Z",
            }
        ],
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


def test_risk_event_accepts_both_coverage_modes():
    event = _risk_event()
    validate_event("risk_event", event)
    event["coverage_mode"] = "overlap"
    validate_event("risk_event", event)


@pytest.mark.parametrize("field", ["event_id", "latitude", "longitude", "coverage_mode"])
def test_risk_event_requires_identity_and_location(field):
    event = _risk_event()
    del event[field]
    with pytest.raises(ContractValidationError, match=field):
        validate_event("risk_event", event)


@pytest.mark.parametrize("field", ["hazard_type", "valid_from", "valid_until"])
def test_matching_hazard_requires_provenance(field):
    event = _risk_event()
    del event["matching_hazards"][0][field]
    with pytest.raises(ContractValidationError, match=field):
        validate_event("risk_event", event)


@pytest.mark.parametrize(
    ("contract", "factory", "field"),
    [
        ("vessel_telemetry", _telemetry, "latitude"),
        ("vessel_telemetry", _telemetry, "speed_knots"),
        ("vessel_telemetry", _telemetry, "heading_deg"),
        ("risk_event", _risk_event, "longitude"),
    ],
)
@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_non_finite_numbers_are_rejected(contract, factory, field, value):
    event = dict(factory(), **{field: value})
    with pytest.raises(ContractValidationError, match=f"{field}: must be a finite number"):
        validate_event(contract, event)


@pytest.mark.parametrize(
    "value",
    [
        "2026-01-01T12:00:00Z",
        "2026-01-01T12:00:00.1Z",
        "2026-01-01T12:00:00.123456789Z",
        "2026-01-01T12:00:00+00:00",
        "2026-01-01T12:00:00-00:00",
    ],
)
def test_rfc3339_utc_timestamps_are_accepted(value):
    validate_event("vessel_telemetry", dict(_telemetry(), event_time=value))
    assert contracts.parse_utc_timestamp(value).tzinfo == timezone.utc


@pytest.mark.parametrize(
    "value",
    [
        "2026-01-01 12:00:00Z",
        "20260101T120000Z",
        "2026-W01-4T12:00:00Z",
        "2026-01-01T12Z",
        "2026-01-01T12:00:00",
        "2026-01-01T12:00:00+00:00:00",
        "2026-02-30T12:00:00Z",
        "2026-01-01T12:00:00+01:00",
    ],
)
def test_non_rfc3339_timestamps_are_rejected(value):
    with pytest.raises(ContractValidationError, match="event_time"):
        validate_event("vessel_telemetry", dict(_telemetry(), event_time=value))


def test_fractional_seconds_are_truncated_to_microseconds():
    parsed = contracts.parse_utc_timestamp("2026-01-01T12:00:00.1234567Z")
    assert parsed.microsecond == 123456


@pytest.mark.parametrize(
    ("ring", "message"),
    [
        ([[1, 1], [1, 1], [1, 1], [1, 1]], "three distinct positions"),
        ([[0, 0], [1, 1], [2, 2], [0, 0]], "zero area"),
        ([[179, 0], [-179, 0], [-179, 1], [179, 0]], "antimeridian"),
    ],
)
def test_degenerate_or_wrapping_rings_are_rejected(ring, message):
    alert = _alert()
    alert["geometry"] = {"type": "Polygon", "coordinates": [ring]}
    with pytest.raises(ContractValidationError, match=message):
        validate_event("weather_alert", alert)


def test_risk_alert_flag_must_match_hazards():
    with pytest.raises(ContractValidationError, match="risk_alert"):
        validate_event("risk_event", dict(_risk_event(), risk_alert=False))
    with pytest.raises(ContractValidationError, match="risk_alert"):
        validate_event("risk_event", dict(_risk_event(), matching_hazards=[]))


@pytest.mark.parametrize("contract", ["risk_event", "density_metric"])
def test_h3_index_must_be_valid_and_match_resolution(contract):
    factory = _risk_event if contract == "risk_event" else _density_metric
    with pytest.raises(ContractValidationError, match="valid H3 cell"):
        validate_event(contract, dict(factory(), h3_index="not-h3"))
    with pytest.raises(ContractValidationError, match="resolution does not match"):
        validate_event(contract, dict(factory(), h3_resolution=8))


def test_density_window_must_be_ordered():
    metric = dict(_density_metric(), window_end=_density_metric()["window_start"])
    with pytest.raises(ContractValidationError, match="window_end"):
        validate_event("density_metric", metric)


def test_risk_event_rejects_unknown_coverage_mode():
    with pytest.raises(ContractValidationError, match="coverage_mode"):
        validate_event("risk_event", dict(_risk_event(), coverage_mode="unknown"))


def test_matching_hazard_requires_alert_id():
    event = _risk_event()
    del event["matching_hazards"][0]["alert_id"]
    with pytest.raises(ContractValidationError, match="alert_id"):
        validate_event("risk_event", event)


@pytest.mark.parametrize(
    "hazard",
    [
        {"alert_id": "a"},
        {"alert_id": "", "severity": 4},
        {"alert_id": "a", "severity": 0},
        {"alert_id": "a", "severity": 6},
        {"alert_id": "a", "severity": 4, "unknown": 1},
        {"alert_id": "a", "severity": 4, "valid_from": "bad"},
    ],
)
def test_matching_hazard_fields_are_validated(hazard):
    with pytest.raises(ContractValidationError):
        validate_event("risk_event", dict(_risk_event(), matching_hazards=[hazard]))


@pytest.mark.parametrize("position", [[500, 500], [181, 12], [78, 91], [-181, 12], [78, -91]])
@pytest.mark.parametrize("multipolygon", [False, True])
def test_alert_rejects_out_of_range_coordinates(position, multipolygon):
    event = _alert()
    event["geometry"]["coordinates"][0][1] = position
    if multipolygon:
        event["geometry"] = {
            "type": "MultiPolygon",
            "coordinates": [event["geometry"]["coordinates"]],
        }
    with pytest.raises(ContractValidationError, match="geometry"):
        validate_event("weather_alert", event)


@pytest.mark.parametrize("multipolygon", [False, True])
def test_alert_rejects_unclosed_ring(multipolygon):
    event = _alert()
    ring = [[78.2, 12.2], [78.4, 12.2], [78.4, 12.4], [78.3, 12.3]]
    event["geometry"]["coordinates"].append(ring)
    path = "geometry.coordinates[1]"
    if multipolygon:
        event["geometry"] = {
            "type": "MultiPolygon",
            "coordinates": [event["geometry"]["coordinates"]],
        }
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

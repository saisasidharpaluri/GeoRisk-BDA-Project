"""Validation helpers for the versioned GeoRisk event contracts."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from jsonschema import Draft202012Validator, FormatChecker
from jsonschema.exceptions import SchemaError, ValidationError

SCHEMA_VERSION = 1
SUPPORTED_CONTRACTS = frozenset(
    {"vessel_telemetry", "weather_alert", "risk_event", "density_metric"}
)
FORMAT_CHECKER = FormatChecker()


@FORMAT_CHECKER.checks("date-time", raises=(TypeError, ValueError))
def _is_utc_datetime(value: object) -> bool:
    """Require an ISO-8601 timestamp with an explicit UTC offset."""
    if not isinstance(value, str):
        return False
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed.tzinfo is not None and parsed.utcoffset() == timezone.utc.utcoffset(
        parsed
    )


class ContractValidationError(ValueError):
    """Raised when an event does not satisfy its versioned contract."""

    def __init__(self, contract: str, errors: list[str]) -> None:
        self.contract = contract
        self.errors = errors
        details = "; ".join(errors)
        super().__init__(f"{contract} contract validation failed: {details}")


def _schema_path(contract: str) -> Path:
    if contract not in SUPPORTED_CONTRACTS:
        supported = ", ".join(sorted(SUPPORTED_CONTRACTS))
        raise ValueError(f"Unsupported contract '{contract}'. Expected one of: {supported}")
    return (
        Path(__file__).resolve().parents[2]
        / "schemas"
        / "v1"
        / f"{contract}.schema.json"
    )


def load_schema(contract: str) -> dict[str, Any]:
    """Load and validate one checked-in JSON Schema."""
    path = _schema_path(contract)
    try:
        with path.open(encoding="utf-8") as schema_file:
            schema = json.load(schema_file)
        Draft202012Validator.check_schema(schema)
    except (OSError, json.JSONDecodeError, SchemaError) as exc:
        raise RuntimeError(f"Unable to load schema '{path}': {exc}") from exc
    return schema


def validate_event(contract: str, event: Mapping[str, Any]) -> None:
    """Validate an event and raise one error containing all schema violations."""
    if not isinstance(event, Mapping):
        raise TypeError("event must be a mapping")

    validator = Draft202012Validator(
        load_schema(contract), format_checker=FORMAT_CHECKER
    )
    errors = sorted(validator.iter_errors(dict(event)), key=lambda error: list(error.path))
    if errors:
        messages = [
            f"{'.'.join(map(str, error.path)) or '<root>'}: {error.message}"
            for error in errors
        ]
        raise ContractValidationError(contract, messages)


def is_valid_event(contract: str, event: Mapping[str, Any]) -> bool:
    """Return whether an event satisfies its contract."""
    try:
        validate_event(contract, event)
    except (ContractValidationError, TypeError):
        return False
    return True

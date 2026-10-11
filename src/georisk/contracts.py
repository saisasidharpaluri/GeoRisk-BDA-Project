"""Validation helpers for the versioned GeoRisk event contracts."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from functools import lru_cache
from importlib.resources import files
from typing import TYPE_CHECKING, Any, Mapping

from jsonschema import Draft202012Validator, FormatChecker
from jsonschema.exceptions import SchemaError, ValidationError

if TYPE_CHECKING:
    from importlib.abc import Traversable

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


def _schema_path(contract: str) -> Traversable:
    if contract not in SUPPORTED_CONTRACTS:
        supported = ", ".join(sorted(SUPPORTED_CONTRACTS))
        raise ValueError(f"Unsupported contract '{contract}'. Expected one of: {supported}")
    return (
        files("georisk")
        / "schemas"
        / "v1"
        / f"{contract}.schema.json"
    )


def load_schema(contract: str) -> dict[str, Any]:
    """Load and validate one checked-in JSON Schema."""
    path = _schema_path(contract)
    try:
        schema = json.loads(path.read_text(encoding="utf-8"))
        Draft202012Validator.check_schema(schema)
    except (OSError, json.JSONDecodeError, SchemaError) as exc:
        raise RuntimeError(f"Unable to load schema '{path}': {exc}") from exc
    return schema


@lru_cache(maxsize=None)
def _validator(contract: str) -> Draft202012Validator:
    """Build one reusable validator per contract."""
    return Draft202012Validator(load_schema(contract), format_checker=FORMAT_CHECKER)


def validate_event(contract: str, event: Mapping[str, Any]) -> None:
    """Validate an event and raise one error containing all schema violations."""
    if not isinstance(event, Mapping):
        raise TypeError("event must be a mapping")

    validator = _validator(contract)
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

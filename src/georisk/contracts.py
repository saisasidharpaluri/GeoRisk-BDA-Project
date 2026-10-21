"""Validation helpers for the versioned GeoRisk event contracts."""

from __future__ import annotations

import json
import math
import re
from collections.abc import Iterator, Mapping
from datetime import datetime, timezone
from functools import cache, lru_cache
from importlib.resources import files
from typing import TYPE_CHECKING, Any

import h3
from jsonschema import Draft202012Validator, FormatChecker
from jsonschema.exceptions import SchemaError

if TYPE_CHECKING:
    from importlib.abc import Traversable

SCHEMA_VERSION = 1
SUPPORTED_CONTRACTS = frozenset(
    {"vessel_telemetry", "weather_alert", "risk_event", "density_metric", "delay_estimate"}
)
FORMAT_CHECKER = FormatChecker()

# RFC 3339 profile shared by every contract: uppercase ``T``, optional fraction,
# and a UTC designator (``Z``, ``+00:00`` or ``-00:00``). Parsing is independent
# of the Python version, unlike ``datetime.fromisoformat``.
_UTC_TIMESTAMP = re.compile(
    r"^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?(Z|[+-]00:00)$"
)


def parse_utc_timestamp(value: object) -> datetime:
    """Parse a strict RFC 3339 UTC timestamp into an aware ``datetime``.

    Fractions beyond microseconds are truncated. Raises ``ValueError`` for any
    other format, a non-UTC offset, or an impossible calendar value.
    """
    if not isinstance(value, str):
        raise ValueError("timestamp must be a string")
    return _parse_utc_string(value)


@lru_cache(maxsize=65536)
def _parse_utc_string(value: str) -> datetime:
    """Parse one timestamp string; cached because streams repeat the same instants."""
    match = _UTC_TIMESTAMP.fullmatch(value)
    if match is None:
        raise ValueError(f"timestamp must be RFC 3339 UTC (YYYY-MM-DDTHH:MM:SS[.f]Z): {value!r}")
    year, month, day, hour, minute, second, fraction, _ = match.groups()
    microsecond = int((fraction or "0").ljust(6, "0")[:6])
    try:
        return datetime(
            int(year),
            int(month),
            int(day),
            int(hour),
            int(minute),
            int(second),
            microsecond,
            tzinfo=timezone.utc,
        )
    except ValueError as exc:
        raise ValueError(f"invalid timestamp: {value!r}") from exc


@FORMAT_CHECKER.checks("date-time", raises=(TypeError, ValueError))
def _is_utc_datetime(value: object) -> bool:
    """Require a strict RFC 3339 timestamp with a UTC offset."""
    if not isinstance(value, str):
        # JSON Schema format assertions apply only to strings; the type keyword
        # reports non-string values.
        return True
    parse_utc_timestamp(value)
    return True


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
    return files("georisk") / "schemas" / "v1" / f"{contract}.schema.json"


def load_schema(contract: str) -> dict[str, Any]:
    """Load and validate one checked-in JSON Schema."""
    path = _schema_path(contract)
    try:
        schema = json.loads(path.read_text(encoding="utf-8"))
        Draft202012Validator.check_schema(schema)
    except (OSError, json.JSONDecodeError, SchemaError) as exc:
        raise RuntimeError(f"Unable to load schema '{path}': {exc}") from exc
    return schema


@cache
def _validator(contract: str) -> Draft202012Validator:
    """Build one reusable validator per contract."""
    return Draft202012Validator(load_schema(contract), format_checker=FORMAT_CHECKER)


def _non_finite_paths(value: Any, path: str = "") -> Iterator[str]:
    """Yield dotted paths of NaN or infinite floats, which JSON cannot represent."""
    if isinstance(value, float) and not math.isfinite(value):
        yield path or "<root>"
    elif isinstance(value, Mapping):
        for key, item in value.items():
            yield from _non_finite_paths(item, f"{path}.{key}" if path else str(key))
    elif isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            yield from _non_finite_paths(item, f"{path}.{index}" if path else str(index))


def _ring_errors(ring: list[list[float]], label: str) -> list[str]:
    """Check closure, non-zero area, and antimeridian crossing for one linear ring."""
    errors = []
    if ring[0] != ring[-1]:
        errors.append(f"{label}: ring is not closed")
    if len({(position[0], position[1]) for position in ring}) < 3:
        errors.append(f"{label}: ring needs at least three distinct positions")
    else:
        twice_area = sum(
            a[0] * b[1] - b[0] * a[1] for a, b in zip(ring, ring[1:] + ring[:1], strict=True)
        )
        if twice_area == 0:
            errors.append(f"{label}: ring has zero area")
    if any(abs(b[0] - a[0]) > 180 for a, b in zip(ring, ring[1:], strict=False)):
        errors.append(
            f"{label}: ring crosses the antimeridian; split it into a MultiPolygon instead"
        )
    return errors


def _semantic_errors(contract: str, event: Mapping[str, Any]) -> list[str]:
    """Check relationships after the event's JSON structure has been validated."""
    errors = []
    if contract == "weather_alert":
        geometry = event["geometry"]
        for index, coordinates in enumerate(geometry["coordinates"]):
            if geometry["type"] == "Polygon":
                errors.extend(_ring_errors(coordinates, f"geometry.coordinates[{index}]"))
            else:
                for ring_index, ring in enumerate(coordinates):
                    errors.extend(
                        _ring_errors(ring, f"geometry.coordinates[{index}][{ring_index}]")
                    )
        if parse_utc_timestamp(event["valid_until"]) <= parse_utc_timestamp(event["valid_from"]):
            errors.append("valid_until: must be after valid_from")
    elif contract == "risk_event":
        if event["risk_alert"] != bool(event["matching_hazards"]):
            errors.append("risk_alert: must be true exactly when matching_hazards is non-empty")
        errors.extend(_h3_errors(event["h3_index"], event["h3_resolution"]))
        for index, match in enumerate(event["matching_hazards"]):
            if parse_utc_timestamp(match["valid_until"]) <= parse_utc_timestamp(
                match["valid_from"]
            ):
                errors.append(f"matching_hazards.{index}.valid_until: must be after valid_from")
    elif contract == "density_metric":
        if parse_utc_timestamp(event["window_end"]) <= parse_utc_timestamp(event["window_start"]):
            errors.append("window_end: must be after window_start")
        errors.extend(_h3_errors(event["h3_index"], event["h3_resolution"]))
    return errors


def _h3_errors(cell: str, resolution: int) -> list[str]:
    if not h3.is_valid_cell(cell):
        return ["h3_index: must be a valid H3 cell"]
    if h3.get_resolution(cell) != resolution:
        return ["h3_index: resolution does not match h3_resolution"]
    return []


def validate_event(contract: str, event: Mapping[str, Any]) -> None:
    """Validate structure and semantics, raising detailed contract violations."""
    if not isinstance(event, Mapping):
        raise TypeError("event must be a mapping")

    validator = _validator(contract)
    errors = sorted(validator.iter_errors(dict(event)), key=lambda error: list(error.path))
    messages = [
        f"{'.'.join(map(str, error.path)) or '<root>'}: {error.message}" for error in errors
    ]
    messages.extend(f"{path}: must be a finite number" for path in _non_finite_paths(event))
    if messages:
        raise ContractValidationError(contract, messages)
    semantic_errors = _semantic_errors(contract, event)
    if semantic_errors:
        raise ContractValidationError(contract, semantic_errors)


def is_valid_event(contract: str, event: Mapping[str, Any]) -> bool:
    """Return whether an event satisfies its contract."""
    try:
        validate_event(contract, event)
    except (ContractValidationError, TypeError):
        return False
    return True

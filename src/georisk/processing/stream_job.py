"""Local event-time spatial processing core for the Phase 3 pipeline."""

from __future__ import annotations

import json
import logging
import math
import os
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from georisk.contracts import (
    SCHEMA_VERSION,
    ContractValidationError,
    parse_utc_timestamp,
    validate_event,
)
from georisk.processing.h3_index import (
    COVERAGE_MODES,
    DEFAULT_COVERAGE_MODE,
    DEFAULT_RESOLUTION,
    _validate_resolution,
    point_to_cell,
    polygon_to_cells,
)

logger = logging.getLogger(__name__)

UTC = timezone.utc
WINDOW_MINUTES = 15
MAX_FUTURE_MINUTES = 60


def _parse_utc(value: str) -> datetime:
    return parse_utc_timestamp(value)


def _timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _window_start(value: datetime, minutes: int) -> datetime:
    seconds = int(value.timestamp())
    return datetime.fromtimestamp(seconds - seconds % (minutes * 60), tz=UTC)


@dataclass(frozen=True)
class AlertRevision:
    """One accepted alert revision and the event-time interval in which it applies.

    A revision applies from its own ``event_time`` until the next revision of
    the same alert is issued. The first revision also applies to earlier
    telemetry, so a single-revision alert behaves exactly like its validity
    interval. ``None`` bounds are open.
    """

    alert: dict[str, Any]
    valid_from: datetime
    valid_until: datetime
    effective_from: datetime | None
    effective_until: datetime | None

    def applies_at(self, event_time: datetime) -> bool:
        return (
            self.valid_from <= event_time < self.valid_until
            and (self.effective_from is None or self.effective_from <= event_time)
            and (self.effective_until is None or event_time < self.effective_until)
        )


def alert_revisions(alerts: Iterable[Mapping[str, Any]]) -> list[AlertRevision]:
    """Order validated alerts into as-of revision intervals per ``alert_id``.

    Revisions sort by ``event_time`` and then input order; at equal issue times
    the later input wins and the earlier one never applies.
    """
    grouped: dict[str, list[tuple[datetime, int, dict[str, Any]]]] = defaultdict(list)
    for order, alert in enumerate(alerts):
        record = alert if isinstance(alert, dict) else dict(alert)
        grouped[alert["alert_id"]].append((_parse_utc(alert["event_time"]), order, record))
    revisions = []
    for alert_id in sorted(grouped):
        ordered = sorted(grouped[alert_id], key=lambda item: (item[0], item[1]))
        collapsed: list[tuple[datetime, dict[str, Any]]] = []
        for issued, _, alert in ordered:
            if collapsed and collapsed[-1][0] == issued:
                collapsed[-1] = (issued, alert)
            else:
                collapsed.append((issued, alert))
        for index, (issued, alert) in enumerate(collapsed):
            revisions.append(
                AlertRevision(
                    alert=alert,
                    valid_from=_parse_utc(alert["valid_from"]),
                    valid_until=_parse_utc(alert["valid_until"]),
                    effective_from=None if index == 0 else issued,
                    effective_until=(
                        collapsed[index + 1][0] if index + 1 < len(collapsed) else None
                    ),
                )
            )
    return revisions


def _matching_hazards(
    cell: str,
    event_time: datetime,
    coverage: dict[str, list[AlertRevision]],
) -> list[dict[str, Any]]:
    matches = []
    for revision in coverage.get(cell, []):
        alert = revision.alert
        if revision.applies_at(event_time):
            matches.append(
                {
                    "alert_id": alert["alert_id"],
                    "hazard_type": alert["hazard_type"],
                    "severity": alert["severity"],
                    "valid_from": alert["valid_from"],
                    "valid_until": alert["valid_until"],
                }
            )
    return sorted(matches, key=lambda match: match["alert_id"])


class StreamProcessor:
    """Incremental event-time processor; ``process_events`` runs one as a single batch.

    Alerts and telemetry may arrive in micro-batches. Alerts must be added
    before the telemetry they apply to: each ``add_alerts`` call rebuilds the
    as-of revision coverage from every alert accepted so far, so telemetry is
    matched exactly as in one batch over the same events in the same order.
    Deduplication, the watermark, and density state persist across calls, and
    ``result`` returns everything accumulated so far.

    Telemetry stamped more than ``max_future_minutes`` after ``now`` (default:
    the current UTC time) is quarantined as ``future_event`` so that a single
    bad clock cannot advance the watermark and mark later valid data as late.
    """

    def __init__(
        self,
        *,
        resolution: int = DEFAULT_RESOLUTION,
        strict: bool = False,
        coverage_mode: str = DEFAULT_COVERAGE_MODE,
        window_minutes: int = WINDOW_MINUTES,
        allowed_lateness_minutes: int = 10,
        max_future_minutes: int = MAX_FUTURE_MINUTES,
        now: datetime | None = None,
    ) -> None:
        if not isinstance(strict, bool):
            raise TypeError("strict must be a boolean")
        if (
            not isinstance(max_future_minutes, int)
            or isinstance(max_future_minutes, bool)
            or max_future_minutes < 0
        ):
            raise ValueError("max_future_minutes must be a non-negative integer")
        reference_time = datetime.now(UTC) if now is None else now
        if reference_time.tzinfo is None or reference_time.utcoffset() != timedelta(0):
            raise ValueError("now must be timezone-aware UTC")
        if (
            not isinstance(window_minutes, int)
            or isinstance(window_minutes, bool)
            or not 1 <= window_minutes <= 1440
            or 1440 % window_minutes != 0
        ):
            raise ValueError("window_minutes must be 1..1440 and divide 1440")
        if (
            not isinstance(allowed_lateness_minutes, int)
            or isinstance(allowed_lateness_minutes, bool)
            or not 0 <= allowed_lateness_minutes <= 1440
        ):
            raise ValueError("allowed_lateness_minutes must be an integer from 0 to 1440")
        _validate_resolution(resolution)
        if coverage_mode not in COVERAGE_MODES:
            raise ValueError("coverage_mode must be center or overlap")
        self.settings = {
            "h3_resolution": resolution,
            "coverage_mode": coverage_mode,
            "window_minutes": window_minutes,
            "allowed_lateness_minutes": allowed_lateness_minutes,
        }
        self._strict = strict
        self._resolution = resolution
        self._coverage_mode = coverage_mode
        self._window_minutes = window_minutes
        self._allowed_lateness = timedelta(minutes=allowed_lateness_minutes)
        self._future_limit = reference_time + timedelta(minutes=max_future_minutes)
        self.quarantine: list[dict[str, Any]] = []
        self.stats: dict[str, Any] = {
            "telemetry_in": 0,
            "alerts_in": 0,
            "quarantined": 0,
            "duplicates_dropped": 0,
            "late_dropped": 0,
            "future_dropped": 0,
            "alert_revisions_superseded": 0,
        }
        self._covered_alerts: list[tuple[dict[str, Any], set[str]]] = []
        self._coverage: dict[str, list[AlertRevision]] = {}
        self._seen_event_ids: set[str] = set()
        self._max_event_time: datetime | None = None
        self._risk_events: list[dict[str, Any]] = []
        self._density: defaultdict[tuple[datetime, datetime, str], set[str]] = defaultdict(set)

    def add_alerts(self, alerts: Iterable[Mapping[str, Any]]) -> None:
        """Validate alerts, quarantine rejects, and rebuild hazard coverage."""
        accepted = []
        for alert in alerts:
            self.stats["alerts_in"] += 1
            try:
                validate_event("weather_alert", alert)
            except (ContractValidationError, TypeError) as exc:
                if self._strict:
                    raise
                reason = (
                    "invalid_validity_range"
                    if isinstance(exc, ContractValidationError)
                    and "valid_until: must be after valid_from" in exc.errors
                    else "contract_violation"
                )
                self.quarantine.append(_quarantine_entry("weather_alert", reason, alert, exc))
                continue
            accepted.append(dict(alert))
        # Geometry is checked before revision ordering so a rejected revision does
        # not leave an interval in which no version of the alert applies.
        for alert in accepted:
            try:
                cells = polygon_to_cells(alert["geometry"], self._resolution, self._coverage_mode)
            except ValueError as exc:
                if self._strict:
                    raise
                self.quarantine.append(
                    _quarantine_entry("weather_alert", "invalid_geometry", alert, exc)
                )
                continue
            self._covered_alerts.append((alert, cells))
        cells_by_alert = {id(alert): cells for alert, cells in self._covered_alerts}
        coverage: dict[str, list[AlertRevision]] = {}
        for revision in alert_revisions(alert for alert, _ in self._covered_alerts):
            for cell in cells_by_alert[id(revision.alert)]:
                coverage.setdefault(cell, []).append(revision)
        self._coverage = coverage
        self.stats["alert_revisions_superseded"] = len(self._covered_alerts) - len(
            {alert["alert_id"] for alert, _ in self._covered_alerts}
        )

    def add_telemetry(self, telemetry: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
        """Match telemetry against the current coverage; return the new risk events."""
        new_events = []
        for event in telemetry:
            self.stats["telemetry_in"] += 1
            try:
                validate_event("vessel_telemetry", event)
            except (ContractValidationError, TypeError) as exc:
                if self._strict:
                    raise
                self.quarantine.append(
                    _quarantine_entry("vessel_telemetry", "contract_violation", event, exc)
                )
                continue
            if event["event_id"] in self._seen_event_ids:
                self.stats["duplicates_dropped"] += 1
                continue
            self._seen_event_ids.add(event["event_id"])
            event_time = _parse_utc(event["event_time"])
            if event_time > self._future_limit:
                self.stats["future_dropped"] += 1
                self.quarantine.append(
                    _quarantine_entry(
                        "vessel_telemetry",
                        "future_event",
                        event,
                        ValueError(
                            "event_time is later than the allowed limit "
                            f"{_timestamp(self._future_limit)}"
                        ),
                    )
                )
                continue
            latest = self._max_event_time
            if latest is not None and event_time < latest - self._allowed_lateness:
                self.stats["late_dropped"] += 1
                self.quarantine.append(
                    _quarantine_entry(
                        "vessel_telemetry",
                        "late_event",
                        event,
                        ValueError(
                            "event_time precedes watermark "
                            f"{_timestamp(latest - self._allowed_lateness)}"
                        ),
                    )
                )
                continue
            self._max_event_time = max(latest, event_time) if latest else event_time
            cell = point_to_cell(event["latitude"], event["longitude"], self._resolution)
            matches = _matching_hazards(cell, event_time, self._coverage)
            risk_event = {
                "schema_version": SCHEMA_VERSION,
                "event_id": event["event_id"],
                "latitude": event["latitude"],
                "longitude": event["longitude"],
                "coverage_mode": self._coverage_mode,
                "event_time": event["event_time"],
                "vessel_id": event["vessel_id"],
                "h3_index": cell,
                "h3_resolution": self._resolution,
                "risk_alert": bool(matches),
                "matching_hazards": matches,
            }
            validate_event("risk_event", risk_event)
            new_events.append(risk_event)
            window_start = _window_start(event_time, self._window_minutes)
            window_end = window_start + timedelta(minutes=self._window_minutes)
            self._density[(window_start, window_end, cell)].add(event["vessel_id"])
        self._risk_events.extend(new_events)
        return new_events

    def result(self) -> dict[str, Any]:
        """Return risk events, density metrics, quarantine, and stats accumulated so far."""
        density_metrics = [
            {
                "schema_version": SCHEMA_VERSION,
                "window_start": _timestamp(window_start),
                "window_end": _timestamp(window_end),
                "h3_index": cell,
                "h3_resolution": self._resolution,
                "unique_vessel_count": len(vessels),
            }
            for (window_start, window_end, cell), vessels in sorted(self._density.items())
        ]
        for metric in density_metrics:
            validate_event("density_metric", metric)
        risk_events = sorted(
            self._risk_events,
            key=lambda event: (
                _parse_utc(event["event_time"]),
                event["vessel_id"],
                event["event_id"],
            ),
        )
        latest = self._max_event_time
        stats = dict(self.stats)
        stats["max_event_time"] = _timestamp(latest) if latest else None
        stats["watermark"] = _timestamp(latest - self._allowed_lateness) if latest else None
        stats["quarantined"] = len(self.quarantine)
        return {
            "risk_events": risk_events,
            "density_metrics": density_metrics,
            "quarantine": list(self.quarantine),
            "stats": stats,
        }


def process_events(
    telemetry: Iterable[Mapping[str, Any]],
    alerts: Iterable[Mapping[str, Any]],
    *,
    resolution: int = DEFAULT_RESOLUTION,
    strict: bool = False,
    coverage_mode: str = DEFAULT_COVERAGE_MODE,
    window_minutes: int = WINDOW_MINUTES,
    allowed_lateness_minutes: int = 10,
    max_future_minutes: int = MAX_FUTURE_MINUTES,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Create risk events and unique-vessel density metrics from one batch.

    All alerts are applied before any telemetry; see :class:`StreamProcessor`
    for the matching, watermark, and future-event rules.
    """
    processor = StreamProcessor(
        resolution=resolution,
        strict=strict,
        coverage_mode=coverage_mode,
        window_minutes=window_minutes,
        allowed_lateness_minutes=allowed_lateness_minutes,
        max_future_minutes=max_future_minutes,
        now=now,
    )
    logger.info("Processing batch", extra={"georisk": processor.settings})
    processor.add_alerts(alerts)
    processor.add_telemetry(telemetry)
    result = processor.result()
    logger.info("Processing complete", extra={"georisk": {**processor.settings, **result["stats"]}})
    return result


def _json_safe(value: Any) -> Any:
    """Replace NaN/infinity with strings so quarantined records stay valid JSON."""
    if isinstance(value, float) and not math.isfinite(value):
        return str(value)
    if isinstance(value, Mapping):
        return {key: _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return value


def _quarantine_entry(contract: str, reason: str, record: Any, exc: Exception) -> dict[str, Any]:
    """Preserve rejected input with validation details and a UTC timestamp."""
    entry = {
        "contract": contract,
        "reason": reason,
        "errors": exc.errors if isinstance(exc, ContractValidationError) else [str(exc)],
        "record": _json_safe(dict(record) if isinstance(record, Mapping) else record),
        "quarantined_at": _timestamp(datetime.now(UTC)),
    }
    logger.debug("Record quarantined", extra={"georisk": entry})
    return entry


def _read_jsonl(
    path: Path, contract: str, quarantine: list[dict[str, Any]], *, strict: bool
) -> list[dict[str, Any]]:
    records = []
    with path.open(encoding="utf-8") as source:
        for line_number, line in enumerate(source, 1):
            if not line.strip():
                continue
            try:
                record = json.loads(line)
                if not isinstance(record, dict):
                    raise ValueError(f"expected an object in {path}:{line_number}")
            except ValueError as exc:
                if strict:
                    raise ValueError(f"invalid JSON object in {path}:{line_number}: {exc}") from exc
                quarantine.append(
                    _quarantine_entry(
                        contract, "invalid_json", {"line": line_number, "raw": line[:1000]}, exc
                    )
                )
                continue
            records.append(record)
    return records


def _write_atomic(path: Path, text: str) -> None:
    """Replace ``path`` in one step so concurrent readers never see a partial file."""
    temporary = path.with_name(f".{path.name}.tmp")
    try:
        temporary.write_text(text, encoding="utf-8")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _write_jsonl(path: Path, records: Iterable[Mapping[str, Any]]) -> None:
    _write_atomic(path, "".join(json.dumps(record, sort_keys=True) + "\n" for record in records))


def include_unreadable(results: dict[str, Any], unreadable: list[dict[str, Any]]) -> None:
    """Prepend quarantine entries for unreadable input lines and count them as input."""
    for entry in unreadable:
        key = "telemetry_in" if entry["contract"] == "vessel_telemetry" else "alerts_in"
        results["stats"][key] += 1
    results["quarantine"] = unreadable + results["quarantine"]
    results["stats"]["quarantined"] = len(results["quarantine"])


def run_metadata(
    results: Mapping[str, Any],
    *,
    resolution: int,
    coverage_mode: str,
    window_minutes: int,
    allowed_lateness_minutes: int,
    max_future_minutes: int,
) -> dict[str, Any]:
    """Describe one processing run for ``run-metadata.json`` and the dashboard."""
    return {
        "processed_at": _timestamp(datetime.now(UTC)),
        "latest_event_time": results["stats"]["max_event_time"],
        "watermark": results["stats"]["watermark"],
        "h3_resolution": resolution,
        "coverage_mode": coverage_mode,
        "window_minutes": window_minutes,
        "allowed_lateness_minutes": allowed_lateness_minutes,
        "max_future_minutes": max_future_minutes,
        "counts": dict(results["stats"]),
        "simulated": True,
    }


def write_outputs(output_dir: str | Path, results: Mapping[str, Any]) -> None:
    """Write processed JSONL outputs, then ``run-metadata.json`` last.

    Each file is replaced atomically and the metadata goes last, so a reader
    that sees new metadata also sees the outputs it describes.
    """
    directory = Path(output_dir)
    directory.mkdir(parents=True, exist_ok=True)
    _write_jsonl(directory / "risk-events.jsonl", results["risk_events"])
    _write_jsonl(directory / "density-metrics.jsonl", results["density_metrics"])
    _write_jsonl(directory / "quarantine.jsonl", results["quarantine"])
    _write_atomic(
        directory / "run-metadata.json",
        json.dumps(results["metadata"], indent=2, sort_keys=True) + "\n",
    )


def run_streaming_job(config: Mapping[str, Any]) -> dict[str, Any]:
    """Run the local batch-compatible processing path.

    The config accepts either in-memory ``telemetry``/``weather_alerts`` lists
    or an ``input_dir`` containing Phase 2 JSONL files. Results are returned
    and optionally written to ``output_dir`` as JSONL.
    """
    if not isinstance(config, Mapping):
        raise TypeError("config must be a mapping")
    resolution = config.get("resolution", DEFAULT_RESOLUTION)
    strict = config.get("strict", False)
    if not isinstance(strict, bool):
        raise TypeError("strict must be a boolean")
    quarantine: list[dict[str, Any]] = []
    if "telemetry" in config or "weather_alerts" in config:
        telemetry = config.get("telemetry", [])
        alerts = config.get("weather_alerts", [])
    else:
        input_dir = config.get("input_dir")
        if not input_dir:
            raise ValueError("config requires telemetry/weather_alerts or input_dir")
        directory = Path(input_dir)
        telemetry = _read_jsonl(
            directory / "vessel-telemetry.jsonl", "vessel_telemetry", quarantine, strict=strict
        )
        alerts = _read_jsonl(
            directory / "weather-alerts.jsonl", "weather_alert", quarantine, strict=strict
        )
    results = process_events(
        telemetry,
        alerts,
        resolution=resolution,
        strict=strict,
        coverage_mode=config.get("coverage_mode", DEFAULT_COVERAGE_MODE),
        window_minutes=config.get("window_minutes", WINDOW_MINUTES),
        allowed_lateness_minutes=config.get("allowed_lateness_minutes", 10),
        max_future_minutes=config.get("max_future_minutes", MAX_FUTURE_MINUTES),
    )
    include_unreadable(results, quarantine)
    results["metadata"] = run_metadata(
        results,
        resolution=resolution,
        coverage_mode=config.get("coverage_mode", DEFAULT_COVERAGE_MODE),
        window_minutes=config.get("window_minutes", WINDOW_MINUTES),
        allowed_lateness_minutes=config.get("allowed_lateness_minutes", 10),
        max_future_minutes=config.get("max_future_minutes", MAX_FUTURE_MINUTES),
    )
    if config.get("output_dir"):
        write_outputs(config["output_dir"], results)
    logger.info(
        "Processing run complete",
        extra={
            "georisk": {
                **results["stats"],
                "h3_resolution": resolution,
                "coverage_mode": results["metadata"]["coverage_mode"],
                "window_minutes": results["metadata"]["window_minutes"],
                "allowed_lateness_minutes": results["metadata"]["allowed_lateness_minutes"],
            }
        },
    )
    return results

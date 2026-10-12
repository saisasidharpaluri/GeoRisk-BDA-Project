"""Measure H3 coverage, exposure, density, and runtime across resolutions."""

from __future__ import annotations

import json
import logging
from time import perf_counter
from typing import Any, Iterable, Mapping

import h3

from georisk.config import GeoRiskConfig
from georisk.contracts import ContractValidationError, validate_event
from georisk.ingestion.generator import generate_events
from georisk.logging_utils import configure_logging
from georisk.processing.h3_index import polygon_to_cells
from georisk.processing.stream_job import _parse_utc, process_events

logger = logging.getLogger(__name__)


def compare_resolutions(
    telemetry: Iterable[Mapping[str, Any]],
    alerts: Iterable[Mapping[str, Any]],
    resolutions: Iterable[int] = (7, 8),
    coverage_mode: str = "overlap",
) -> list[dict[str, Any]]:
    """Compare identical batches; hazard and density counts are unique cells."""
    telemetry_records = list(telemetry)
    alert_records = list(alerts)
    latest_alerts: dict[str, Mapping[str, Any]] = {}
    for alert in alert_records:
        try:
            validate_event("weather_alert", alert)
        except (ContractValidationError, TypeError):
            continue
        previous = latest_alerts.get(alert["alert_id"])
        if previous is None or _parse_utc(alert["event_time"]) >= _parse_utc(
            previous["event_time"]
        ):
            latest_alerts[alert["alert_id"]] = alert

    rows = []
    for resolution in resolutions:
        start = perf_counter()
        result = process_events(
            telemetry_records,
            alert_records,
            resolution=resolution,
            coverage_mode=coverage_mode,
        )
        hazard_cells: set[str] = set()
        for alert in latest_alerts.values():
            hazard_cells.update(polygon_to_cells(alert["geometry"], resolution, coverage_mode))
        alerted = [event for event in result["risk_events"] if event["risk_alert"]]
        density = result["density_metrics"]
        rows.append(
            {
                "resolution": resolution,
                "avg_cell_area_km2": h3.average_hexagon_area(resolution, "km^2"),
                "hazard_cell_count": len(hazard_cells),
                "alerted_event_count": len(alerted),
                "exposed_vessel_count": len({event["vessel_id"] for event in alerted}),
                "density_cell_count": len({metric["h3_index"] for metric in density}),
                "max_unique_vessels": max(
                    (metric["unique_vessel_count"] for metric in density), default=0
                ),
                "runtime_seconds": perf_counter() - start,
            }
        )
    return rows


def comparison_cli() -> None:
    """Write a comparison report beside the configured pipeline report."""
    settings = GeoRiskConfig.from_env()
    configure_logging(settings.log_level)
    events = generate_events(
        settings.seed,
        settings.vessel_count,
        telemetry_points=settings.telemetry_points,
    )
    rows = compare_resolutions(
        events["telemetry"],
        events["weather_alerts"],
        coverage_mode=settings.coverage_mode,
    )
    destination = settings.report_path.parent / "resolution-comparison.json"
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(rows, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    logger.info(
        "Resolution comparison written",
        extra={
            "georisk": {
                "report_path": str(destination),
                "seed": settings.seed,
                "coverage_mode": settings.coverage_mode,
            }
        },
    )
    print(json.dumps(rows, indent=2, sort_keys=True))


if __name__ == "__main__":
    comparison_cli()

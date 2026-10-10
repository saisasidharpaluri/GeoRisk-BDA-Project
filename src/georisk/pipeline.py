"""Repeatable local end-to-end orchestration for the GeoRisk demonstration."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from georisk.config import GeoRiskConfig
from georisk.ingestion.generator import generate_events, write_jsonl
from georisk.processing.stream_job import run_streaming_job
from georisk.route_dashboard.dashboard import dashboard_summary, load_dashboard_data
from georisk.route_dashboard.route_optimizer import (
    demonstration_graph,
    find_alternative_route,
)
from georisk.storage_analytics.delay_model import make_synthetic_label, split_by_time, train_and_evaluate
from georisk.storage_analytics.lake import density_summary, risk_summary, write_dataset
from georisk.operations import acceptance_checks, artifact_manifest


def _event_date(events: list[dict[str, Any]]) -> str:
    if not events:
        return datetime.now().date().isoformat()
    return events[0]["event_time"][:10]


def _delay_rows(
    risk_events: list[dict[str, Any]], density_metrics: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    density_by_cell = {
        row["h3_index"]: row["unique_vessel_count"] for row in density_metrics
    }
    rows = []
    for index, event in enumerate(risk_events):
        hazards = event["matching_hazards"]
        row = {
            "event_time": event["event_time"],
            "baseline_hours": 16.0,
            "density": float(density_by_cell.get(event["h3_index"], 0)),
            "hazard_severity": float(max((item["severity"] for item in hazards), default=0)),
            "speed_knots": 14.0,
            "route_distance_nm": 500.0 + index,
        }
        row["delay_hours"] = make_synthetic_label(row)
        rows.append(row)
    return rows


def run_end_to_end(config: GeoRiskConfig | None = None) -> dict[str, Any]:
    """Run generation, processing, lake writes, model evaluation, and routing."""
    settings = config or GeoRiskConfig.from_env()
    events = generate_events(
        settings.seed,
        settings.vessel_count,
        telemetry_points=settings.telemetry_points,
    )
    generation_stats = write_jsonl(events, settings.input_dir)
    processed = run_streaming_job(
        {
            "input_dir": settings.input_dir,
            "output_dir": settings.processed_dir,
            "resolution": settings.resolution,
        }
    )
    event_date = _event_date(events["telemetry"])
    risk_path = write_dataset(
        processed["risk_events"],
        settings.lake_dir,
        event_date,
        dataset_name="risk_events",
    )
    density_path = write_dataset(
        processed["density_metrics"],
        settings.lake_dir,
        event_date,
        dataset_name="density_metrics",
    )
    delay_rows = _delay_rows(
        processed["risk_events"], processed["density_metrics"]
    )
    if len(delay_rows) < 2:
        raise ValueError("end-to-end run requires at least two telemetry-derived model rows")
    training, evaluation = split_by_time(delay_rows, settings.evaluation_fraction)
    model = train_and_evaluate(training, evaluation)
    route = find_alternative_route(
        demonstration_graph(),
        "port_a",
        "port_b",
        settings.route_risk_threshold,
        risk_penalty=settings.route_risk_penalty,
    )
    summary = dashboard_summary(load_dashboard_data(settings.dashboard_dir))
    artifacts = artifact_manifest(
        [
            settings.input_dir / "vessel-telemetry.jsonl",
            settings.input_dir / "weather-alerts.jsonl",
            settings.processed_dir / "risk-events.jsonl",
            settings.processed_dir / "density-metrics.jsonl",
            risk_path,
            density_path,
        ]
    )
    report = {
        "status": "success",
        "config": {
            "seed": settings.seed,
            "vessel_count": settings.vessel_count,
            "telemetry_points": settings.telemetry_points,
            "resolution": settings.resolution,
            "event_date": event_date,
            "acceptance_required": settings.acceptance_required,
        },
        "generation": {
            "generated": generation_stats.generated,
            "published": generation_stats.published,
            "rejected": generation_stats.rejected,
        },
        "processing": {
            "risk_event_count": len(processed["risk_events"]),
            "density_metric_count": len(processed["density_metrics"]),
        },
        "lake": {
            "risk_path": str(risk_path),
            "density_path": str(density_path),
            "risk_summary": risk_summary(settings.lake_dir),
            "density_row_count": len(density_summary(settings.lake_dir)),
        },
        "model": {
            "evaluation_count": model["evaluation_count"],
            "model_metrics": model["model_metrics"],
            "baseline_metrics": model["baseline_metrics"],
        },
        "route": route,
        "dashboard": summary,
        "artifacts": artifacts,
    }
    report["acceptance"] = acceptance_checks(report)
    settings.report_path.parent.mkdir(parents=True, exist_ok=True)
    settings.report_path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    return report


def run_cli() -> None:
    """Console entry point for a configured local demonstration run."""
    report = run_end_to_end()
    print(json.dumps(report, indent=2, sort_keys=True))
    if report["config"]["acceptance_required"] and not report["acceptance"]["passed"]:
        raise SystemExit(1)

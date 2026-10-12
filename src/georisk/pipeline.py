"""Repeatable local end-to-end orchestration for the GeoRisk demonstration."""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any

from georisk.config import GeoRiskConfig, load_env_file
from georisk.contracts import SCHEMA_VERSION
from georisk.geo_demo import DEMO_NODES
from georisk.ingestion.generator import write_jsonl
from georisk.ingestion.scenario import ScenarioConfig, generate_scenario
from georisk.logging_utils import configure_logging
from georisk.operations import acceptance_checks, artifact_manifest
from georisk.processing.stream_job import run_streaming_job
from georisk.route_dashboard.dashboard import (
    dashboard_route,
    dashboard_summary,
    load_dashboard_data,
)
from georisk.scenarios import run_acceptance_scenarios
from georisk.storage_analytics.delay_features import build_delay_rows
from georisk.storage_analytics.delay_model import (
    FEATURES,
    predict,
    split_by_time,
    train_and_evaluate,
)
from georisk.storage_analytics.lake import density_summary, risk_summary, write_partitioned

logger = logging.getLogger(__name__)


def run_end_to_end(config: GeoRiskConfig | None = None) -> dict[str, Any]:
    """Run generation, processing, lake writes, model evaluation, and routing."""
    settings = config or GeoRiskConfig.from_env()
    events = generate_scenario(
        ScenarioConfig(
            seed=settings.seed,
            vessel_count=settings.vessel_count,
            duration_minutes=settings.scenario_duration_minutes,
            interval_seconds=settings.telemetry_interval_seconds,
        )
    )
    generation_stats = write_jsonl(events, settings.input_dir)
    logger.info(
        "Generation complete",
        extra={
            "georisk": {
                "generated": generation_stats.generated,
                "published": generation_stats.published,
                "rejected": generation_stats.rejected,
            }
        },
    )
    processed = run_streaming_job(
        {
            "input_dir": settings.input_dir,
            "output_dir": settings.processed_dir,
            "resolution": settings.resolution,
            "coverage_mode": settings.coverage_mode,
            "window_minutes": settings.density_window_minutes,
            "allowed_lateness_minutes": settings.allowed_lateness_minutes,
            "strict": settings.strict_validation,
        }
    )
    lake_paths = {
        name: write_partitioned(records, settings.lake_dir, dataset_name=name)
        for name, records in {
            "vessel_telemetry": events["telemetry"],
            "weather_alerts": events["weather_alerts"],
            "risk_events": processed["risk_events"],
            "density_metrics": processed["density_metrics"],
        }.items()
    }
    logger.info(
        "Lake datasets written",
        extra={
            "georisk": {
                "paths": lake_paths,
            }
        },
    )
    delay_rows = build_delay_rows(
        events["telemetry"],
        processed["risk_events"],
        processed["density_metrics"],
        destination=DEMO_NODES["port_b"],
        window_minutes=settings.density_window_minutes,
        seed=settings.seed,
        noise_std_hours=settings.delay_noise_std_hours,
    )
    if len(delay_rows) < 2:
        raise ValueError("end-to-end run requires at least two telemetry-derived model rows")
    training, evaluation = split_by_time(delay_rows, settings.evaluation_fraction)
    all_rows = training + evaluation
    if settings.delay_model == "gbt":
        from georisk.spark_session import get_spark
        from georisk.storage_analytics.spark_delay_model import (
            predict_gbt,
            save_gbt_model,
            train_and_evaluate_gbt,
        )

        spark = get_spark("georisk-delay")
        model = train_and_evaluate_gbt(spark, training, evaluation, seed=settings.seed)
        predictions = predict_gbt(spark, model["model"], all_rows)
        model_path = settings.processed_dir / "delay-model-gbt"
        save_gbt_model(model["model"], model_path)
        model_artifacts = sorted(
            path
            for path in model_path.rglob("*")
            if path.is_file() and not path.name.startswith(".")
        )
    else:
        model = train_and_evaluate(training, evaluation)
        predictions = predict(model["coefficients"], all_rows)
        model_path = settings.processed_dir / "delay-model.json"
        model_path.write_text(
            json.dumps(
                {
                    "model_name": "linear",
                    "features": model["features"],
                    "coefficients": model["coefficients"],
                },
                indent=2,
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        model_artifacts = [model_path]
    baseline_delay = model["baseline_predictions"][0]
    estimates = [
        {
            "schema_version": SCHEMA_VERSION,
            "event_id": row["event_id"],
            "event_time": row["event_time"],
            "vessel_id": row["vessel_id"],
            "split": "train" if index < len(training) else "evaluation",
            "model_name": settings.delay_model,
            "predicted_delay_hours": prediction,
            "baseline_delay_hours": baseline_delay,
            "synthetic_label_hours": row["delay_hours"],
            **{feature: row[feature] for feature in FEATURES},
        }
        for index, (row, prediction) in enumerate(zip(all_rows, predictions, strict=False))
    ]
    lake_paths["delay_estimates"] = write_partitioned(
        estimates,
        settings.lake_dir,
        dataset_name="delay_estimates",
    )
    estimates_path = settings.processed_dir / "delay-estimates.jsonl"
    estimates_path.write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in estimates),
        encoding="utf-8",
    )
    logger.info(
        "Model evaluated",
        extra={
            "georisk": {
                "evaluation_count": model["evaluation_count"],
                "model_metrics": model["model_metrics"],
                "baseline_metrics": model["baseline_metrics"],
            }
        },
    )
    dashboard_data = load_dashboard_data(
        settings.dashboard_dir,
        input_dir=settings.input_dir,
        processed_dir=settings.processed_dir,
    )
    route = dashboard_route(
        dashboard_data,
        resolution=settings.resolution,
        coverage_mode=settings.coverage_mode,
        risk_threshold=settings.route_risk_threshold,
        risk_penalty=settings.route_risk_penalty,
    )
    logger.info("Route decision", extra={"georisk": route})
    summary = dashboard_summary(dashboard_data, resolution=settings.resolution)
    artifacts = artifact_manifest(
        [
            settings.input_dir / "vessel-telemetry.jsonl",
            settings.input_dir / "weather-alerts.jsonl",
            settings.processed_dir / "risk-events.jsonl",
            settings.processed_dir / "density-metrics.jsonl",
            estimates_path,
            *model_artifacts,
            *(path for paths in lake_paths.values() for path in paths),
        ]
    )
    report = {
        "status": "success",
        "config": {
            "seed": settings.seed,
            "vessel_count": settings.vessel_count,
            "telemetry_points": settings.telemetry_points,
            "scenario_duration_minutes": settings.scenario_duration_minutes,
            "telemetry_interval_seconds": settings.telemetry_interval_seconds,
            "resolution": settings.resolution,
            "coverage_mode": settings.coverage_mode,
            "window_minutes": settings.density_window_minutes,
            "allowed_lateness_minutes": settings.allowed_lateness_minutes,
            "acceptance_required": settings.acceptance_required,
            "strict_validation": settings.strict_validation,
            "delay_noise_std_hours": settings.delay_noise_std_hours,
            "delay_model": settings.delay_model,
        },
        "generation": {
            "generated": generation_stats.generated,
            "published": generation_stats.published,
            "rejected": generation_stats.rejected,
        },
        "processing": {
            "risk_event_count": len(processed["risk_events"]),
            "density_metric_count": len(processed["density_metrics"]),
            "quarantined_count": len(processed["quarantine"]),
            "stats": processed["stats"],
            "metadata": processed["metadata"],
        },
        "lake": {
            "paths": lake_paths,
            "risk_summary": risk_summary(settings.lake_dir),
            "density_row_count": len(density_summary(settings.lake_dir)),
        },
        "model": {
            "model_name": settings.delay_model,
            "evaluation_count": model["evaluation_count"],
            "model_metrics": model["model_metrics"],
            "baseline_metrics": model["baseline_metrics"],
        },
        "route": route,
        "dashboard": summary,
        "artifacts": artifacts,
        "scenarios": run_acceptance_scenarios(
            resolution=settings.resolution,
            coverage_mode=settings.coverage_mode,
            window_minutes=settings.density_window_minutes,
            allowed_lateness_minutes=settings.allowed_lateness_minutes,
        ),
    }
    report["acceptance"] = acceptance_checks(report)
    settings.report_path.parent.mkdir(parents=True, exist_ok=True)
    settings.report_path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    return report


def run_cli() -> None:
    """Console entry point for a configured local demonstration run."""
    env_file = Path(os.environ.get("GEORISK_ENV_FILE", ".env"))
    if env_file.is_file():
        load_env_file(env_file)
    settings = GeoRiskConfig.from_env()
    configure_logging(settings.log_level)
    report = run_end_to_end(settings)
    print(json.dumps(report, indent=2, sort_keys=True))
    if report["config"]["acceptance_required"] and not report["acceptance"]["passed"]:
        raise SystemExit(1)

"""Operational manifests and final acceptance checks for GeoRisk runs."""

from __future__ import annotations

import hashlib
import json
import math
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping


def sha256_file(path: str | Path) -> str:
    """Return the SHA-256 digest of one file."""
    target = Path(path)
    if not target.is_file():
        raise FileNotFoundError(f"artifact does not exist: {target}")
    digest = hashlib.sha256()
    with target.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def artifact_manifest(paths: Iterable[str | Path]) -> list[dict[str, Any]]:
    """Build a stable manifest for existing files, sorted by path."""
    entries = []
    for value in paths:
        path = Path(value)
        if path.is_file():
            entries.append(
                {
                    "path": str(path),
                    "bytes": path.stat().st_size,
                    "sha256": sha256_file(path),
                }
            )
    return sorted(entries, key=lambda entry: entry["path"])


def acceptance_checks(report: Mapping[str, Any]) -> dict[str, Any]:
    """Evaluate the project acceptance criteria from one pipeline report."""
    checks: list[dict[str, Any]] = []

    def check(name: str, passed: bool, detail: str) -> None:
        checks.append({"name": name, "passed": bool(passed), "detail": detail})

    generation = report.get("generation", {})
    processing = report.get("processing", {})
    lake = report.get("lake", {})
    model = report.get("model", {})
    route = report.get("route", {})
    dashboard = report.get("dashboard", {})

    check("run_status", report.get("status") == "success", "pipeline status is success")
    check(
        "generation_accounting",
        generation.get("generated")
        == generation.get("published", 0) + generation.get("rejected", 0),
        "generated equals published plus rejected",
    )
    check(
        "risk_and_density_outputs",
        processing.get("risk_event_count", 0) > 0
        and processing.get("density_metric_count", 0) > 0,
        "risk and density outputs are non-empty",
    )
    lake_paths = [lake.get("risk_path"), lake.get("density_path")]
    check(
        "parquet_outputs",
        all(path and Path(path).is_file() for path in lake_paths),
        "curated Parquet files exist",
    )
    model_metrics = model.get("model_metrics", {})
    baseline_metrics = model.get("baseline_metrics", {})
    try:
        model_valid = all(
            math.isfinite(float(model_metrics.get(metric, math.nan)))
            and math.isfinite(float(baseline_metrics.get(metric, math.nan)))
            for metric in ("mae", "rmse")
        )
    except (TypeError, ValueError):
        model_valid = False
    check("model_metrics", model_valid, "model and baseline metrics are finite")
    check(
        "model_beats_baseline",
        model_valid
        and model_metrics["mae"] <= baseline_metrics["mae"]
        and model_metrics["rmse"] <= baseline_metrics["rmse"],
        "model metrics are no worse than the mean baseline",
    )
    check(
        "route_comparison",
        bool(route.get("baseline", {}).get("path"))
        and bool(route.get("alternative", {}).get("path")),
        "baseline and alternative routes are present",
    )
    check(
        "dashboard_freshness",
        dashboard.get("has_data") is True
        and bool(dashboard.get("latest_event_time")),
        "dashboard has data and a latest event timestamp",
    )
    passed = all(item["passed"] for item in checks)
    return {
        "status": "accepted" if passed else "rejected",
        "passed": passed,
        "checked_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "checks": checks,
    }


def validate_report(report_path: str | Path) -> dict[str, Any]:
    """Load a persisted report and evaluate its acceptance checks."""
    path = Path(report_path)
    if not path.is_file():
        raise FileNotFoundError(f"run report does not exist: {path}")
    try:
        report = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"run report is not valid JSON: {path}") from exc
    if not isinstance(report, dict):
        raise ValueError("run report must contain a JSON object")
    result = acceptance_checks(report)
    return {"report_path": str(path), **result}


def validate_cli() -> None:
    """Validate the configured Phase 7 report from the command line."""
    report_path = os.environ.get("GEORISK_REPORT_PATH", "output/reports/phase6-run.json")
    result = validate_report(report_path)
    print(json.dumps(result, indent=2, sort_keys=True))
    if not result["passed"]:
        raise SystemExit(1)

from __future__ import annotations

import pytest

from georisk.config import GeoRiskConfig
from georisk.operations import acceptance_checks
from georisk.pipeline import run_end_to_end
from georisk.scenarios import expected_scenario_names, run_acceptance_scenarios


@pytest.mark.parametrize(
    "resolution,mode,window,lateness",
    [
        (7, "overlap", 15, 10),
        (8, "overlap", 5, 0),
        (7, "center", 30, 60),
    ],
)
def test_all_scenarios_pass_with_defaults(resolution, mode, window, lateness):
    results = run_acceptance_scenarios(
        resolution=resolution,
        coverage_mode=mode,
        window_minutes=window,
        allowed_lateness_minutes=lateness,
    )
    assert {item["name"] for item in results} == set(expected_scenario_names(mode))
    assert all(item["passed"] and item["detail"] for item in results), results


def test_missing_scenario_rejects_report(tmp_path):
    report = run_end_to_end(
        GeoRiskConfig(
            input_dir=tmp_path / "data",
            processed_dir=tmp_path / "processed",
            lake_dir=tmp_path / "lake",
            dashboard_dir=tmp_path,
            report_path=tmp_path / "report.json",
        )
    )
    assert report["acceptance"]["passed"]
    for scenario in report["scenarios"]:
        changed = {
            **report,
            "scenarios": [item for item in report["scenarios"] if item != scenario],
        }
        result = acceptance_checks(changed)
        assert not result["passed"]
        assert (
            next(
                item for item in result["checks"] if item["name"] == "scenario_" + scenario["name"]
            )["passed"]
            is False
        )
    report["scenarios"][0]["passed"] = False
    assert not acceptance_checks(report)["passed"]


def test_dashboard_labels_are_required():
    config = {"resolution": 8}
    labels = {
        "simulated": True,
        "h3_resolution": 8,
        "units": {"delay": "hours", "speed": "knots", "distance": "nm"},
    }
    for key in labels:
        dashboard = {name: value for name, value in labels.items() if name != key}
        result = acceptance_checks({"config": config, "dashboard": dashboard})
        assert (
            next(item for item in result["checks"] if item["name"] == "dashboard_labels")["passed"]
            is False
        )

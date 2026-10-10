import json

import pytest

from georisk.config import GeoRiskConfig
from georisk.operations import (
    acceptance_checks,
    artifact_manifest,
    sha256_file,
    validate_report,
)
from georisk.pipeline import run_end_to_end


def test_artifact_manifest_is_stable_and_detects_content(tmp_path):
    first = tmp_path / "a.txt"
    second = tmp_path / "b.txt"
    first.write_text("alpha", encoding="utf-8")
    second.write_text("beta", encoding="utf-8")

    manifest = artifact_manifest([second, first])
    assert [entry["path"] for entry in manifest] == sorted(
        [str(first), str(second)]
    )
    assert manifest[0]["sha256"] == sha256_file(manifest[0]["path"])
    first.write_text("changed", encoding="utf-8")
    assert artifact_manifest([first])[0]["sha256"] != manifest[0]["sha256"]


def test_acceptance_checks_reject_missing_or_invalid_report_data():
    result = acceptance_checks(
        {
            "status": "failed",
            "generation": {},
            "processing": {},
            "lake": {},
            "model": {},
            "route": {},
            "dashboard": {},
        }
    )
    assert result["status"] == "rejected"
    assert result["passed"] is False
    assert any(not check["passed"] for check in result["checks"])


def test_phase7_pipeline_report_is_accepted(tmp_path):
    config = GeoRiskConfig(
        vessel_count=6,
        telemetry_points=3,
        input_dir=tmp_path / "data",
        processed_dir=tmp_path / "processed",
        lake_dir=tmp_path / "lake",
        dashboard_dir=tmp_path,
        report_path=tmp_path / "reports" / "phase7.json",
    )
    report = run_end_to_end(config)
    assert report["acceptance"]["status"] == "accepted"
    assert len(report["artifacts"]) == 6
    validated = validate_report(config.report_path)
    assert validated["passed"] is True


def test_validate_report_rejects_tampered_artifact(tmp_path):
    config = GeoRiskConfig(
        vessel_count=4,
        telemetry_points=3,
        input_dir=tmp_path / "data",
        processed_dir=tmp_path / "processed",
        lake_dir=tmp_path / "lake",
        dashboard_dir=tmp_path,
        report_path=tmp_path / "reports" / "phase7.json",
    )
    run_end_to_end(config)
    report = json.loads(config.report_path.read_text(encoding="utf-8"))
    report["model"]["model_metrics"]["mae"] = 999
    config.report_path.write_text(json.dumps(report), encoding="utf-8")
    assert validate_report(config.report_path)["passed"] is False


def test_validate_report_rejects_invalid_json_and_missing_file(tmp_path):
    with pytest.raises(FileNotFoundError):
        validate_report(tmp_path / "missing.json")
    invalid = tmp_path / "invalid.json"
    invalid.write_text("not-json", encoding="utf-8")
    with pytest.raises(ValueError, match="valid JSON"):
        validate_report(invalid)

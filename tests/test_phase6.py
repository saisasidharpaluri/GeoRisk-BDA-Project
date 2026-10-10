import json

import pytest

from georisk.config import GeoRiskConfig
from georisk.pipeline import run_end_to_end


def test_config_loads_and_validates_environment_values(tmp_path):
    config = GeoRiskConfig.from_env(
        {
            "GEORISK_SEED": "11",
            "GEORISK_VESSEL_COUNT": "4",
            "GEORISK_TELEMETRY_POINTS": "2",
            "GEORISK_RESOLUTION": "7",
            "GEORISK_INPUT_DIR": str(tmp_path / "data"),
            "GEORISK_OUTPUT_DIR": str(tmp_path / "processed"),
            "GEORISK_LAKE_DIR": str(tmp_path / "lake"),
            "GEORISK_DASHBOARD_DATA_DIR": str(tmp_path),
            "GEORISK_REPORT_PATH": str(tmp_path / "report.json"),
            "GEORISK_ROUTE_RISK_THRESHOLD": "0.5",
            "GEORISK_ROUTE_RISK_PENALTY": "4",
            "GEORISK_EVALUATION_FRACTION": "0.25",
        }
    )
    assert config.seed == 11
    assert config.vessel_count == 4
    assert config.evaluation_fraction == 0.25
    assert config.acceptance_required is True


def test_config_rejects_invalid_values():
    with pytest.raises(ValueError, match="GEORISK_RESOLUTION"):
        GeoRiskConfig.from_env({"GEORISK_RESOLUTION": "16"})
    with pytest.raises(ValueError, match="GEORISK_ROUTE_RISK_THRESHOLD"):
        GeoRiskConfig.from_env({"GEORISK_ROUTE_RISK_THRESHOLD": "bad"})
    with pytest.raises(ValueError, match="GEORISK_ACCEPTANCE_REQUIRED"):
        GeoRiskConfig.from_env({"GEORISK_ACCEPTANCE_REQUIRED": "sometimes"})


def test_phase6_runs_all_local_stages_and_writes_report(tmp_path):
    config = GeoRiskConfig(
        seed=7,
        vessel_count=6,
        telemetry_points=3,
        input_dir=tmp_path / "data",
        processed_dir=tmp_path / "processed",
        lake_dir=tmp_path / "lake",
        dashboard_dir=tmp_path,
        report_path=tmp_path / "reports" / "phase6.json",
    )
    report = run_end_to_end(config)

    assert report["status"] == "success"
    assert report["generation"]["generated"] == 20
    assert report["processing"]["risk_event_count"] == 18
    assert report["processing"]["density_metric_count"] > 0
    assert report["model"]["evaluation_count"] > 0
    assert report["model"]["model_metrics"]["mae"] <= report["model"]["baseline_metrics"]["mae"]
    assert report["route"]["route_changed"] is True
    assert report["dashboard"]["has_data"] is True
    saved = json.loads(config.report_path.read_text(encoding="utf-8"))
    assert saved["status"] == "success"


def test_aws_foundation_template_is_safe_and_complete():
    path = "infra/aws/georisk-foundation.template.json"
    template = json.loads(open(path, encoding="utf-8").read())
    resources = template["Resources"]
    assert {"DataBucket", "TelemetryStream", "WeatherAlertStream", "GlueDatabase", "AthenaWorkGroup"} <= set(resources)
    bucket = resources["DataBucket"]["Properties"]
    assert bucket["BucketEncryption"]["ServerSideEncryptionConfiguration"][0]["ServerSideEncryptionByDefault"]["SSEAlgorithm"] == "AES256"
    assert bucket["PublicAccessBlockConfiguration"]["BlockPublicPolicy"] is True
    assert resources["TelemetryStream"]["Properties"]["ShardCount"] == 1
    assert resources["WeatherAlertStream"]["Properties"]["RetentionPeriodHours"] == 24

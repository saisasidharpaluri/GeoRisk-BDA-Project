import json
import os

import pytest

from georisk.config import GeoRiskConfig, load_env_file
from georisk.pipeline import run_end_to_end


def test_config_loads_and_validates_environment_values(tmp_path):
    config = GeoRiskConfig.from_env(
        {
            "GEORISK_SEED": "11",
            "GEORISK_VESSEL_COUNT": "4",
            "GEORISK_TELEMETRY_POINTS": "2",
            "GEORISK_SCENARIO_DURATION_MINUTES": "60",
            "GEORISK_TELEMETRY_INTERVAL_SECONDS": "120",
            "GEORISK_RESOLUTION": "7",
            "GEORISK_DENSITY_WINDOW_MINUTES": "5",
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
    assert config.density_window_minutes == 5
    assert config.seed == 11
    assert config.vessel_count == 4
    assert config.scenario_duration_minutes == 60
    assert config.telemetry_interval_seconds == 120
    assert config.evaluation_fraction == 0.25
    assert config.acceptance_required is True


def test_env_file_loaded_without_overriding_shell(tmp_path, monkeypatch):
    path = tmp_path / ".env"
    path.write_text("GEORISK_SEED=19\nGEORISK_VESSEL_COUNT=4\n")
    monkeypatch.setattr(os, "environ", {"GEORISK_SEED": "11"})
    assert load_env_file(path) == {"GEORISK_VESSEL_COUNT": "4"}
    assert GeoRiskConfig.from_env().seed == 11
    assert load_env_file(path, override=True) == {"GEORISK_SEED": "19", "GEORISK_VESSEL_COUNT": "4"}
    assert GeoRiskConfig.from_env().seed == 19


def test_env_file_ignores_comments_and_quotes(tmp_path, monkeypatch):
    path = tmp_path / ".env"
    path.write_text('\n # comment\n A = "hello world"\nB=\'x=y#literal\'\nC=\n', encoding="utf-8")
    monkeypatch.setattr(os, "environ", {})
    assert load_env_file(path) == {"A": "hello world", "B": "x=y#literal", "C": ""}


def test_env_file_rejects_malformed_assignment_without_partial_application(tmp_path, monkeypatch):
    path = tmp_path / ".env"
    path.write_text("GEORISK_SEED=99\ninvalid assignment\n")
    monkeypatch.setattr(os, "environ", {})
    with pytest.raises(ValueError, match=":2"):
        load_env_file(path)
    assert "GEORISK_SEED" not in os.environ


def test_legacy_clis_load_selected_env_file(tmp_path, monkeypatch, capsys):
    from georisk.pipeline import run_cli
    from georisk.operations import validate_cli

    path = tmp_path / "demo.env"
    report_path = tmp_path / "report.json"
    values = {"GEORISK_INPUT_DIR": str(tmp_path / "inputs"),
              "GEORISK_OUTPUT_DIR": str(tmp_path / "processed"),
              "GEORISK_LAKE_DIR": str(tmp_path / "lake"),
              "GEORISK_REPORT_PATH": str(report_path), "GEORISK_SEED": "19"}
    path.write_text("\n".join(f"{key}={value}" for key, value in values.items()))
    monkeypatch.setattr(os, "environ", {"GEORISK_ENV_FILE": str(path)})
    run_cli()
    assert json.loads(report_path.read_text())["config"]["seed"] == 19
    for key in values:
        os.environ.pop(key)
    validate_cli()
    assert os.environ["GEORISK_REPORT_PATH"] == str(report_path)
    assert '"accepted"' in capsys.readouterr().out


def test_config_rejects_invalid_values():
    with pytest.raises(ValueError, match="GEORISK_RESOLUTION"):
        GeoRiskConfig.from_env({"GEORISK_RESOLUTION": "16"})
    with pytest.raises(ValueError, match="GEORISK_ROUTE_RISK_THRESHOLD"):
        GeoRiskConfig.from_env({"GEORISK_ROUTE_RISK_THRESHOLD": "bad"})
    with pytest.raises(ValueError, match="GEORISK_ACCEPTANCE_REQUIRED"):
        GeoRiskConfig.from_env({"GEORISK_ACCEPTANCE_REQUIRED": "sometimes"})


def test_phase6_runs_all_local_stages_and_writes_report(tmp_path):
    config = GeoRiskConfig(
        density_window_minutes=5,
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

    assert report["config"]["coverage_mode"] == "overlap"
    assert report["config"]["window_minutes"] == 5
    density = [json.loads(line) for line in (config.processed_dir / "density-metrics.jsonl").read_text().splitlines()]
    steps = config.scenario_duration_minutes * 60 // config.telemetry_interval_seconds
    expected_count = config.vessel_count * steps
    expected_windows = len({step * config.telemetry_interval_seconds // (config.density_window_minutes * 60)
                            for step in range(steps)})
    assert len({row["window_start"] for row in density}) == expected_windows
    assert report["status"] == "success"
    assert report["generation"]["generated"] == expected_count + 2
    assert report["processing"]["risk_event_count"] == expected_count
    assert report["processing"]["density_metric_count"] > 0
    assert report["processing"]["quarantined_count"] == 0
    assert report["processing"]["stats"]["telemetry_in"] == expected_count
    assert (config.processed_dir / "quarantine.jsonl").exists()
    assert "quarantine.jsonl" not in json.dumps(report["artifacts"])
    assert "run-metadata.json" not in json.dumps(report["artifacts"])
    metadata = json.loads((config.processed_dir / "run-metadata.json").read_text())
    assert report["processing"]["metadata"] == metadata
    assert metadata["window_minutes"] == 5
    assert report["model"]["evaluation_count"] > 0
    assert report["model"]["model_metrics"]["mae"] <= report["model"]["baseline_metrics"]["mae"]
    assert report["route"]["route_changed"] is True
    assert report["route"]["baseline"]["travel_hours"] == pytest.approx(6.47)
    assert report["route"]["alternative"]["travel_hours"] == pytest.approx(8.88)
    assert report["route"]["alternative"]["blocked_edges"]
    assert report["route"]["explanation"]
    assert report["route"]["at_time"] == metadata["latest_event_time"]
    assert report["route"]["edge_scores"]["port_a"][0]["risk_detail"]["alert_ids"] == ["H1"]
    assert report["route"]["edge_scores"]["port_a"][1]["risk_detail"]["alert_ids"] == []
    assert report["dashboard"]["has_data"] is True
    saved = json.loads(config.report_path.read_text(encoding="utf-8"))
    assert saved["status"] == "success"


def test_strict_validation_configuration():
    assert GeoRiskConfig.from_env({}).strict_validation is False
    assert GeoRiskConfig.from_env({"GEORISK_STRICT_VALIDATION": "true"}).strict_validation is True
    with pytest.raises(ValueError, match="GEORISK_STRICT_VALIDATION"):
        GeoRiskConfig.from_env({"GEORISK_STRICT_VALIDATION": "sometimes"})
    with pytest.raises(ValueError, match="GEORISK_STRICT_VALIDATION"):
        GeoRiskConfig(strict_validation="false")


def test_aws_foundation_template_is_safe_and_complete(project_root):
    path = project_root / "infra" / "aws" / "georisk-foundation.template.json"
    template = json.loads(path.read_text(encoding="utf-8"))
    resources = template["Resources"]
    assert {"DataBucket", "TelemetryStream", "WeatherAlertStream", "GlueDatabase", "AthenaWorkGroup"} <= set(resources)
    bucket = resources["DataBucket"]["Properties"]
    assert bucket["BucketEncryption"]["ServerSideEncryptionConfiguration"][0]["ServerSideEncryptionByDefault"]["SSEAlgorithm"] == "AES256"
    assert bucket["PublicAccessBlockConfiguration"]["BlockPublicPolicy"] is True
    assert resources["TelemetryStream"]["Properties"]["ShardCount"] == 1
    assert resources["WeatherAlertStream"]["Properties"]["RetentionPeriodHours"] == 24


def test_config_rejects_window_not_dividing_day():
    with pytest.raises(ValueError, match="GEORISK_DENSITY_WINDOW_MINUTES"):
        GeoRiskConfig.from_env({"GEORISK_DENSITY_WINDOW_MINUTES": "7"})


@pytest.mark.parametrize("name", ["GEORISK_SCENARIO_DURATION_MINUTES", "GEORISK_TELEMETRY_INTERVAL_SECONDS"])
@pytest.mark.parametrize("value", ["0", "-1", "1.5"])
def test_config_rejects_invalid_scenario_settings(name, value):
    with pytest.raises(ValueError, match=name):
        GeoRiskConfig.from_env({name: value})


def test_pipeline_keeps_inner_route_after_hazards_expire(tmp_path):
    config = GeoRiskConfig(
        vessel_count=6, scenario_duration_minutes=180,
        input_dir=tmp_path / "data", processed_dir=tmp_path / "processed",
        lake_dir=tmp_path / "lake", dashboard_dir=tmp_path,
        report_path=tmp_path / "report.json",
    )
    report = run_end_to_end(config)
    assert report["route"]["at_time"] == "2026-01-01T14:55:00Z"
    assert report["route"]["route_changed"] is False
    assert report["route"]["baseline_blocked_edges"] == []
    assert all(edge["risk_detail"]["alert_ids"] == []
               for edges in report["route"]["edge_scores"].values() for edge in edges)

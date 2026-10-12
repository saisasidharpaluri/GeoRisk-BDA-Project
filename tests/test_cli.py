from __future__ import annotations

import json
import os
from pathlib import Path
import sys
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from georisk import cli


@pytest.fixture(autouse=True)
def isolated_environment(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    for key in tuple(os.environ):
        if key.startswith("GEORISK_"):
            monkeypatch.delenv(key)


def test_help_lists_subcommands(capsys, monkeypatch):
    monkeypatch.setattr(cli, "_spark_available", lambda: False)
    with pytest.raises(SystemExit) as exc:
        cli.main(["--help"])
    assert exc.value.code == 0
    output = capsys.readouterr().out
    for command in ("generate", "replay", "process", "run", "validate", "compare-resolutions", "dashboard"):
        assert command in output
    assert "spark-stream" not in output


def test_run_and_validate_exit_codes(tmp_path, capsys):
    report_path = tmp_path / "report.json"
    args = ["run", "--input-dir", str(tmp_path / "inputs"),
            "--processed-dir", str(tmp_path / "processed"), "--lake-dir", str(tmp_path / "lake"),
            "--report-path", str(report_path)]
    assert cli.main(args) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["acceptance"]["passed"]
    assert cli.main(["validate", "--report-path", str(report_path)]) == 0
    capsys.readouterr()
    artifact = Path(report["lake"]["paths"]["risk_events"][0])
    with artifact.open("ab") as target:
        target.write(b"changed")
    assert cli.main(["validate", "--report", str(report_path)]) == 1
    checks = json.loads(capsys.readouterr().out)["checks"]
    assert next(item for item in checks if item["name"] == "artifact_integrity")["passed"] is False
    assert cli.main(["validate", "--report", str(tmp_path / "missing.json")]) == 1


def test_generate_writes_files(tmp_path):
    destination = tmp_path / "fixtures"
    assert cli.main(["generate", "--output-dir", str(destination), "--vessel-count", "3",
                     "--duration-minutes", "15", "--interval-seconds", "300"]) == 0
    telemetry = (destination / "vessel-telemetry.jsonl").read_text().splitlines()
    assert len(telemetry) == 9
    assert len((destination / "weather-alerts.jsonl").read_text().splitlines()) == 2


def test_flags_override_shell_and_env_file(tmp_path, monkeypatch, capsys):
    path = tmp_path / "custom.env"
    path.write_text("GEORISK_VESSEL_COUNT=2\nGEORISK_SCENARIO_DURATION_MINUTES=10\n")
    monkeypatch.setenv("GEORISK_VESSEL_COUNT", "invalid-but-overridden")
    args = ["--env-file", str(path), "generate", "--vessel-count", "4", "--input-dir", str(tmp_path / "data")]
    assert cli.main(args) == 0
    assert json.loads(capsys.readouterr().out)["generated"] == 10
    assert os.environ["GEORISK_VESSEL_COUNT"] == "invalid-but-overridden"
    monkeypatch.setenv("GEORISK_VESSEL_COUNT", "3")
    assert cli.main(["generate", "--env-file", str(path)]) == 0
    assert json.loads(capsys.readouterr().out)["generated"] == 8


def test_process_and_replay_use_configured_settings(tmp_path, monkeypatch, capsys):
    from georisk.ingestion import generator

    inputs = tmp_path / "inputs"
    processed = tmp_path / "processed"
    stream = tmp_path / "stream"
    assert cli.main(["generate", "--input-dir", str(inputs), "--vessels", "2", "--duration-minutes", "15"]) == 0
    capsys.readouterr()
    assert cli.main(["process", "--input-dir", str(inputs), "--output-dir", str(processed),
                     "--resolution", "8", "--coverage-mode", "center", "--window-minutes", "5",
                     "--allowed-lateness-minutes", "0", "--strict-validation"]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["metadata"]["h3_resolution"] == 8
    assert result["metadata"]["coverage_mode"] == "center"
    assert result["metadata"]["window_minutes"] == 5
    assert result["metadata"]["allowed_lateness_minutes"] == 0
    assert len((processed / "risk-events.jsonl").read_text().splitlines()) == 6
    delays = []
    real_replay = generator.replay_events

    def replay(events, publish, **kwargs):
        assert kwargs["speed_factor"] == 120
        return real_replay(events, publish, sleep=delays.append, **kwargs)

    monkeypatch.setattr(generator, "replay_events", replay)
    assert cli.main(["replay", "--input-dir", str(inputs), "--stream-dir", str(stream), "--speed", "120"]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result == {"generated": 8, "published": 8, "rejected": 0}
    assert delays and 2.5 in delays
    assert sum(len(path.read_text().splitlines()) for path in stream.rglob("*.jsonl")) == 8


def test_compare_resolutions_writes_report(tmp_path, capsys):
    assert cli.main(["compare-resolutions", "--report-path", str(tmp_path / "reports" / "run.json"),
                     "--vessels", "2", "--duration-minutes", "15"]) == 0
    rows = json.loads(capsys.readouterr().out)
    assert [row["resolution"] for row in rows] == [7, 8]
    assert json.loads((tmp_path / "reports" / "resolution-comparison.json").read_text()) == rows


def test_dashboard_launches_current_python_with_flag_overrides(tmp_path, monkeypatch):
    calls = []

    def run(command, **kwargs):
        calls.append((command, kwargs))
        return SimpleNamespace(returncode=7)

    monkeypatch.setattr(cli.subprocess, "run", run)
    monkeypatch.setenv("GEORISK_ROUTE_RISK_PENALTY", "2")
    assert cli.main(["dashboard", "--input-dir", str(tmp_path), "--route-risk-penalty", "9"]) == 7
    command, kwargs = calls[0]
    assert command[:4] == [sys.executable, "-m", "streamlit", "run"]
    assert Path(command[4]).is_file()
    assert kwargs["env"]["GEORISK_INPUT_DIR"] == str(tmp_path)
    assert kwargs["env"]["GEORISK_ROUTE_RISK_PENALTY"] == "9.0"
    assert Path(kwargs["env"]["STREAMLIT_THEME_BASE"]).name == "theme.toml"
    assert Path(kwargs["env"]["STREAMLIT_THEME_BASE"]).is_file()
    assert not kwargs.get("shell", False)


@pytest.mark.parametrize("watch", [False, True])
def test_optional_spark_command_wires_settings_and_stops_session(tmp_path, monkeypatch, watch):
    query, spark = Mock(), Mock()
    query.isActive = False
    run_stream = Mock(return_value=query)
    monkeypatch.setattr(cli, "_spark_available", lambda: True)
    monkeypatch.setitem(sys.modules, "georisk.processing.spark_stream_job", SimpleNamespace(run_stream=run_stream))
    monkeypatch.setitem(sys.modules, "georisk.spark_session", SimpleNamespace(get_spark=lambda name: spark))
    args = ["spark-stream", "--stream-dir", str(tmp_path / "stream"), "--output-dir", str(tmp_path / "lake"),
            "--checkpoint-dir", str(tmp_path / "checkpoint"), "--allowed-lateness-minutes", "20"]
    assert cli.main(args + (["--watch"] if watch else [])) == 0
    assert run_stream.call_args.args == (spark, tmp_path / "stream", tmp_path / "lake", tmp_path / "checkpoint")
    assert run_stream.call_args.kwargs["available_now"] is not watch
    assert run_stream.call_args.kwargs["allowed_lateness_minutes"] == 20
    query.awaitTermination.assert_called_once()
    spark.stop.assert_called_once()


def test_run_rejected_acceptance_exit_code_and_override(monkeypatch):
    from georisk import pipeline

    monkeypatch.setattr(pipeline, "run_end_to_end", lambda settings: {"acceptance": {"passed": False}})
    assert cli.main(["run"]) == 1
    assert cli.main(["run", "--no-acceptance-required"]) == 0


def test_invalid_configuration_returns_error(capsys):
    assert cli.main(["generate", "--duration-minutes", "-1"]) == 1
    assert "GEORISK_SCENARIO_DURATION_MINUTES" in capsys.readouterr().err

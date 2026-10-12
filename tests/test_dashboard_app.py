from pathlib import Path

import pytest

pytest.importorskip("streamlit")
try:  # App Control on some machines blocks numpy's random DLL, which pydeck needs.
    import numpy  # noqa: F401
    import pydeck  # noqa: F401
except Exception as exc:  # pragma: no cover - depends on the host
    pytest.skip(f"numpy/pydeck unavailable: {exc}", allow_module_level=True)

from streamlit.testing.v1 import AppTest  # noqa: E402

import georisk.route_dashboard.dashboard as dashboard_module  # noqa: E402
from georisk.config import GeoRiskConfig  # noqa: E402
from georisk.pipeline import run_end_to_end  # noqa: E402

DASHBOARD = str(Path(dashboard_module.__file__).resolve())
PANELS = ("Live map", "Risk summary", "Corridor density", "Delay estimates",
          "Route comparison", "Data freshness")


@pytest.fixture(scope="module")
def pipeline_dirs(tmp_path_factory):
    root = tmp_path_factory.mktemp("dashboard")
    config = GeoRiskConfig(input_dir=root / "data", processed_dir=root / "processed",
                           lake_dir=root / "lake", dashboard_dir=root,
                           report_path=root / "reports" / "run.json")
    run_end_to_end(config)
    return config


def _app(monkeypatch, *, input_dir, processed_dir, lake_dir) -> AppTest:
    monkeypatch.setenv("GEORISK_INPUT_DIR", str(input_dir))
    monkeypatch.setenv("GEORISK_OUTPUT_DIR", str(processed_dir))
    monkeypatch.setenv("GEORISK_LAKE_DIR", str(lake_dir))
    monkeypatch.setenv("GEORISK_ROUTE_RISK_THRESHOLD", "0.5")
    monkeypatch.setenv("GEORISK_STALE_AFTER_MINUTES", "30")
    return AppTest.from_file(DASHBOARD, default_timeout=60).run()


@pytest.fixture
def app(pipeline_dirs, monkeypatch):
    return _app(monkeypatch, input_dir=pipeline_dirs.input_dir,
                processed_dir=pipeline_dirs.processed_dir, lake_dir=pipeline_dirs.lake_dir)


def _table(at, column):
    return next(frame.value for frame in at.dataframe if column in frame.value.columns)


def test_dashboard_renders_without_exceptions(app):
    assert not app.exception


def test_dashboard_shows_all_panels(app):
    subheaders = {element.value for element in app.subheader}
    assert set(PANELS) <= subheaders
    assert any("Fresh" in element.value for element in app.success)
    assert [expander.label for expander in app.expander if expander.label == "History"]


def test_dashboard_no_data_state(tmp_path, monkeypatch):
    at = _app(monkeypatch, input_dir=tmp_path / "in", processed_dir=tmp_path / "out",
              lake_dir=tmp_path / "lake")
    assert not at.exception
    assert any("No vessel telemetry" in element.value for element in at.warning)
    assert any("Unavailable" in element.value for element in at.error)
    infos = " ".join(element.value for element in at.info)
    for message in ("No risk events yet", "No density metrics yet", "No delay estimates yet",
                    "no curated risk or density data"):
        assert message in infos


def test_threshold_one_keeps_baseline(app):
    app.slider(key="route_threshold").set_value(1.0).run()
    assert not app.exception
    assert any("within the risk threshold" in element.value for element in app.success)


def test_default_threshold_explains_detour(app):
    assert any("were unavailable" in element.value for element in app.warning)


def test_scenario_switch_recalculates_route(app):
    app.selectbox(key="route_scenario").select("No hazards").run()
    assert any("within the risk threshold" in element.value for element in app.success)
    lanes = _table(app, "used_by")
    assert set(lanes["status"]) == {"open"}
    app.selectbox(key="route_scenario").select("Close inner lane").run()
    lanes = _table(app, "used_by")
    assert "WHAT-IF-CLOSURE" in " ".join(lanes["risk_detail"])
    assert "blocked" in set(lanes["status"])


def test_risk_filters_change_table(app):
    before = len(_table(app, "max_severity"))
    app.slider(key="risk_severity").set_value(5).run()
    assert not app.exception
    frames = [frame.value for frame in app.dataframe if "max_severity" in frame.value.columns]
    after = len(frames[0]) if frames else 0
    assert after < before


def test_density_window_selection_and_comparison(app):
    slider = app.select_slider(key="density_window")
    first = slider.options[0]
    slider.set_value(first).run()
    assert not app.exception
    comparison = _table(app, "previous")
    assert comparison["previous"].isna().all()


def test_delay_demo_mode_toggle(app):
    labels = [metric.label for metric in app.metric]
    assert any(label.startswith("Model MAE (h)") for label in labels)
    assert "Baseline MAE (h)" in labels
    assert len(_table(app, "error_hours")) > 0
    app.toggle(key="delay_truth").set_value(True).run()
    assert not app.exception
    assert any("Synthetic demonstration, not a real-world forecast." == element.value
               for element in app.caption)


def test_layer_toggles_and_auto_refresh(app):
    for key in ("layer_vessels", "layer_hazards", "layer_risk_cells", "layer_density", "layer_route"):
        app.checkbox(key=key).uncheck()
    app.toggle(key="auto_refresh").set_value(True).run()
    assert not app.exception

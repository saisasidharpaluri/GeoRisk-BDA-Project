from __future__ import annotations

import json

import pytest

pytest.importorskip("pyspark")
pytestmark = pytest.mark.spark

from pyspark.ml import PipelineModel

from georisk.config import GeoRiskConfig
from georisk.geo_demo import DEMO_NODES
from georisk.ingestion.scenario import ScenarioConfig, generate_scenario
from georisk.pipeline import run_end_to_end
from georisk.processing.stream_job import process_events
from georisk.storage_analytics.delay_features import build_delay_rows
from georisk.storage_analytics.delay_model import FEATURES, split_by_time, train_and_evaluate
from georisk.storage_analytics.spark_delay_model import (
    predict_gbt, save_gbt_model, train_and_evaluate_gbt,
)


@pytest.fixture(scope="module")
def fitted_gbt(spark):
    scenario = generate_scenario(ScenarioConfig(seed=7, vessel_count=20))
    processed = process_events(scenario["telemetry"], scenario["weather_alerts"])
    rows = build_delay_rows(
        scenario["telemetry"], processed["risk_events"], processed["density_metrics"],
        destination=DEMO_NODES["port_b"], window_minutes=15, seed=7,
    )
    training, evaluation = split_by_time(rows)
    return train_and_evaluate_gbt(spark, training, evaluation), training, evaluation


def test_gbt_beats_mean_baseline_on_delay_rows(fitted_gbt):
    result, _, _ = fitted_gbt
    assert result["model_metrics"]["mae"] < result["baseline_metrics"]["mae"]
    assert result["model_metrics"]["rmse"] < result["baseline_metrics"]["rmse"]


def test_gbt_result_shape_matches_linear(fitted_gbt):
    result, training, evaluation = fitted_gbt
    assert set(train_and_evaluate(training, evaluation)) <= set(result)
    assert result["model_name"] == "gbt"
    assert result["coefficients"] is None
    assert result["features"] == list(FEATURES)
    assert len(result["predictions"]) == result["evaluation_count"] == len(evaluation)
    mean = sum(row["delay_hours"] for row in training) / len(training)
    assert result["baseline_predictions"] == [mean] * len(evaluation)
    errors = [prediction - row["delay_hours"] for prediction, row in zip(result["predictions"], evaluation)]
    assert result["model_metrics"]["mae"] == pytest.approx(sum(abs(error) for error in errors) / len(errors))


def test_gbt_saved_pipeline_preserves_prediction_order(spark, fitted_gbt, tmp_path):
    result, _, evaluation = fitted_gbt
    path = tmp_path / "model"
    save_gbt_model(result["model"], path)
    restored = PipelineModel.load(str(path))
    features = [{name: row[name] for name in FEATURES} for row in reversed(evaluation)]
    assert predict_gbt(spark, restored, features) == list(reversed(result["predictions"]))


def test_gbt_pipeline_writes_estimates_model_and_report(spark, tmp_path):
    settings = GeoRiskConfig(
        delay_model="gbt", input_dir=tmp_path / "data", processed_dir=tmp_path / "processed",
        lake_dir=tmp_path / "lake", dashboard_dir=tmp_path, report_path=tmp_path / "report.json",
    )
    report = run_end_to_end(settings)
    assert report["model"]["model_name"] == "gbt"
    assert report["acceptance"]["passed"]
    rows = [json.loads(line) for line in (settings.processed_dir / "delay-estimates.jsonl").read_text().splitlines()]
    assert {row["model_name"] for row in rows} == {"gbt"}
    restored = PipelineModel.load(str(settings.processed_dir / "delay-model-gbt"))
    assert predict_gbt(spark, restored, rows) == [row["predicted_delay_hours"] for row in rows]
    assert any("delay-model-gbt" in entry["path"] for entry in report["artifacts"])


def test_gbt_rejects_empty_or_invalid_training_data(spark):
    with pytest.raises(ValueError, match="cannot be empty"):
        train_and_evaluate_gbt(spark, [], [])
    with pytest.raises(ValueError, match="max_iter"):
        train_and_evaluate_gbt(spark, [], [], max_iter=0)

"""Optional MLlib gradient-boosted delay regression for synthetic data."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from georisk.storage_analytics.delay_model import FEATURES, LABEL, _validate_rows


def _frame(spark: SparkSession, rows: list[dict[str, Any]], *, labeled: bool) -> DataFrame:
    """Keep an explicit row index so predictions retain the caller's ordering."""
    names = (*FEATURES, LABEL) if labeled else FEATURES
    schema = T.StructType(
        [T.StructField("_row_index", T.LongType(), False)]
        + [T.StructField(name, T.DoubleType(), False) for name in names]
    )
    return spark.createDataFrame(
        [(index, *(float(row[name]) for name in names)) for index, row in enumerate(rows)], schema
    )


def predict_gbt(
    spark: SparkSession,
    model: PipelineModel,
    rows: Iterable[Mapping[str, Any]],
) -> list[float]:
    """Predict from features only, preserving input order."""
    records = [dict(row) for row in rows]
    if not records:
        return []
    _validate_rows(records, require_label=False)
    return [
        float(row.prediction)
        for row in model.transform(
            _frame(spark, records, labeled=False),
        )
        .orderBy("_row_index")
        .select("prediction")
        .collect()
    ]


def train_and_evaluate_gbt(
    spark: SparkSession,
    training_rows: Iterable[Mapping[str, Any]],
    evaluation_rows: Iterable[Mapping[str, Any]],
    *,
    seed: int = 7,
    max_iter: int = 50,
    max_depth: int = 4,
) -> dict[str, Any]:
    """Fit on training rows and compare held-out predictions to their training mean."""
    for name, value in (("seed", seed), ("max_iter", max_iter), ("max_depth", max_depth)):
        if not isinstance(value, int) or isinstance(value, bool):
            raise ValueError(f"{name} must be an integer")
    if max_iter < 1 or not 1 <= max_depth <= 30:
        raise ValueError("max_iter must be positive and max_depth must be 1..30")
    training, evaluation = (
        [dict(row) for row in training_rows],
        [dict(row) for row in evaluation_rows],
    )
    _validate_rows(training)
    _validate_rows(evaluation)
    pipeline = Pipeline(
        stages=[
            VectorAssembler(inputCols=list(FEATURES), outputCol="features"),
            GBTRegressor(
                labelCol=LABEL,
                featuresCol="features",
                seed=seed,
                maxIter=max_iter,
                maxDepth=max_depth,
            ),
        ]
    )
    model = pipeline.fit(_frame(spark, training, labeled=True))
    predicted = model.transform(_frame(spark, evaluation, labeled=True)).cache()
    mean = sum(float(row[LABEL]) for row in training) / len(training)
    try:
        predictions = [
            float(row.prediction)
            for row in predicted.orderBy("_row_index").select("prediction").collect()
        ]
        baseline = predicted.withColumn("prediction", F.lit(mean))
        model_metrics, baseline_metrics = {}, {}
        for metric in ("mae", "rmse"):
            evaluator = RegressionEvaluator(
                labelCol=LABEL, predictionCol="prediction", metricName=metric
            )
            model_metrics[metric] = evaluator.evaluate(predicted)
            baseline_metrics[metric] = evaluator.evaluate(baseline)
    finally:
        predicted.unpersist()
    return {
        "features": list(FEATURES),
        "coefficients": None,
        "model_metrics": model_metrics,
        "baseline_metrics": baseline_metrics,
        "predictions": predictions,
        "baseline_predictions": [mean] * len(evaluation),
        "evaluation_count": len(evaluation),
        "model_name": "gbt",
        "model": model,
    }


def save_gbt_model(model: PipelineModel, path: str | Path) -> None:
    """Persist the assembler and trees together for identical future predictions."""
    if not str(path):
        raise ValueError("path must be non-empty")
    model.write().overwrite().save(str(path))

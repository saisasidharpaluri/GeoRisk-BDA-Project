"""Deterministic delay regression and evaluation for synthetic history."""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime
from typing import Any

FEATURES = (
    "baseline_hours",
    "density",
    "hazard_severity",
    "speed_knots",
    "route_distance_nm",
)
LABEL = "delay_hours"


def make_synthetic_label(row: Mapping[str, Any], noise_hours: float = 0.0) -> float:
    """Create the documented synthetic delay label used by the demo."""
    return max(
        0.0,
        0.05 * float(row["baseline_hours"])
        + 0.015 * float(row["density"])
        + 0.45 * float(row["hazard_severity"])
        + 0.002 * float(row["route_distance_nm"])
        - 0.01 * float(row["speed_knots"])
        + noise_hours,
    )


def split_by_time(
    records: Iterable[Mapping[str, Any]], evaluation_fraction: float = 0.2
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Split records chronologically, keeping later records for evaluation."""
    if not 0 < evaluation_fraction < 1:
        raise ValueError("evaluation_fraction must be between 0 and 1")
    materialized = [dict(record) for record in records]
    if len(materialized) < 2:
        raise ValueError("at least two records are required")
    if "event_time" not in materialized[0]:
        raise ValueError("records require event_time for a time-based split")
    ordered = sorted(materialized, key=lambda row: _time_key(row["event_time"]))
    evaluation_count = max(1, math.ceil(len(ordered) * evaluation_fraction))
    return ordered[:-evaluation_count], ordered[-evaluation_count:]


def _time_key(value: Any) -> datetime | str:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    raise ValueError("event_time must be a datetime or ISO timestamp")


def _matrix(records: Sequence[Mapping[str, Any]]) -> list[list[float]]:
    return [[1.0] + [float(record[name]) for name in FEATURES] for record in records]


def _validate_rows(records: Sequence[Mapping[str, Any]], *, require_label: bool = True) -> None:
    if not records:
        raise ValueError("training and evaluation data cannot be empty")
    for record in records:
        fields = (*FEATURES, LABEL) if require_label else FEATURES
        missing = [name for name in fields if name not in record]
        if missing:
            raise ValueError(f"record is missing required fields: {', '.join(missing)}")
        values = [float(record[name]) for name in fields]
        if not all(math.isfinite(value) for value in values):
            raise ValueError("features and labels must be finite numbers")


def _solve_ridge(
    matrix: list[list[float]], labels: list[float], penalty: float = 1e-8
) -> list[float]:
    size = len(matrix[0])
    normal = [[0.0] * size for _ in range(size)]
    vector = [0.0] * size
    for features, label in zip(matrix, labels, strict=False):
        for i in range(size):
            vector[i] += features[i] * label
            for j in range(size):
                normal[i][j] += features[i] * features[j]
    for index in range(1, size):
        normal[index][index] += penalty
    for pivot in range(size):
        pivot_row = max(range(pivot, size), key=lambda row: abs(normal[row][pivot]))
        if abs(normal[pivot_row][pivot]) < 1e-12:
            raise ValueError("training data does not contain enough feature variation")
        normal[pivot], normal[pivot_row] = normal[pivot_row], normal[pivot]
        vector[pivot], vector[pivot_row] = vector[pivot_row], vector[pivot]
        divisor = normal[pivot][pivot]
        normal[pivot] = [value / divisor for value in normal[pivot]]
        vector[pivot] /= divisor
        for row in range(size):
            if row == pivot:
                continue
            factor = normal[row][pivot]
            normal[row] = [
                left - factor * right
                for left, right in zip(normal[row], normal[pivot], strict=False)
            ]
            vector[row] -= factor * vector[pivot]
    return vector


def predict(
    coefficients: Iterable[float],
    rows: Iterable[Mapping[str, Any]],
) -> list[float]:
    """Predict from an intercept followed by FEATURES coefficients; labels are optional."""
    weights = [float(value) for value in coefficients]
    if len(weights) != len(FEATURES) + 1:
        raise ValueError(f"coefficients must contain an intercept and {len(FEATURES)} weights")
    if not all(math.isfinite(value) for value in weights):
        raise ValueError("coefficients must be finite numbers")
    records = list(rows)
    if not records:
        return []
    _validate_rows(records, require_label=False)
    return [sum(a * b for a, b in zip(row, weights, strict=False)) for row in _matrix(records)]


def _metrics(actual: list[float], predicted: list[float]) -> dict[str, float]:
    errors = [prediction - target for target, prediction in zip(actual, predicted, strict=False)]
    return {
        "mae": sum(abs(error) for error in errors) / len(errors),
        "rmse": math.sqrt(sum(error * error for error in errors) / len(errors)),
    }


def train_and_evaluate(
    training_data: Iterable[Mapping[str, Any]],
    evaluation_data: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    """Fit a regression model and compare it with a training-mean baseline."""
    training = [dict(row) for row in training_data]
    evaluation = [dict(row) for row in evaluation_data]
    _validate_rows(training)
    _validate_rows(evaluation)
    coefficients = _solve_ridge(_matrix(training), [float(row[LABEL]) for row in training])
    predictions = predict(coefficients, evaluation)
    actual = [float(row[LABEL]) for row in evaluation]
    mean_prediction = sum(float(row[LABEL]) for row in training) / len(training)
    baseline_predictions = [mean_prediction] * len(evaluation)
    return {
        "features": list(FEATURES),
        "coefficients": coefficients,
        "model_metrics": _metrics(actual, predictions),
        "baseline_metrics": _metrics(actual, baseline_predictions),
        "predictions": predictions,
        "baseline_predictions": baseline_predictions,
        "evaluation_count": len(evaluation),
    }

from datetime import datetime, timedelta, timezone

import pytest

from georisk.storage_analytics.delay_model import (
    FEATURES,
    make_synthetic_label,
    split_by_time,
    train_and_evaluate,
)
from georisk.storage_analytics.lake import density_summary, read_dataset, risk_summary, write_dataset

UTC = timezone.utc


def _risk(index: int, alerted: bool = True) -> dict:
    return {
        "schema_version": 1,
        "event_time": f"2026-01-01T12:{index:02d}:00Z",
        "vessel_id": f"vessel-{index % 2}",
        "h3_index": "87754e649ffffff",
        "h3_resolution": 7,
        "risk_alert": alerted,
        "matching_hazards": (
            [{"alert_id": "alert-1", "severity": 4}] if alerted else []
        ),
    }


def _density(index: int) -> dict:
    return {
        "schema_version": 1,
        "window_start": f"2026-01-01T12:{index:02d}:00Z",
        "window_end": f"2026-01-01T12:{index + 15:02d}:00Z",
        "h3_index": "87754e649ffffff",
        "h3_resolution": 7,
        "unique_vessel_count": index + 1,
    }


def _delay_row(index: int) -> dict:
    row = {
        "event_time": (datetime(2026, 1, 1, tzinfo=UTC) + timedelta(hours=index)).isoformat(),
        "baseline_hours": 10 + index,
        "density": 2 + index,
        "hazard_severity": index % 5 + 1,
        "speed_knots": 12,
        "route_distance_nm": 400 + index * 10,
    }
    row["delay_hours"] = make_synthetic_label(row)
    return row


def test_write_and_read_risk_parquet_with_date_partition(tmp_path):
    output = write_dataset(
        [_risk(0), _risk(1, alerted=False)],
        str(tmp_path / "lake"),
        "2026-01-01",
    )

    assert "risk_events" in output
    assert "event_date=2026-01-01" in output
    rows = read_dataset(tmp_path / "lake", "risk_events")
    assert len(rows) == 2
    assert rows[0]["matching_hazards"][0]["alert_id"] == "alert-1"


def test_density_summary_and_risk_summary(tmp_path):
    lake = tmp_path / "lake"
    write_dataset([_risk(0), _risk(1, False)], lake.as_posix(), "2026-01-01")
    write_dataset([_density(0), _density(1)], lake.as_posix(), "2026-01-01")

    assert risk_summary(lake) == {
        "risk_event_count": 2,
        "alerted_event_count": 1,
        "vessel_count": 2,
        "exposed_vessel_count": 1,
    }
    assert [row["unique_vessel_count"] for row in density_summary(lake)] == [1, 2]


def test_lake_rejects_invalid_records_and_dates(tmp_path):
    with pytest.raises(ValueError, match="YYYY-MM-DD"):
        write_dataset([_risk(0)], str(tmp_path), "01-01-2026")
    invalid = _risk(0)
    invalid["h3_resolution"] = 16
    with pytest.raises(ValueError):
        write_dataset([invalid], str(tmp_path), "2026-01-01")


def test_time_split_is_chronological_and_non_empty():
    training, evaluation = split_by_time([_delay_row(i) for i in range(10)], 0.2)
    assert len(training) == 8
    assert len(evaluation) == 2
    assert training[-1]["event_time"] < evaluation[0]["event_time"]


def test_delay_model_beats_mean_baseline_on_synthetic_data():
    rows = [_delay_row(i) for i in range(30)]
    result = train_and_evaluate(rows[:20], rows[20:])

    assert result["features"] == list(FEATURES)
    assert result["evaluation_count"] == 10
    assert result["model_metrics"]["mae"] < result["baseline_metrics"]["mae"]
    assert result["model_metrics"]["rmse"] < result["baseline_metrics"]["rmse"]


def test_delay_model_rejects_missing_features():
    row = _delay_row(0)
    del row["density"]
    with pytest.raises(ValueError, match="density"):
        train_and_evaluate([_delay_row(1)], [row])

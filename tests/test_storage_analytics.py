from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from georisk.contracts import ContractValidationError, load_schema, validate_event
from georisk.config import GeoRiskConfig
from georisk.geo_demo import DEMO_NODES, haversine_nm
from georisk.ingestion.generator import generate_events
from georisk.ingestion.scenario import ScenarioConfig, generate_scenario
from georisk.processing.stream_job import process_events
from georisk.pipeline import run_end_to_end
from georisk.storage_analytics.delay_features import build_delay_rows
from georisk.storage_analytics.arrow_schemas import ARROW_SCHEMAS
from georisk.storage_analytics.lake import CONTRACT_BY_DATASET

from georisk.storage_analytics.delay_model import (
    FEATURES,
    make_synthetic_label,
    predict,
    split_by_time,
    train_and_evaluate,
)
from georisk.storage_analytics.lake import (
    density_summary, read_dataset, risk_summary, write_dataset, write_partitioned,
)

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


def _delay_estimate(index: int) -> dict:
    row = _delay_row(index)
    return {
        "schema_version": 1, "event_id": f"event-{index}",
        "vessel_id": f"vessel-{index}", "event_time": row["event_time"],
        "split": "evaluation", "model_name": "linear",
        "predicted_delay_hours": row["delay_hours"],
        "baseline_delay_hours": 1.0, "synthetic_label_hours": row["delay_hours"],
        **{feature: row[feature] for feature in FEATURES},
    }


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


@pytest.mark.parametrize("dataset", CONTRACT_BY_DATASET)
def test_batch_spanning_midnight_creates_two_partitions(tmp_path, dataset):
    events = generate_events(7, 2)
    records = {
        "vessel_telemetry": events["telemetry"][:2],
        "weather_alerts": events["weather_alerts"][:2],
        "risk_events": [_risk(0), _risk(1)],
        "density_metrics": [_density(0), _density(1)],
        "delay_estimates": [_delay_estimate(0), _delay_estimate(1)],
    }[dataset]
    timestamp_field = "window_start" if dataset == "density_metrics" else "event_time"
    for record, timestamp in zip(records, ["2026-01-01T23:50:00Z", "2026-01-02T00:00:00Z"]):
        record[timestamp_field] = timestamp
        if dataset == "density_metrics":
            record["window_end"] = (
                datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                + timedelta(minutes=15)
            ).isoformat()

    paths = write_partitioned(reversed(records), tmp_path, dataset_name=dataset)

    assert [Path(path).parent.name for path in paths] == [
        "event_date=2026-01-01", "event_date=2026-01-02",
    ]
    assert all(Path(path).is_file() for path in paths)
    rows = read_dataset(tmp_path, dataset)
    assert [(row[timestamp_field], row["event_date"]) for row in rows] == [
        ("2026-01-01T23:50:00Z", "2026-01-01"),
        ("2026-01-02T00:00:00Z", "2026-01-02"),
    ]


def test_partitions_accept_explicit_utc_offset(tmp_path):
    records = [
        {**_risk(0), "event_time": "2026-01-01T23:30:00+00:00"},
        {**_risk(1), "event_time": "2026-01-02T00:30:00+00:00"},
    ]
    write_partitioned(records, tmp_path, dataset_name="risk_events")
    assert {row["event_time"]: row["event_date"] for row in read_dataset(tmp_path, "risk_events")} == {
        records[0]["event_time"]: "2026-01-01",
        records[1]["event_time"]: "2026-01-02",
    }


def test_partitioned_batch_preserves_utc_only_contract(tmp_path):
    record = {**_risk(0), "event_time": "2026-01-02T00:30:00+05:30"}
    with pytest.raises(ValueError, match="event_time"):
        write_partitioned([record], tmp_path, dataset_name="risk_events")
    assert list(tmp_path.iterdir()) == []


def test_rewrite_same_batch_id_is_idempotent(tmp_path):
    records = [_risk(0), {**_risk(1), "event_time": "2026-01-02T12:00:00Z"}]
    paths = write_partitioned(records, tmp_path, dataset_name="risk_events", batch_id="batch-1")
    assert write_partitioned(
        records, tmp_path, dataset_name="risk_events", batch_id="batch-1",
    ) == paths
    assert len(read_dataset(tmp_path, "risk_events")) == 2

    write_partitioned([_risk(2)], tmp_path, dataset_name="risk_events", batch_id="batch-2")
    replacement = _risk(3, False)
    write_partitioned([replacement], tmp_path, dataset_name="risk_events", batch_id="batch-1")
    rows = read_dataset(tmp_path, "risk_events")
    assert {row["event_time"] for row in rows} == {
        replacement["event_time"], records[1]["event_time"], _risk(2)["event_time"],
    }
    assert len(rows) == 3
    assert len(list(tmp_path.rglob("*.parquet"))) == 3
    assert risk_summary(tmp_path)["alerted_event_count"] == 2


def test_raw_and_curated_zones(tmp_path):
    events = generate_events(7, 1)
    for dataset, records, zone in [
        ("vessel_telemetry", events["telemetry"], "raw"),
        ("weather_alerts", events["weather_alerts"], "raw"),
        ("risk_events", [_risk(0)], "curated"),
        ("density_metrics", [_density(0)], "curated"),
        ("delay_estimates", [_delay_estimate(0)], "curated"),
    ]:
        paths = write_partitioned(records, tmp_path, dataset_name=dataset)
        assert len(paths) == 1
        assert Path(paths[0]).relative_to(tmp_path).parts == (
            zone, dataset, "event_date=2026-01-01", "part-00000.parquet",
        )
        assert len(read_dataset(tmp_path, dataset)) == len(records)


def test_empty_partitioned_batch_writes_no_files(tmp_path):
    assert write_partitioned([], tmp_path, dataset_name="risk_events") == []
    assert list(tmp_path.iterdir()) == []
    assert read_dataset(tmp_path, "risk_events") == []


@pytest.mark.parametrize("batch_id", ["", "../escape", "nested/file", "nested\\file"])
def test_partitioned_batch_rejects_unsafe_ids(tmp_path, batch_id):
    with pytest.raises(ValueError, match="batch_id"):
        write_partitioned([_risk(0)], tmp_path, dataset_name="risk_events", batch_id=batch_id)
    assert list(tmp_path.iterdir()) == []


def test_partitioned_batch_validates_all_rows_before_writing(tmp_path):
    invalid = {**_risk(1), "event_time": "invalid"}
    with pytest.raises(ValueError):
        write_partitioned([_risk(0), invalid], tmp_path, dataset_name="risk_events")
    assert list(tmp_path.iterdir()) == []


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


@pytest.fixture(scope="module")
def delay_inputs():
    events = generate_scenario(ScenarioConfig(seed=7))
    processed = process_events(events["telemetry"], events["weather_alerts"])
    return events["telemetry"], processed["risk_events"], processed["density_metrics"]


def test_delay_features_vary(delay_inputs):
    rows = build_delay_rows(
        *delay_inputs, destination=DEMO_NODES["port_b"], window_minutes=15, seed=7,
    )
    for feature in ("speed_knots", "route_distance_nm", "density"):
        assert len({row[feature] for row in rows}) > 1
    positions = {event["event_id"]: event for event in delay_inputs[0]}
    for row in rows:
        position = positions[row["event_id"]]
        assert row["vessel_id"] == position["vessel_id"]
        assert row["event_time"] == position["event_time"]
        assert row["speed_knots"] == max(1.0, position["speed_knots"])
        assert row["route_distance_nm"] == haversine_nm(
            position["latitude"], position["longitude"], *DEMO_NODES["port_b"],
        )
        assert row["baseline_hours"] == row["route_distance_nm"] / row["speed_knots"]


@pytest.mark.parametrize("window_minutes", [5, 15, 60])
def test_density_feature_uses_event_window(window_minutes):
    times = [datetime(2026, 1, 1, 12, tzinfo=UTC) + timedelta(minutes=offset)
             for offset in (window_minutes - 1, window_minutes, 2 * window_minutes)]
    risks = [dict(_risk(0), event_id=f"event-{i}", event_time=time.isoformat())
             for i, time in enumerate(times)]
    telemetry = [dict(event, latitude=13.0, longitude=80.5, speed_knots=0)
                 for event in reversed(risks)]
    density = [dict(_density(0), unique_vessel_count=count,
                    window_start=(datetime(2026, 1, 1, 12, tzinfo=UTC)
                                  + timedelta(minutes=i * window_minutes)).isoformat())
               for i, count in enumerate((2, 9))]
    density.append(dict(density[-1], h3_index="another-cell", unique_vessel_count=99))
    rows = build_delay_rows(
        telemetry, risks, density, destination=DEMO_NODES["port_b"],
        window_minutes=window_minutes, seed=7, noise_std_hours=0,
    )
    assert [row["density"] for row in rows] == [2, 9, 0]
    assert all(row["speed_knots"] == 1.0 for row in rows)
    assert all(row["delay_hours"] == make_synthetic_label(row) for row in rows)


def test_delay_rows_deterministic_with_seed(delay_inputs):
    kwargs = dict(destination=DEMO_NODES["port_b"], window_minutes=15)
    first = build_delay_rows(*delay_inputs, seed=7, **kwargs)
    assert first == build_delay_rows(*delay_inputs, seed=7, **kwargs)
    second = build_delay_rows(*delay_inputs, seed=8, **kwargs)
    assert [row["delay_hours"] for row in first] != [row["delay_hours"] for row in second]
    assert [{key: row[key] for key in FEATURES} for row in first] == [
        {key: row[key] for key in FEATURES} for row in second
    ]


def test_model_beats_baseline_but_not_perfectly(delay_inputs):
    rows = build_delay_rows(
        *delay_inputs, destination=DEMO_NODES["port_b"], window_minutes=15, seed=7,
    )
    result = train_and_evaluate(*split_by_time(rows))
    assert 0.01 < result["model_metrics"]["mae"] < result["baseline_metrics"]["mae"]


def test_synthetic_label_adds_noise_and_clamps_at_zero():
    row = _delay_row(0)
    assert make_synthetic_label(row, 0.5) == pytest.approx(row["delay_hours"] + 0.5)
    assert make_synthetic_label(row, -100) == 0


def test_delay_rows_reject_missing_telemetry():
    with pytest.raises(ValueError, match="matching telemetry event_id"):
        build_delay_rows([], [_risk(0)], [], destination=DEMO_NODES["port_b"],
                         window_minutes=15, seed=7)


@pytest.mark.parametrize("value", [-1, float("nan"), float("inf"), True])
def test_delay_noise_validation(value):
    with pytest.raises(ValueError, match="GEORISK_DELAY_NOISE_STD_HOURS"):
        GeoRiskConfig(delay_noise_std_hours=value)
    with pytest.raises(ValueError, match="GEORISK_DELAY_NOISE_STD_HOURS"):
        GeoRiskConfig.from_env({"GEORISK_DELAY_NOISE_STD_HOURS": str(value)})
    with pytest.raises(ValueError, match="noise_std_hours"):
        build_delay_rows([], [], [], destination=DEMO_NODES["port_b"],
                         window_minutes=15, seed=7, noise_std_hours=value)


def test_delay_noise_setting_from_env():
    assert GeoRiskConfig.from_env({}).delay_noise_std_hours == 0.25
    assert GeoRiskConfig.from_env({"GEORISK_DELAY_NOISE_STD_HOURS": "0.5"}).delay_noise_std_hours == 0.5
    assert GeoRiskConfig.from_env({"GEORISK_DELAY_NOISE_STD_HOURS": "0"}).delay_noise_std_hours == 0


def test_predict_matches_train_and_evaluate_predictions():
    training, evaluation = split_by_time([_delay_row(i) for i in range(30)])
    model = train_and_evaluate(training, evaluation)
    features_only = [{feature: row[feature] for feature in FEATURES} for row in evaluation]
    assert predict(model["coefficients"], iter(features_only)) == model["predictions"]
    assert predict(model["coefficients"], []) == []


def test_predict_rejects_invalid_coefficients_and_features():
    with pytest.raises(ValueError, match="intercept"):
        predict([0.0], [_delay_row(0)])
    with pytest.raises(ValueError, match="finite"):
        predict([float("nan")] * 6, [_delay_row(0)])
    with pytest.raises(ValueError, match="speed_knots"):
        predict([0.0] * 6, [{}])
    with pytest.raises(ValueError, match="finite"):
        predict([0.0] * 6, [dict(_delay_row(0), density=float("inf"))])


def test_delay_estimates_written_and_valid(tmp_path):
    config = GeoRiskConfig(
        input_dir=tmp_path / "data", processed_dir=tmp_path / "processed",
        lake_dir=tmp_path / "lake", dashboard_dir=tmp_path,
        report_path=tmp_path / "report.json",
    )
    report = run_end_to_end(config)
    estimates_path = config.processed_dir / "delay-estimates.jsonl"
    model_path = config.processed_dir / "delay-model.json"
    estimates = [json.loads(line) for line in estimates_path.read_text().splitlines()]
    model = json.loads(model_path.read_text())
    assert len(estimates) == report["processing"]["risk_event_count"]
    assert len({row["event_id"] for row in estimates}) == len(estimates)
    for row in estimates:
        validate_event("delay_estimate", row)
    assert model["model_name"] == "linear"
    assert model["features"] == list(FEATURES)
    assert len(model["coefficients"]) == len(FEATURES) + 1
    assert predict(model["coefficients"], estimates) == [
        row["predicted_delay_hours"] for row in estimates
    ]
    training = [row for row in estimates if row["split"] == "train"]
    evaluation = [row for row in estimates if row["split"] == "evaluation"]
    assert len(evaluation) == report["model"]["evaluation_count"]
    assert max(row["event_time"] for row in training) <= min(row["event_time"] for row in evaluation)
    assert {row["event_id"] for row in training}.isdisjoint(row["event_id"] for row in evaluation)
    mean_label = sum(row["synthetic_label_hours"] for row in training) / len(training)
    assert all(row["baseline_delay_hours"] == mean_label for row in estimates)
    for name, field in (("model", "predicted_delay_hours"), ("baseline", "baseline_delay_hours")):
        errors = [row[field] - row["synthetic_label_hours"] for row in evaluation]
        assert report["model"][f"{name}_metrics"]["mae"] == pytest.approx(
            sum(abs(error) for error in errors) / len(errors)
        )
        assert report["model"][f"{name}_metrics"]["rmse"] == pytest.approx(
            (sum(error ** 2 for error in errors) / len(errors)) ** 0.5
        )
    stored = read_dataset(config.lake_dir, "delay_estimates")
    assert {row["event_id"]: {k: v for k, v in row.items() if k != "event_date"}
            for row in stored} == {row["event_id"]: row for row in estimates}
    paths = report["lake"]["paths"]["delay_estimates"]
    assert all("curated/delay_estimates/" in Path(path).as_posix() for path in paths)
    assert {str(estimates_path), str(model_path), *paths} <= {
        entry["path"] for entry in report["artifacts"]
    }
    assert report["config"]["delay_noise_std_hours"] == 0.25
    assert report["model"]["model_metrics"]["mae"] > 0.01
    assert report["acceptance"]["passed"]


@pytest.mark.parametrize("field,value", [
    ("split", "test"), ("event_id", ""), ("schema_version", 2),
    ("synthetic_label_hours", -1), ("density", -1), ("hazard_severity", 6),
    ("event_time", "2026-01-01T12:00:00+05:30"),
])
def test_delay_estimate_contract_rejects_invalid_values(field, value):
    with pytest.raises(ContractValidationError):
        validate_event("delay_estimate", dict(_delay_estimate(0), **{field: value}))


@pytest.mark.parametrize("field", load_schema("delay_estimate")["required"])
def test_delay_estimate_contract_requires_fields(field):
    row = _delay_estimate(0)
    del row[field]
    with pytest.raises(ContractValidationError, match=field):
        validate_event("delay_estimate", row)


def test_mixed_polygon_and_multipolygon_alerts_round_trip(tmp_path):
    alerts = generate_events(7, 0)["weather_alerts"]
    alerts[1]["geometry"] = {
        "type": "MultiPolygon", "coordinates": [alerts[1]["geometry"]["coordinates"]],
    }
    write_dataset(alerts, tmp_path, "2026-01-01")
    rows = read_dataset(tmp_path, "weather_alerts")
    assert [row["geometry"] for row in rows] == [alert["geometry"] for alert in alerts]
    assert all("geometry_json" not in row for row in rows)


def test_empty_and_alerted_risk_partitions_read_together(tmp_path):
    write_dataset([_risk(0, False)], tmp_path, "2026-01-02")
    write_dataset([_risk(1)], tmp_path, "2026-01-03")
    rows = read_dataset(tmp_path, "risk_events")
    assert len(rows) == 2
    assert {row["event_date"] for row in rows} == {"2026-01-02", "2026-01-03"}
    assert rows[0]["matching_hazards"] == []
    assert rows[1]["matching_hazards"][0]["alert_id"] == "alert-1"
    assert risk_summary(tmp_path) == {
        "risk_event_count": 2, "alerted_event_count": 1,
        "vessel_count": 2, "exposed_vessel_count": 1,
    }


@pytest.mark.parametrize("dataset,contract", CONTRACT_BY_DATASET.items())
def test_arrow_schemas_match_contract_properties(dataset, contract):
    expected = {"geometry_json" if key == "geometry" else key
                for key in load_schema(contract)["properties"]}
    assert set(ARROW_SCHEMAS[dataset].names) == expected
    assert all(field.nullable for field in ARROW_SCHEMAS[dataset])

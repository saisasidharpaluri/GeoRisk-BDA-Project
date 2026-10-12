from __future__ import annotations

import pytest

duckdb = pytest.importorskip(
    "duckdb",
    reason="DuckDB is unavailable or its DLL is blocked by App Control",
    exc_type=ImportError,
)

from importlib.resources import files
from typing import Any

from georisk.storage_analytics.lake import density_summary, risk_summary, write_partitioned


def _risk(event_time: str, vessel_id: str, alerted: bool) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "event_time": event_time,
        "vessel_id": vessel_id,
        "h3_index": "87754e649ffffff",
        "h3_resolution": 7,
        "risk_alert": alerted,
        "matching_hazards": [{"alert_id": "alert-1", "severity": 4}] if alerted else [],
    }


def _density(window_start: str, window_end: str, cell: str, count: int) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "window_start": window_start,
        "window_end": window_end,
        "h3_index": cell,
        "h3_resolution": 7,
        "unique_vessel_count": count,
    }


@pytest.fixture
def sql_lake(tmp_path):
    """Expose actual date-partitioned Parquet files through the SQL table names."""
    lake = tmp_path / "lake"
    write_partitioned(
        [
            _risk("2026-01-01T12:00:00Z", "vessel-a", True),
            _risk("2026-01-01T12:15:00Z", "vessel-a", True),
            _risk("2026-01-01T13:00:00Z", "vessel-b", False),
            _risk("2026-01-02T00:00:00Z", "vessel-b", True),
            _risk("2026-01-02T00:30:00Z", "vessel-c", False),
            _risk("2026-01-31T23:59:00Z", "vessel-a", True),
        ],
        lake,
        dataset_name="risk_events",
    )
    write_partitioned(
        [
            _density("2026-01-01T12:00:00Z", "2026-01-01T12:15:00Z", "87754e649ffffff", 2),
            _density("2026-01-01T12:00:00Z", "2026-01-01T12:15:00Z", "87754e648ffffff", 4),
            _density("2026-01-02T00:00:00Z", "2026-01-02T00:15:00Z", "87754e649ffffff", 7),
            _density("2026-01-31T23:45:00Z", "2026-02-01T00:00:00Z", "87754e64bffffff", 7),
        ],
        lake,
        dataset_name="density_metrics",
    )
    with duckdb.connect(":memory:") as connection:
        for dataset in ["risk_events", "density_metrics"]:
            pattern = (lake / "curated" / dataset / "*" / "*.parquet").as_posix().replace("'", "''")
            connection.execute(
                f"CREATE VIEW georisk_{dataset} AS SELECT * FROM read_parquet("
                f"'{pattern}', hive_partitioning = true, hive_types = {{'event_date': DATE}})"
            )
        yield lake, connection


def _query(connection, name: str) -> list[dict[str, Any]]:
    sql = (files("georisk") / "storage_analytics" / "sql" / name).read_text(encoding="utf-8")
    cursor = connection.execute(sql)
    columns = [column[0] for column in cursor.description]
    return [dict(zip(columns, row, strict=False)) for row in cursor.fetchall()]


def test_risk_summary_sql_matches_python_summary(sql_lake):
    lake, connection = sql_lake
    assert _query(connection, "risk_summary.sql") == [risk_summary(lake)]


def test_density_summary_sql_matches_python_summary(sql_lake):
    lake, connection = sql_lake
    expected = [
        {
            key: row[key]
            for key in (
                "window_start",
                "window_end",
                "h3_index",
                "h3_resolution",
                "unique_vessel_count",
            )
        }
        for row in density_summary(lake)
    ]
    assert _query(connection, "density_summary.sql") == expected


@pytest.mark.parametrize(
    "query",
    [
        "risk_summary.sql",
        "density_summary.sql",
        "top_congested_cells.sql",
        "exposure_by_vessel.sql",
        "hourly_alert_counts.sql",
    ],
)
def test_sql_filters_date_partitions(sql_lake, query):
    lake, connection = sql_lake
    expected = _query(connection, query)
    write_partitioned(
        [
            _risk("2025-12-31T23:59:00Z", "outside-before", True),
            _risk("2026-02-01T00:00:00Z", "outside-after", True),
        ],
        lake,
        dataset_name="risk_events",
    )
    write_partitioned(
        [
            _density("2025-12-31T23:45:00Z", "2026-01-01T00:00:00Z", "87754e649ffffff", 999),
            _density("2026-02-01T00:00:00Z", "2026-02-01T00:15:00Z", "87754e649ffffff", 999),
        ],
        lake,
        dataset_name="density_metrics",
    )
    assert _query(connection, query) == expected


def test_top_congested_cells_sql(sql_lake):
    _, connection = sql_lake
    assert _query(connection, "top_congested_cells.sql") == [
        {"h3_index": "87754e649ffffff", "peak_unique_vessel_count": 7},
        {"h3_index": "87754e64bffffff", "peak_unique_vessel_count": 7},
        {"h3_index": "87754e648ffffff", "peak_unique_vessel_count": 4},
    ]


def test_top_congested_cells_sql_limits_to_twenty(sql_lake):
    lake, connection = sql_lake
    write_partitioned(
        [
            _density(
                "2026-01-03T00:00:00Z", "2026-01-03T00:15:00Z", f"cell-{index:02d}", 10 + index
            )
            for index in range(25)
        ],
        lake,
        dataset_name="density_metrics",
        batch_id="more-cells",
    )
    assert _query(connection, "top_congested_cells.sql") == [
        {"h3_index": f"cell-{index:02d}", "peak_unique_vessel_count": 10 + index}
        for index in range(24, 4, -1)
    ]


def test_exposure_by_vessel_sql(sql_lake):
    _, connection = sql_lake
    assert _query(connection, "exposure_by_vessel.sql") == [
        {
            "vessel_id": "vessel-a",
            "alerted_event_count": 3,
            "first_alerted_event_time": "2026-01-01T12:00:00Z",
            "last_alerted_event_time": "2026-01-31T23:59:00Z",
        },
        {
            "vessel_id": "vessel-b",
            "alerted_event_count": 1,
            "first_alerted_event_time": "2026-01-02T00:00:00Z",
            "last_alerted_event_time": "2026-01-02T00:00:00Z",
        },
    ]


def test_hourly_alert_counts_sql(sql_lake):
    _, connection = sql_lake
    assert _query(connection, "hourly_alert_counts.sql") == [
        {"hour_bucket": "2026-01-01T12", "alerted_event_count": 2},
        {"hour_bucket": "2026-01-02T00", "alerted_event_count": 1},
        {"hour_bucket": "2026-01-31T23", "alerted_event_count": 1},
    ]

"""Batch Spark transformations sharing the local processor's contracts."""

from __future__ import annotations

from functools import reduce
from pathlib import Path
from typing import Any

import pandas as pd
from pyspark.sql import DataFrame, SparkSession, functions as F, types as T

from georisk.contracts import validate_event
from georisk.processing.h3_index import COVERAGE_MODES, _validate_resolution, polygon_to_cells
from georisk.processing.stream_job import _parse_utc

TELEMETRY_SCHEMA = T.StructType(
    [
        T.StructField("schema_version", T.IntegerType()),
        T.StructField("event_id", T.StringType()),
        T.StructField("event_time", T.StringType()),
        T.StructField("vessel_id", T.StringType()),
        T.StructField("latitude", T.DoubleType()),
        T.StructField("longitude", T.DoubleType()),
        T.StructField("speed_knots", T.DoubleType()),
        T.StructField("heading_deg", T.DoubleType()),
        T.StructField("_corrupt_record", T.StringType()),
    ]
)
ALERT_SCHEMA = T.StructType(
    [
        T.StructField("schema_version", T.IntegerType()),
        T.StructField("event_id", T.StringType()),
        T.StructField("alert_id", T.StringType()),
        T.StructField("event_time", T.StringType()),
        T.StructField("valid_from", T.StringType()),
        T.StructField("valid_until", T.StringType()),
        T.StructField("hazard_type", T.StringType()),
        T.StructField("severity", T.IntegerType()),
        T.StructField("geometry", T.StringType()),
        T.StructField("_corrupt_record", T.StringType()),
    ]
)


def read_telemetry(spark: SparkSession, path: str | Path) -> DataFrame:
    """Read JSONL telemetry with explicit types and malformed-record capture."""
    return spark.read.schema(TELEMETRY_SCHEMA).json(str(path))


def read_alerts(spark: SparkSession, path: str | Path) -> DataFrame:
    """Read JSONL alerts, retaining Polygon/MultiPolygon geometry as JSON."""
    return spark.read.schema(ALERT_SCHEMA).json(str(path))


def with_event_ts(df: DataFrame) -> DataFrame:
    """Parse event time without aborting the batch on malformed timestamps."""
    return df.withColumn("event_ts", F.try_to_timestamp("event_time"))


def split_valid(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split typed telemetry into accepted and reason-tagged rejected rows."""
    parsed = with_event_ts(df)
    checks = [
        F.col(name).isNotNull()
        for name in TELEMETRY_SCHEMA.fieldNames()
        if name != "_corrupt_record"
    ]
    checks.extend(
        [
            F.col("schema_version") == 1,
            F.length("event_id") > 0,
            F.length("vessel_id") > 0,
            F.col("event_ts").isNotNull(),
            F.col("event_time").rlike(r"(?:Z|[+-]00:00)$"),
            F.col("latitude").between(-90, 90),
            F.col("longitude").between(-180, 180),
            F.col("speed_knots").between(0, 60),
            F.col("heading_deg") >= 0,
            F.col("heading_deg") < 360,
        ]
    )
    if "_corrupt_record" in parsed.columns:
        checks.append(F.col("_corrupt_record").isNull())
    valid = F.coalesce(reduce(lambda left, right: left & right, checks), F.lit(False))
    return parsed.filter(valid), parsed.filter(~valid).withColumn(
        "reason",
        F.lit("contract_violation"),
    )


def make_h3_udf(resolution: int) -> Any:
    """Create a vectorized H3 mapper for already validated coordinates."""
    _validate_resolution(resolution)

    @F.pandas_udf("string")
    def to_h3(latitude: pd.Series, longitude: pd.Series) -> pd.Series:
        import h3

        return pd.Series(
            [
                h3.latlng_to_cell(float(lat), float(lon), resolution)
                for lat, lon in zip(latitude, longitude)
            ]
        )

    return to_h3


def assign_h3(df: DataFrame, resolution: int) -> DataFrame:
    """Add H3 cell and resolution to valid telemetry."""
    return df.withColumn("h3_index", make_h3_udf(resolution)("latitude", "longitude")).withColumn(
        "h3_resolution", F.lit(resolution).cast("int")
    )


def hazard_cells_df(
    spark: SparkSession,
    alerts: list[dict[str, Any]],
    resolution: int,
    coverage_mode: str,
) -> DataFrame:
    """Expand the latest valid revision of each driver-side alert into H3 cells."""
    _validate_resolution(resolution)
    if coverage_mode not in COVERAGE_MODES:
        raise ValueError("coverage_mode must be center or overlap")
    latest: dict[str, dict[str, Any]] = {}
    for alert in alerts:
        validate_event("weather_alert", alert)
        previous = latest.get(alert["alert_id"])
        if previous is None or _parse_utc(alert["event_time"]) >= _parse_utc(
            previous["event_time"]
        ):
            latest[alert["alert_id"]] = alert
    schema = (
        "h3_index string, alert_id string, hazard_type string, severity int, "
        "valid_from string, valid_until string, valid_from_ts timestamp, valid_until_ts timestamp"
    )
    rows = [
        (
            cell,
            alert["alert_id"],
            alert["hazard_type"],
            alert["severity"],
            alert["valid_from"],
            alert["valid_until"],
            _parse_utc(alert["valid_from"]),
            _parse_utc(alert["valid_until"]),
        )
        for alert in latest.values()
        for cell in sorted(polygon_to_cells(alert["geometry"], resolution, coverage_mode))
    ]
    return spark.createDataFrame(rows, schema)


def match_risk(
    telemetry_h3_df: DataFrame,
    hazard_cells: DataFrame,
    coverage_mode: str,
) -> DataFrame:
    """Match half-open hazard validity intervals and emit Arrow-compatible risk rows."""
    if coverage_mode not in COVERAGE_MODES:
        raise ValueError("coverage_mode must be center or overlap")
    telemetry = telemetry_h3_df.dropDuplicates(["event_id"]).alias("t")
    hazards = F.broadcast(hazard_cells).alias("h")
    joined = telemetry.join(
        hazards,
        (
            (F.col("t.h3_index") == F.col("h.h3_index"))
            & (F.col("h.valid_from_ts") <= F.col("t.event_ts"))
            & (F.col("t.event_ts") < F.col("h.valid_until_ts"))
        ),
        "left",
    )
    fields = [
        "event_id",
        "event_time",
        "vessel_id",
        "latitude",
        "longitude",
        "h3_index",
        "h3_resolution",
    ]
    hazard = F.when(
        F.col("h.alert_id").isNotNull(),
        F.struct(
            *[
                F.col(f"h.{name}").alias(name)
                for name in ("alert_id", "hazard_type", "severity", "valid_from", "valid_until")
            ],
        ),
    )
    result = joined.groupBy(*[F.col(f"t.{name}") for name in fields]).agg(
        F.sort_array(F.collect_list(hazard)).alias("matching_hazards"),
    )
    return result.select(
        F.lit(1).cast("int").alias("schema_version"),
        "event_time",
        "event_id",
        F.col("latitude").cast("double"),
        F.col("longitude").cast("double"),
        F.lit(coverage_mode).alias("coverage_mode"),
        "vessel_id",
        "h3_index",
        F.col("h3_resolution").cast("int"),
        (F.size("matching_hazards") > 0).alias("risk_alert"),
        "matching_hazards",
    )


def density_keys(telemetry_h3_df: DataFrame, window_minutes: int) -> DataFrame:
    """Return distinct vessel/window/cell keys, reusable across microbatches."""
    if (
        not isinstance(window_minutes, int)
        or isinstance(window_minutes, bool)
        or not 1 <= window_minutes <= 1440
        or 1440 % window_minutes != 0
    ):
        raise ValueError("window_minutes must be 1..1440 and divide 1440")
    return (
        telemetry_h3_df.withColumn(
            "window",
            F.window("event_ts", f"{window_minutes} minutes"),
        )
        .select(
            F.date_format("window.start", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("window_start"),
            F.date_format("window.end", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("window_end"),
            "h3_index",
            "h3_resolution",
            "vessel_id",
        )
        .distinct()
    )


def density(telemetry_h3_df: DataFrame, window_minutes: int) -> DataFrame:
    """Count distinct vessels in epoch-aligned, half-open UTC windows."""
    return (
        density_keys(telemetry_h3_df, window_minutes)
        .groupBy(
            "window_start",
            "window_end",
            "h3_index",
            "h3_resolution",
        )
        .agg(F.countDistinct("vessel_id").alias("unique_vessel_count"))
        .select(
            F.lit(1).cast("int").alias("schema_version"),
            "window_start",
            "window_end",
            "h3_index",
            F.col("h3_resolution").cast("int"),
            F.col("unique_vessel_count").cast("long"),
        )
    )

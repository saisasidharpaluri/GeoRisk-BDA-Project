"""Batch Spark transformations sharing the local processor's contracts."""

from __future__ import annotations

from functools import reduce
from pathlib import Path
from typing import Any

import pandas as pd
from pyspark.sql import DataFrame, SparkSession, functions as F, types as T

from georisk.processing.h3_index import _validate_resolution

TELEMETRY_SCHEMA = T.StructType([
    T.StructField("schema_version", T.IntegerType()),
    T.StructField("event_id", T.StringType()),
    T.StructField("event_time", T.StringType()),
    T.StructField("vessel_id", T.StringType()),
    T.StructField("latitude", T.DoubleType()),
    T.StructField("longitude", T.DoubleType()),
    T.StructField("speed_knots", T.DoubleType()),
    T.StructField("heading_deg", T.DoubleType()),
    T.StructField("_corrupt_record", T.StringType()),
])
ALERT_SCHEMA = T.StructType([
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
])


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
    checks = [F.col(name).isNotNull() for name in TELEMETRY_SCHEMA.fieldNames()
              if name != "_corrupt_record"]
    checks.extend([
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
    ])
    if "_corrupt_record" in parsed.columns:
        checks.append(F.col("_corrupt_record").isNull())
    valid = F.coalesce(reduce(lambda left, right: left & right, checks), F.lit(False))
    return parsed.filter(valid), parsed.filter(~valid).withColumn(
        "reason", F.lit("contract_violation"),
    )


def make_h3_udf(resolution: int) -> Any:
    """Create a vectorized H3 mapper for already validated coordinates."""
    _validate_resolution(resolution)

    @F.pandas_udf("string")
    def to_h3(latitude: pd.Series, longitude: pd.Series) -> pd.Series:
        import h3

        return pd.Series([
            h3.latlng_to_cell(float(lat), float(lon), resolution)
            for lat, lon in zip(latitude, longitude)
        ])

    return to_h3


def assign_h3(df: DataFrame, resolution: int) -> DataFrame:
    """Add H3 cell and resolution to valid telemetry."""
    return df.withColumn("h3_index", make_h3_udf(resolution)("latitude", "longitude")) \
        .withColumn("h3_resolution", F.lit(resolution).cast("int"))

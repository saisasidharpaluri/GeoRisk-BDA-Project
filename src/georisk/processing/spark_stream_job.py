"""Checkpointed local file streaming with batch Spark transformations."""

from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.streaming import StreamingQuery

from georisk.config import GeoRiskConfig
from georisk.contracts import ContractValidationError, validate_event
from georisk.processing.spark_transforms import (
    TELEMETRY_SCHEMA, assign_h3, density_keys, hazard_cells_df, match_risk, read_alerts,
    split_valid, with_event_ts,
)
from georisk.processing.stream_job import _parse_utc, _quarantine_entry, _timestamp


def _alerts(spark: SparkSession, directory: Path) -> tuple[list[dict], list[dict]]:
    """Collect the small static alert stream and quarantine invalid revisions."""
    if not any(directory.glob("*.jsonl")) and not any(directory.glob("*.json")):
        return [], []
    accepted, rejected = [], []
    for row in read_alerts(spark, directory).collect():
        record = row.asDict()
        corrupt = record.pop("_corrupt_record", None)
        try:
            if corrupt:
                raise ValueError(f"invalid alert JSON: {corrupt}")
            record["geometry"] = json.loads(record["geometry"])
            validate_event("weather_alert", record)
        except (ValueError, TypeError) as exc:
            reason = "invalid_validity_range" if (
                isinstance(exc, ContractValidationError)
                and "valid_until: must be after valid_from" in exc.errors
            ) else "contract_violation"
            rejected.append(_quarantine_entry("weather_alert", reason, record, exc))
        else:
            accepted.append(record)
    return accepted, rejected


def _unwritten(rows: DataFrame, risk_root: Path) -> DataFrame:
    """Prune the existing lake to affected dates before event-ID deduplication."""
    dates = [row.event_date for row in rows.select("event_date").distinct().collect()]
    partitions = [str(risk_root / f"event_date={date}") for date in dates
                  if any((risk_root / f"event_date={date}").glob("*.parquet"))]
    if not partitions:
        return rows
    existing = rows.sparkSession.read.parquet(*partitions).select("event_id").distinct()
    return rows.join(existing, "event_id", "left_anti")


def _write_alert_quarantine(root: Path, batch_id: int, rows: list[dict]) -> None:
    """Use one replaceable file per batch so a retry does not duplicate rejections."""
    directory = root / "quarantine" / "alerts"
    directory.mkdir(parents=True, exist_ok=True)
    temporary = directory / f".batch-{batch_id}.tmp"
    temporary.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
                         encoding="utf-8")
    temporary.replace(directory / f"batch-{batch_id}.jsonl")


def _recompute_density(accepted: DataFrame, output: Path, window_minutes: int) -> None:
    """Append vessel keys and replace only the dates touched by this batch."""
    keys = density_keys(with_event_ts(accepted), window_minutes).withColumn(
        "event_date", F.substring("window_start", 1, 10),
    )
    dates = [row.event_date for row in keys.select("event_date").distinct().collect()]
    state_root = output / "state" / "density_keys"
    keys.write.mode("append").partitionBy("event_date").parquet(str(state_root))
    all_keys = accepted.sparkSession.read.option("basePath", str(state_root)).parquet(
        *[str(state_root / f"event_date={date}") for date in dates],
    )
    metrics = all_keys.groupBy(
        "event_date", "window_start", "window_end", "h3_index", "h3_resolution",
    ).agg(F.countDistinct("vessel_id").alias("unique_vessel_count")).withColumn(
        "schema_version", F.lit(1).cast("int"),
    )
    metrics.write.mode("overwrite").option("partitionOverwriteMode", "dynamic").partitionBy(
        "event_date",
    ).parquet(str(output / "curated" / "density_metrics"))


def run_stream(
    spark: SparkSession,
    input_root: str | Path,
    output_root: str | Path,
    checkpoint_root: str | Path,
    *,
    resolution: int = 7,
    coverage_mode: str = "overlap",
    window_minutes: int = 15,
    available_now: bool = True,
    allowed_lateness_minutes: int | None = None,
) -> StreamingQuery:
    """Start a local JSONL stream; callers await or stop the returned query.

    Use one writer and one persistent checkpoint per output root. Completed
    accepted-batch snapshots make retries after a partial sink write safe.
    """
    lateness = (GeoRiskConfig.from_env().allowed_lateness_minutes
                if allowed_lateness_minutes is None else allowed_lateness_minutes)
    GeoRiskConfig(resolution=resolution, coverage_mode=coverage_mode,
                  density_window_minutes=window_minutes, allowed_lateness_minutes=lateness)
    if not isinstance(available_now, bool):
        raise TypeError("available_now must be a boolean")
    source, output, checkpoint = map(Path, (input_root, output_root, checkpoint_root))
    telemetry_dir, alerts_dir = source / "vessel-telemetry", source / "weather-alerts"
    telemetry_dir.mkdir(parents=True, exist_ok=True)
    alerts_dir.mkdir(parents=True, exist_ok=True)
    checkpoint.mkdir(parents=True, exist_ok=True)
    risk_root = output / "curated" / "risk_events"
    watermark_path = checkpoint / "georisk-watermark.json"

    def process_batch(batch_df: DataFrame, batch_id: int) -> None:
        batch = batch_df.persist()
        accepted: DataFrame | None = None
        try:
            if batch.isEmpty():
                return
            valid, invalid = split_valid(batch)
            invalid.write.mode("overwrite").json(
                str(output / "quarantine" / "telemetry" / f"batch-{batch_id}"),
            )
            snapshot = checkpoint / "georisk-batches" / str(batch_id) / "accepted"
            if not (snapshot / "_SUCCESS").exists():
                alerts, rejected = _alerts(spark, alerts_dir)
                _write_alert_quarantine(output, batch_id, rejected)
                distinct = valid.dropDuplicates(["event_id"]).withColumn(
                    "event_date", F.date_format("event_ts", "yyyy-MM-dd"),
                )
                new = _unwritten(distinct, risk_root)
                previous = (json.loads(watermark_path.read_text(encoding="utf-8"))
                            .get("max_event_time") if watermark_path.exists() else None)
                if previous is not None:
                    cutoff = _parse_utc(previous) - timedelta(minutes=lateness)
                    new.filter(F.col("event_ts") < F.lit(cutoff)).withColumn(
                        "reason", F.lit("late_event"),
                    ).write.mode("overwrite").json(
                        str(output / "quarantine" / "late" / f"batch-{batch_id}"),
                    )
                    new = new.filter(F.col("event_ts") >= F.lit(cutoff))
                risk = match_risk(assign_h3(new, resolution),
                                  hazard_cells_df(spark, alerts, resolution, coverage_mode),
                                  coverage_mode).withColumn(
                    "event_date", F.date_format(F.try_to_timestamp("event_time"), "yyyy-MM-dd"),
                )
                risk.write.mode("overwrite").parquet(str(snapshot))
            accepted = spark.read.parquet(str(snapshot)).persist()
            if not accepted.isEmpty():
                _recompute_density(accepted, output, window_minutes)
                _unwritten(accepted, risk_root).write.mode("append").partitionBy(
                    "event_date",
                ).parquet(str(risk_root))
                maximum = accepted.select(F.date_format(
                    F.max(F.try_to_timestamp("event_time")), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'",
                )).first()[0]
                previous = (json.loads(watermark_path.read_text(encoding="utf-8"))
                            .get("max_event_time") if watermark_path.exists() else None)
                current = _parse_utc(maximum)
                if previous is not None:
                    current = max(current, _parse_utc(previous))
                temporary = watermark_path.with_suffix(".tmp")
                temporary.write_text(json.dumps({"max_event_time": _timestamp(current)}) + "\n",
                                     encoding="utf-8")
                temporary.replace(watermark_path)
        finally:
            if accepted is not None:
                accepted.unpersist()
            batch.unpersist()

    stream = spark.readStream.schema(TELEMETRY_SCHEMA).json(str(telemetry_dir))
    writer = stream.writeStream.foreachBatch(process_batch).option(
        "checkpointLocation", str(checkpoint / "risk"),
    )
    if available_now:
        writer = writer.trigger(availableNow=True)
    return writer.start()

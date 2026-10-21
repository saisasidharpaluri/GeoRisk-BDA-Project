"""Live mode: stream the scenario through file micro-batches and process each one as it lands.

A producer thread publishes one atomic JSONL batch per scenario time step to
``<stream_dir>/live`` and waits ``tick_seconds`` between steps. The consumer
polls that directory, feeds every new batch to an incremental
:class:`~georisk.processing.stream_job.StreamProcessor`, and rewrites the
dashboard's input and processed files after each batch, with live progress in
``run-metadata.json``. When the producer has finished, the run completes like
``georisk run``: Parquet lake, delay model, route decision, and acceptance
report. Processing the same events in time order gives the same risk events
and density metrics as the batch path.
"""

from __future__ import annotations

import json
import logging
import shutil
import threading
import time
from collections.abc import Callable, Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from georisk.config import GeoRiskConfig
from georisk.contracts import parse_utc_timestamp
from georisk.ingestion.file_stream import FileStreamPublisher
from georisk.ingestion.generator import Event, iter_events_by_time, write_jsonl
from georisk.pipeline import finish_run, scenario_events
from georisk.processing.stream_job import (
    StreamProcessor,
    _read_jsonl,
    _write_atomic,
    _write_jsonl,
    include_unreadable,
    run_metadata,
    write_outputs,
)

logger = logging.getLogger(__name__)

UTC = timezone.utc
LIVE_SUBDIR = "live"
POLL_SECONDS = 0.1

Tick = tuple[datetime, list[tuple[str, Event]]]
STREAMS = {"weather_alerts": "weather-alerts", "telemetry": "vessel-telemetry"}


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def scenario_ticks(events: Mapping[str, list[Event]]) -> list[Tick]:
    """Group events by event time in replay order; each group is one micro-batch."""
    ticks: list[Tick] = []
    for stream_key, event in iter_events_by_time(events):
        event_time = parse_utc_timestamp(event["event_time"])
        if ticks and ticks[-1][0] == event_time:
            ticks[-1][1].append((stream_key, event))
        else:
            ticks.append((event_time, [(stream_key, event)]))
    return ticks


def publish_ticks(
    ticks: list[Tick],
    root: Path,
    *,
    tick_seconds: float,
    stop: threading.Event,
    progress: dict[str, int],
) -> None:
    """Publish one batch file per stream and tick, pausing between ticks until ``stop``."""
    with FileStreamPublisher(root, flush_every=1_000_000) as publisher:
        for index, (_, records) in enumerate(ticks):
            if stop.is_set():
                return
            for stream_key, event in records:
                publisher(STREAMS[stream_key], event)
            publisher.flush()
            progress["published"] = index + 1
            if index + 1 < len(ticks) and stop.wait(tick_seconds):
                return


class _BatchReader:
    """Return batch files that appeared since the previous poll, in sequence order."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self._seen: set[Path] = set()

    def _new(self, stream: str) -> list[Path]:
        directory = self.root / stream
        if not directory.is_dir():
            return []
        files = sorted(path for path in directory.glob("part-*.jsonl") if path not in self._seen)
        self._seen.update(files)
        return files

    def poll(self) -> tuple[list[Path], list[Path]]:
        # Telemetry is listed first: the producer flushes alerts first, so every
        # telemetry batch seen here has its alerts in the alert listing below.
        telemetry = self._new(STREAMS["telemetry"])
        alerts = self._new(STREAMS["weather_alerts"])
        return alerts, telemetry


def run_live(
    settings: GeoRiskConfig,
    *,
    tick_seconds: float | None = None,
    poll_seconds: float = POLL_SECONDS,
    on_batch: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    """Stream, process, and finish one live run; return the run report.

    ``on_batch`` receives the live statistics after every processed batch.
    """
    tick = settings.live_tick_seconds if tick_seconds is None else tick_seconds
    events = scenario_events(settings)
    ticks = scenario_ticks(events)
    root = settings.stream_dir / LIVE_SUBDIR
    shutil.rmtree(root, ignore_errors=True)
    settings.input_dir.mkdir(parents=True, exist_ok=True)
    # Estimates from an earlier run would describe other events until this run finishes.
    (settings.processed_dir / "delay-estimates.jsonl").unlink(missing_ok=True)

    processor = StreamProcessor(
        resolution=settings.resolution,
        strict=settings.strict_validation,
        coverage_mode=settings.coverage_mode,
        window_minutes=settings.density_window_minutes,
        allowed_lateness_minutes=settings.allowed_lateness_minutes,
        max_future_minutes=settings.max_future_minutes,
    )
    unreadable: list[dict[str, Any]] = []
    received: dict[str, list[dict[str, Any]]] = {"telemetry": [], "weather_alerts": []}
    progress = {"published": 0}
    live: dict[str, Any] = {
        "status": "running",
        "started_at": _now(),
        "updated_at": _now(),
        "tick_seconds": tick,
        "ticks_total": len(ticks),
        "ticks_published": 0,
        "batches": 0,
        "batch_files": 0,
        "events_processed": 0,
        "last_batch_events": 0,
        "last_batch_latency_ms": None,
        "processing_seconds": 0.0,
        "throughput_events_per_second": None,
        "stream_dir": str(root),
    }

    def snapshot() -> dict[str, Any]:
        results = processor.result()
        include_unreadable(results, list(unreadable))
        live["ticks_published"] = progress["published"]
        live["updated_at"] = _now()
        results["metadata"] = {
            **run_metadata(
                results,
                resolution=settings.resolution,
                coverage_mode=settings.coverage_mode,
                window_minutes=settings.density_window_minutes,
                allowed_lateness_minutes=settings.allowed_lateness_minutes,
                max_future_minutes=settings.max_future_minutes,
            ),
            "mode": "live",
            "live": dict(live),
        }
        return results

    def publish_snapshot() -> dict[str, Any]:
        _write_jsonl(settings.input_dir / "weather-alerts.jsonl", received["weather_alerts"])
        _write_jsonl(settings.input_dir / "vessel-telemetry.jsonl", received["telemetry"])
        results = snapshot()
        write_outputs(settings.processed_dir, results)
        return results

    publish_snapshot()
    stop = threading.Event()
    producer_error: list[BaseException] = []

    def produce() -> None:
        try:
            publish_ticks(ticks, root, tick_seconds=tick, stop=stop, progress=progress)
        except BaseException as exc:  # re-raised by the consumer thread
            producer_error.append(exc)

    producer = threading.Thread(target=produce, name="georisk-live-producer", daemon=True)
    producer.start()
    reader = _BatchReader(root)
    try:
        while True:
            finished = not producer.is_alive()
            alert_files, telemetry_files = reader.poll()
            if not alert_files and not telemetry_files:
                if finished:
                    break
                time.sleep(poll_seconds)
                continue
            started = time.perf_counter()
            alerts = [
                record
                for path in alert_files
                for record in _read_jsonl(
                    path, "weather_alert", unreadable, strict=settings.strict_validation
                )
            ]
            telemetry = [
                record
                for path in telemetry_files
                for record in _read_jsonl(
                    path, "vessel_telemetry", unreadable, strict=settings.strict_validation
                )
            ]
            processor.add_alerts(alerts)
            processor.add_telemetry(telemetry)
            received["weather_alerts"].extend(alerts)
            received["telemetry"].extend(telemetry)
            batch_events = len(alerts) + len(telemetry)
            live["batches"] += 1
            live["batch_files"] += len(alert_files) + len(telemetry_files)
            live["events_processed"] += batch_events
            live["last_batch_events"] = batch_events
            live["processing_seconds"] += time.perf_counter() - started
            live["throughput_events_per_second"] = (
                round(live["events_processed"] / live["processing_seconds"])
                if live["processing_seconds"] > 0
                else None
            )
            # Latency: from the newest batch file landing to its results being ready.
            landed = max(path.stat().st_mtime for path in (*alert_files, *telemetry_files))
            live["last_batch_latency_ms"] = round(max(0.0, time.time() - landed) * 1000, 1)
            results = publish_snapshot()
            logger.debug("Live batch processed", extra={"georisk": dict(live)})
            if on_batch is not None:
                on_batch(dict(live, scenario_time=results["stats"]["max_event_time"]))
        if producer_error:
            raise producer_error[0]
    except BaseException:
        stop.set()
        producer.join()
        live["status"] = "stopped"
        try:
            write_outputs(settings.processed_dir, snapshot())
        except (OSError, ValueError):
            logger.exception("Could not record the stopped live run")
        raise
    producer.join()

    live["processing_seconds"] = round(live["processing_seconds"], 4)
    live["status"] = "finishing"
    results = publish_snapshot()
    generation_stats = write_jsonl(events, settings.input_dir)
    report = finish_run(
        settings, events, generation_stats, results, live=dict(live, status="complete")
    )
    # Only the metadata changes; the outputs hashed into the report stay as written.
    live["status"] = "complete"
    _write_atomic(
        settings.processed_dir / "run-metadata.json",
        json.dumps(snapshot()["metadata"], indent=2, sort_keys=True) + "\n",
    )
    logger.info("Live run complete", extra={"georisk": dict(live)})
    return report

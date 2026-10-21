"""Atomic JSONL batches for local file-stream consumers."""

from __future__ import annotations

import json
import os
from pathlib import Path
from types import TracebackType
from typing import Any


class FileStreamPublisher:
    """Buffer each stream separately; use one writer per root directory."""

    def __init__(self, root: str | Path, flush_every: int = 50) -> None:
        if not isinstance(flush_every, int) or isinstance(flush_every, bool) or flush_every < 1:
            raise ValueError("flush_every must be a positive integer")
        self.root = Path(root)
        self.flush_every = flush_every
        self._buffers: dict[str, list[str]] = {}
        self._sequences: dict[str, int] = {}
        self._closed = False

    def __call__(self, stream_name: str, event: dict[str, Any]) -> None:
        if self._closed:
            raise ValueError("publisher is closed")
        if stream_name not in ("vessel-telemetry", "weather-alerts"):
            raise ValueError("stream_name must be vessel-telemetry or weather-alerts")
        line = json.dumps(event, sort_keys=True, allow_nan=False) + "\n"
        buffer = self._buffers.setdefault(stream_name, [])
        buffer.append(line)
        if len(buffer) >= self.flush_every:
            self._flush_stream(stream_name)

    def _flush_stream(self, stream_name: str) -> None:
        buffer = self._buffers[stream_name]
        if not buffer:
            return
        directory = self.root / stream_name
        directory.mkdir(parents=True, exist_ok=True)
        if stream_name not in self._sequences:
            existing = [
                int(path.stem[5:])
                for path in directory.glob("part-*.jsonl")
                if path.stem[5:].isdigit()
            ]
            self._sequences[stream_name] = max(existing, default=-1) + 1
        sequence = self._sequences[stream_name]
        temporary = directory / f".part-{sequence:06d}.tmp"
        destination = directory / f"part-{sequence:06d}.jsonl"
        try:
            with temporary.open("w", encoding="utf-8", newline="\n") as handle:
                handle.writelines(buffer)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, destination)
        finally:
            temporary.unlink(missing_ok=True)
        buffer.clear()
        self._sequences[stream_name] += 1

    def flush(self) -> None:
        """Publish all non-empty pending batches, alerts before telemetry.

        A consumer that lists telemetry before alerts therefore never sees a
        telemetry batch without the alerts flushed together with it.
        """
        for stream_name in sorted(self._buffers, key=lambda name: name != "weather-alerts"):
            self._flush_stream(stream_name)

    def close(self) -> None:
        """Publish final partial batches and reject further records."""
        if not self._closed:
            self.flush()
            self._closed = True

    def __enter__(self) -> FileStreamPublisher:
        if self._closed:
            raise ValueError("publisher is closed")
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

"""Opt-in JSON-lines logging for GeoRisk command-line entry points."""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone

LOG_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL", "NOTSET")


class JsonFormatter(logging.Formatter):
    """Format a record and its optional GeoRisk fields as one JSON line."""

    def format(self, record: logging.LogRecord) -> str:
        fields = dict(getattr(record, "georisk", {}))
        fields.update({
            "ts": datetime.fromtimestamp(record.created, timezone.utc).isoformat().replace("+00:00", "Z"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        })
        if record.exc_info:
            fields["exception"] = self.formatException(record.exc_info)
        return json.dumps(fields, sort_keys=True, default=str)


def configure_logging(level: str | None = None) -> None:
    """Install one GeoRisk stderr handler without changing host handlers."""
    selected = (level if level is not None else os.environ.get("GEORISK_LOG_LEVEL", "INFO")).upper()
    if selected not in LOG_LEVELS:
        raise ValueError(f"GEORISK_LOG_LEVEL must be one of {', '.join(LOG_LEVELS)}")
    logger = logging.getLogger("georisk")
    for handler in list(logger.handlers):
        if getattr(handler, "_georisk_json", False):
            logger.removeHandler(handler)
            handler.close()
    handler = logging.StreamHandler(sys.stderr)
    handler._georisk_json = True
    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)
    logger.setLevel(selected)

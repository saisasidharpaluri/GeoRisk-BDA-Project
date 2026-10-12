import json
import logging

import pytest

from georisk.config import GeoRiskConfig
from georisk.logging_utils import JsonFormatter, configure_logging
from georisk.processing.stream_job import process_events


def test_process_events_logs_settings(caplog):
    with caplog.at_level(logging.INFO, logger="georisk.processing.stream_job"):
        process_events([], [], resolution=8, coverage_mode="center", window_minutes=60)
    record = next(record for record in caplog.records if record.message == "Processing batch")
    assert record.georisk["coverage_mode"] == "center"
    assert record.georisk["h3_resolution"] == 8
    assert record.georisk["window_minutes"] == 60
    completed = next(record for record in caplog.records if record.message == "Processing complete")
    assert completed.georisk["quarantined"] == 0
    assert completed.georisk["duplicates_dropped"] == 0


def test_json_formatter_outputs_valid_json():
    record = logging.LogRecord("georisk.test", logging.INFO, __file__, 1, "Hello %s\nnext", ("world",), None)
    record.georisk = {"coverage_mode": "overlap", "h3_resolution": 7, "counts": {"late": 2}}
    formatted = JsonFormatter().format(record)
    assert len(formatted.splitlines()) == 1
    data = json.loads(formatted)
    assert data["message"] == "Hello world\nnext"
    assert data["level"] == "INFO"
    assert data["logger"] == "georisk.test"
    assert data["ts"].endswith("Z")
    assert data["counts"] == {"late": 2}
    assert data["coverage_mode"] == "overlap"


def test_configure_logging_is_idempotent_and_uses_stderr(capsys, monkeypatch):
    logger = logging.getLogger("georisk")
    previous_handlers, previous_level = list(logger.handlers), logger.level
    host_handler = logging.NullHandler()
    logger.addHandler(host_handler)
    try:
        monkeypatch.setenv("GEORISK_LOG_LEVEL", "DEBUG")
        configure_logging()
        configure_logging()
        assert host_handler in logger.handlers
        assert sum(bool(getattr(h, "_georisk_json", False)) for h in logger.handlers) == 1
        logging.getLogger("georisk.test").debug("debug message")
        captured = capsys.readouterr()
        assert captured.out == ""
        assert len(captured.err.splitlines()) == 1
        assert json.loads(captured.err)["message"] == "debug message"
    finally:
        for handler in logger.handlers:
            if handler not in previous_handlers:
                handler.close()
        logger.handlers[:] = previous_handlers
        logger.setLevel(previous_level)


def test_quarantine_debug_log(caplog):
    with caplog.at_level(logging.DEBUG, logger="georisk.processing.stream_job"):
        process_events([{}], [])
    records = [r for r in caplog.records if r.message == "Record quarantined"]
    assert len(records) == 1
    assert records[0].georisk["reason"] == "contract_violation"
    assert records[0].georisk["contract"] == "vessel_telemetry"


def test_invalid_log_level_rejected():
    with pytest.raises(ValueError, match="GEORISK_LOG_LEVEL"):
        configure_logging("unknown")
    with pytest.raises(ValueError, match="GEORISK_LOG_LEVEL"):
        GeoRiskConfig.from_env({"GEORISK_LOG_LEVEL": "unknown"})
    assert GeoRiskConfig.from_env({"GEORISK_LOG_LEVEL": "debug"}).log_level == "DEBUG"

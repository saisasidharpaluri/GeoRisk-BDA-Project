from __future__ import annotations

import shutil
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def project_root() -> Path:
    """Return the project directory independently of the working directory."""
    return PROJECT_ROOT


@pytest.fixture(scope="session")
def spark():
    """Share a local Spark session, skipping only missing prerequisites."""
    pytest.importorskip("pyspark")
    if shutil.which("java") is None:
        pytest.skip("Java is required for Spark tests; install Java 17")
    import os

    from georisk.spark_session import get_spark

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    session = get_spark("georisk-tests")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()

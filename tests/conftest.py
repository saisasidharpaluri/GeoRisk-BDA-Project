from __future__ import annotations

from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def project_root() -> Path:
    """Return the project directory independently of the working directory."""
    return PROJECT_ROOT

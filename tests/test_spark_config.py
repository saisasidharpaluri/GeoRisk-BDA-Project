from __future__ import annotations

from pathlib import Path

import pytest

from georisk.config import GeoRiskConfig


def test_spark_output_settings():
    settings = GeoRiskConfig.from_env({
        "GEORISK_SPARK_OUTPUT_DIR": "custom/spark", "GEORISK_SPARK_CHECKPOINT_DIR": "custom/checkpoint",
    })
    assert settings.spark_output_dir == Path("custom/spark")
    assert settings.spark_checkpoint_dir == Path("custom/checkpoint")
    with pytest.raises(ValueError, match="GEORISK_SPARK_OUTPUT_DIR"):
        GeoRiskConfig(spark_output_dir="not-a-path")
    with pytest.raises(ValueError, match="GEORISK_SPARK_CHECKPOINT_DIR"):
        GeoRiskConfig(spark_checkpoint_dir="not-a-path")


def test_delay_model_setting():
    assert GeoRiskConfig.from_env({}).delay_model == "linear"
    assert GeoRiskConfig.from_env({"GEORISK_DELAY_MODEL": "gbt"}).delay_model == "gbt"
    with pytest.raises(ValueError, match="GEORISK_DELAY_MODEL"):
        GeoRiskConfig.from_env({"GEORISK_DELAY_MODEL": "unknown"})

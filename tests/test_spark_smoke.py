from __future__ import annotations

import pytest

pytestmark = pytest.mark.spark


def test_spark_session_runs(spark):
    assert spark.range(3).count() == 3
    assert spark.conf.get("spark.sql.session.timeZone") == "UTC"
    assert spark.conf.get("spark.sql.shuffle.partitions") == "4"
    assert spark.conf.get("spark.ui.enabled") == "false"

"""Optional Spark session construction for local analytics."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def get_spark(app_name: str = "georisk", master: str = "local[2]") -> SparkSession:
    """Create a UTC session; install the spark extra and Java 17 first."""
    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:
        raise RuntimeError(
            'Spark requires Python 3.11, Java 17, and pip install -e ".[spark]". '
            "See the README Spark setup instructions."
        ) from exc
    return (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

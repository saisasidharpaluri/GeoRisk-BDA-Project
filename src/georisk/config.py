"""Typed runtime configuration for local runs and AWS deployment."""

from __future__ import annotations

import os
import math
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

from georisk.logging_utils import LOG_LEVELS


def load_env_file(path: str | Path = ".env", *, override: bool = False) -> dict[str, str]:
    """Apply literal KEY=VALUE settings, preserving shell values by default."""
    target = Path(path)
    parsed: dict[str, str] = {}
    for line_number, raw in enumerate(target.read_text(encoding="utf-8-sig").splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        key, separator, value = line.partition("=")
        key, value = key.strip(), value.strip()
        if not separator or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key) or "\x00" in value:
            raise ValueError(f"invalid environment assignment in {target}:{line_number}")
        if len(value) >= 2 and value[0] in ("'", '"') and value[-1] == value[0]:
            value = value[1:-1]
        parsed[key] = value
    applied = {key: value for key, value in parsed.items() if override or key not in os.environ}
    os.environ.update(applied)
    return applied


def _integer(values: Mapping[str, str], name: str, default: int) -> int:
    raw = values.get(name, str(default))
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc


def _float(values: Mapping[str, str], name: str, default: float) -> float:
    raw = values.get(name, str(default))
    try:
        return float(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a number") from exc


def _boolean(values: Mapping[str, str], name: str, default: bool) -> bool:
    raw = values.get(name, str(default)).strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be true or false")


@dataclass(frozen=True)
class GeoRiskConfig:
    """Validated settings shared by all local pipeline stages."""

    seed: int = 7
    env_file: Path = Path(".env")
    vessel_count: int = 10
    telemetry_points: int = 3
    scenario_duration_minutes: int = 120
    telemetry_interval_seconds: int = 300
    stream_dir: Path = Path("output/stream")
    spark_output_dir: Path = Path("output/spark")
    spark_checkpoint_dir: Path = Path("output/spark/checkpoints")
    replay_speed_factor: float = 0.0
    resolution: int = 7
    coverage_mode: str = "overlap"
    density_window_minutes: int = 15
    allowed_lateness_minutes: int = 10
    input_dir: Path = Path("output/data")
    processed_dir: Path = Path("output/processed")
    lake_dir: Path = Path("output/lake")
    dashboard_dir: Path = Path("output")
    report_path: Path = Path("output/reports/phase6-run.json")
    route_risk_threshold: float = 0.5
    route_risk_penalty: float = 4.0
    evaluation_fraction: float = 0.2
    delay_noise_std_hours: float = 0.25
    delay_model: str = "linear"
    acceptance_required: bool = True
    strict_validation: bool = False
    log_level: str = "INFO"
    deployment_mode: str = "local"
    aws_region: str = "us-east-1"
    aws_stack_name: str = "georisk-foundation"
    aws_project_bucket_name: str = ""
    aws_glue_role_arn: str = ""
    deployment_plan_path: Path = Path("output/reports/phase8-deployment-plan.json")
    foundation_template_path: Path = Path(
        "infra/aws/georisk-foundation.template.json"
    )

    def __post_init__(self) -> None:
        if self.delay_model not in ("linear", "gbt"):
            raise ValueError("GEORISK_DELAY_MODEL must be linear or gbt")
        for name, value in (
            ("GEORISK_ENV_FILE", self.env_file),
            ("GEORISK_SPARK_OUTPUT_DIR", self.spark_output_dir),
            ("GEORISK_SPARK_CHECKPOINT_DIR", self.spark_checkpoint_dir),
        ):
            if not isinstance(value, Path):
                raise ValueError(f"{name} must be a Path")
        if (not isinstance(self.delay_noise_std_hours, (int, float))
                or isinstance(self.delay_noise_std_hours, bool)
                or not math.isfinite(self.delay_noise_std_hours)
                or self.delay_noise_std_hours < 0):
            raise ValueError("GEORISK_DELAY_NOISE_STD_HOURS must be a non-negative finite number")
        for name, value in (
            ("GEORISK_SCENARIO_DURATION_MINUTES", self.scenario_duration_minutes),
            ("GEORISK_TELEMETRY_INTERVAL_SECONDS", self.telemetry_interval_seconds),
        ):
            if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
                raise ValueError(f"{name} must be a positive integer")
        if not 1 <= self.scenario_duration_minutes * 60 // self.telemetry_interval_seconds <= 1000:
            raise ValueError("scenario duration and interval must produce 1..1000 telemetry steps")
        if not isinstance(self.stream_dir, Path):
            raise ValueError("GEORISK_STREAM_DIR must be a Path")
        if (not isinstance(self.replay_speed_factor, (int, float))
                or isinstance(self.replay_speed_factor, bool)
                or not math.isfinite(self.replay_speed_factor) or self.replay_speed_factor < 0):
            raise ValueError("GEORISK_REPLAY_SPEED_FACTOR must be a non-negative finite number")
        if self.log_level not in LOG_LEVELS:
            raise ValueError(f"GEORISK_LOG_LEVEL must be one of {LOG_LEVELS}")
        if (
            not isinstance(self.allowed_lateness_minutes, int)
            or isinstance(self.allowed_lateness_minutes, bool)
            or not 0 <= self.allowed_lateness_minutes <= 1440
        ):
            raise ValueError("GEORISK_ALLOWED_LATENESS_MINUTES must be an integer from 0 to 1440")
        if (
            not isinstance(self.density_window_minutes, int)
            or isinstance(self.density_window_minutes, bool)
            or not 1 <= self.density_window_minutes <= 1440
            or 1440 % self.density_window_minutes != 0
        ):
            raise ValueError("GEORISK_DENSITY_WINDOW_MINUTES must be 1..1440 and divide 1440")
        if self.coverage_mode not in ("center", "overlap"):
            raise ValueError("GEORISK_H3_COVERAGE_MODE must be center or overlap")
        if not isinstance(self.strict_validation, bool):
            raise ValueError("GEORISK_STRICT_VALIDATION must be true or false")
        if self.vessel_count < 0 or self.telemetry_points < 1:
            raise ValueError("vessel_count must be non-negative and telemetry_points positive")
        if not 0 <= self.resolution <= 15:
            raise ValueError("GEORISK_RESOLUTION must be between 0 and 15")
        if not 0 <= self.route_risk_threshold <= 1:
            raise ValueError("GEORISK_ROUTE_RISK_THRESHOLD must be between 0 and 1")
        if self.route_risk_penalty < 0:
            raise ValueError("GEORISK_ROUTE_RISK_PENALTY cannot be negative")
        if not 0 < self.evaluation_fraction < 1:
            raise ValueError("GEORISK_EVALUATION_FRACTION must be between 0 and 1")
        if self.deployment_mode not in {"local", "aws"}:
            raise ValueError("GEORISK_DEPLOYMENT_MODE must be local or aws")
        if not self.aws_region.strip():
            raise ValueError("AWS_REGION cannot be empty")
        if not self.aws_stack_name.strip():
            raise ValueError("GEORISK_AWS_STACK_NAME cannot be empty")

    @classmethod
    def from_env(cls, values: Mapping[str, str] | None = None) -> "GeoRiskConfig":
        """Build configuration from environment variables without reading secrets."""
        source = os.environ if values is None else values
        return cls(
            env_file=Path(source.get("GEORISK_ENV_FILE", ".env")),
            seed=_integer(source, "GEORISK_SEED", 7),
            vessel_count=_integer(source, "GEORISK_VESSEL_COUNT", 10),
            telemetry_points=_integer(source, "GEORISK_TELEMETRY_POINTS", 3),
            scenario_duration_minutes=_integer(source, "GEORISK_SCENARIO_DURATION_MINUTES", 120),
            telemetry_interval_seconds=_integer(source, "GEORISK_TELEMETRY_INTERVAL_SECONDS", 300),
            stream_dir=Path(source.get("GEORISK_STREAM_DIR", "output/stream")),
            spark_output_dir=Path(source.get("GEORISK_SPARK_OUTPUT_DIR", "output/spark")),
            spark_checkpoint_dir=Path(source.get("GEORISK_SPARK_CHECKPOINT_DIR", "output/spark/checkpoints")),
            replay_speed_factor=_float(source, "GEORISK_REPLAY_SPEED_FACTOR", 0.0),
            resolution=_integer(source, "GEORISK_RESOLUTION", 7),
            coverage_mode=source.get("GEORISK_H3_COVERAGE_MODE", "overlap"),
            density_window_minutes=_integer(source, "GEORISK_DENSITY_WINDOW_MINUTES", 15),
            allowed_lateness_minutes=_integer(source, "GEORISK_ALLOWED_LATENESS_MINUTES", 10),
            input_dir=Path(source.get("GEORISK_INPUT_DIR", "output/data")),
            processed_dir=Path(source.get("GEORISK_OUTPUT_DIR", "output/processed")),
            lake_dir=Path(source.get("GEORISK_LAKE_DIR", "output/lake")),
            dashboard_dir=Path(source.get("GEORISK_DASHBOARD_DATA_DIR", "output")),
            report_path=Path(
                source.get("GEORISK_REPORT_PATH", "output/reports/phase6-run.json")
            ),
            route_risk_threshold=_float(source, "GEORISK_ROUTE_RISK_THRESHOLD", 0.5),
            route_risk_penalty=_float(source, "GEORISK_ROUTE_RISK_PENALTY", 4.0),
            evaluation_fraction=_float(source, "GEORISK_EVALUATION_FRACTION", 0.2),
            delay_noise_std_hours=_float(source, "GEORISK_DELAY_NOISE_STD_HOURS", 0.25),
            delay_model=source.get("GEORISK_DELAY_MODEL", "linear"),
            acceptance_required=_boolean(source, "GEORISK_ACCEPTANCE_REQUIRED", True),
            strict_validation=_boolean(source, "GEORISK_STRICT_VALIDATION", False),
            log_level=source.get("GEORISK_LOG_LEVEL", "INFO").upper(),
            deployment_mode=source.get("GEORISK_DEPLOYMENT_MODE", "local").strip().lower(),
            aws_region=source.get("AWS_REGION", "us-east-1").strip(),
            aws_stack_name=source.get("GEORISK_AWS_STACK_NAME", "georisk-foundation").strip(),
            aws_project_bucket_name=source.get("GEORISK_AWS_PROJECT_BUCKET_NAME", "").strip(),
            aws_glue_role_arn=source.get("GEORISK_AWS_GLUE_ROLE_ARN", "").strip(),
            deployment_plan_path=Path(
                source.get(
                    "GEORISK_DEPLOYMENT_PLAN_PATH",
                    "output/reports/phase8-deployment-plan.json",
                )
            ),
            foundation_template_path=Path(
                source.get(
                    "GEORISK_FOUNDATION_TEMPLATE",
                    "infra/aws/georisk-foundation.template.json",
                )
            ),
        )

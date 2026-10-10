"""Typed runtime configuration for local runs and AWS deployment."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping


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
    vessel_count: int = 10
    telemetry_points: int = 3
    resolution: int = 7
    input_dir: Path = Path("output/data")
    processed_dir: Path = Path("output/processed")
    lake_dir: Path = Path("output/lake")
    dashboard_dir: Path = Path("output")
    report_path: Path = Path("output/reports/phase6-run.json")
    route_risk_threshold: float = 0.5
    route_risk_penalty: float = 4.0
    evaluation_fraction: float = 0.2
    acceptance_required: bool = True
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
            seed=_integer(source, "GEORISK_SEED", 7),
            vessel_count=_integer(source, "GEORISK_VESSEL_COUNT", 10),
            telemetry_points=_integer(source, "GEORISK_TELEMETRY_POINTS", 3),
            resolution=_integer(source, "GEORISK_RESOLUTION", 7),
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
            acceptance_required=_boolean(source, "GEORISK_ACCEPTANCE_REQUIRED", True),
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

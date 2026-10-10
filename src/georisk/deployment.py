"""Safe, local validation and planning for the AWS GeoRisk foundation."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from georisk.config import GeoRiskConfig


REQUIRED_RESOURCES = {
    "DataBucket": "AWS::S3::Bucket",
    "TelemetryStream": "AWS::Kinesis::Stream",
    "WeatherAlertStream": "AWS::Kinesis::Stream",
    "GlueDatabase": "AWS::Glue::Database",
    "GlueLogGroup": "AWS::Logs::LogGroup",
    "AthenaWorkGroup": "AWS::Athena::WorkGroup",
}


def load_template(path: str | Path) -> dict[str, Any]:
    """Load a CloudFormation JSON template and require an object root."""
    target = Path(path)
    if not target.is_file():
        raise FileNotFoundError(f"foundation template does not exist: {target}")
    try:
        value = json.loads(target.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"foundation template is not valid JSON: {target}") from exc
    if not isinstance(value, dict):
        raise ValueError("foundation template must contain a JSON object")
    return value


def validate_foundation_template(template: Mapping[str, Any]) -> dict[str, Any]:
    """Check the bounded-demo AWS foundation's required safety controls."""
    checks: list[dict[str, Any]] = []

    def check(name: str, passed: bool, detail: str) -> None:
        checks.append({"name": name, "passed": bool(passed), "detail": detail})

    resources = template.get("Resources", {})
    check("resource_types", isinstance(resources, dict), "resource map is present")
    resources = resources if isinstance(resources, dict) else {}

    def resource_properties(logical_id: str) -> dict[str, Any]:
        resource = resources.get(logical_id, {})
        if not isinstance(resource, dict):
            return {}
        properties = resource.get("Properties", {})
        return properties if isinstance(properties, dict) else {}

    for logical_id, resource_type in REQUIRED_RESOURCES.items():
        resource = resources.get(logical_id, {})
        check(
            f"resource_{logical_id}",
            isinstance(resource, dict) and resource.get("Type") == resource_type,
            f"{logical_id} has type {resource_type}",
        )

    bucket = resource_properties("DataBucket")
    encryption = bucket.get("BucketEncryption", {})
    encryption = encryption if isinstance(encryption, dict) else {}
    public_block = bucket.get("PublicAccessBlockConfiguration", {})
    public_block = public_block if isinstance(public_block, dict) else {}
    lifecycle_config = bucket.get("LifecycleConfiguration", {})
    lifecycle_config = lifecycle_config if isinstance(lifecycle_config, dict) else {}
    lifecycle = lifecycle_config.get("Rules", [])
    lifecycle = lifecycle if isinstance(lifecycle, list) else []
    check(
        "bucket_encryption",
        bool(encryption.get("ServerSideEncryptionConfiguration")),
        "S3 server-side encryption is configured",
    )
    check(
        "bucket_public_access_block",
        all(public_block.get(key) is True for key in (
            "BlockPublicAcls",
            "BlockPublicPolicy",
            "IgnorePublicAcls",
            "RestrictPublicBuckets",
        )),
        "all S3 public access block controls are enabled",
    )
    check(
        "bucket_lifecycle",
        bool(lifecycle) and all(rule.get("Status") == "Enabled" for rule in lifecycle),
        "S3 lifecycle expiration is enabled",
    )

    for logical_id in ("TelemetryStream", "WeatherAlertStream"):
        properties = resource_properties(logical_id)
        check(
            f"{logical_id}_bounded",
            properties.get("ShardCount") == 1
            and properties.get("RetentionPeriodHours", 0) <= 24,
            f"{logical_id} is limited to one shard and at most 24 hours retention",
        )

    log_properties = resource_properties("GlueLogGroup")
    check(
        "log_retention",
        0 < log_properties.get("RetentionInDays", 0) <= 14,
        "Glue logs retain at most 14 days",
    )
    athena_properties = resource_properties("AthenaWorkGroup")
    check(
        "athena_enabled",
        athena_properties.get("State") == "ENABLED",
        "Athena workgroup is enabled",
    )
    passed = all(item["passed"] for item in checks)
    return {
        "status": "accepted" if passed else "rejected",
        "passed": passed,
        "checks": checks,
    }


def validate_deployment_settings(config: GeoRiskConfig) -> dict[str, Any]:
    """Validate AWS-only values without contacting or changing AWS."""
    checks: list[dict[str, Any]] = []

    def check(name: str, passed: bool, detail: str) -> None:
        checks.append({"name": name, "passed": bool(passed), "detail": detail})

    is_aws = config.deployment_mode == "aws"
    check("deployment_mode", is_aws, "deployment mode is AWS")
    check(
        "project_bucket_name",
        bool(config.aws_project_bucket_name)
        and 3 <= len(config.aws_project_bucket_name) <= 63,
        "an S3 bucket name is supplied",
    )
    check(
        "glue_role_arn",
        config.aws_glue_role_arn.startswith("arn:aws:iam::")
        and ":role/" in config.aws_glue_role_arn,
        "a least-privilege Glue role ARN is supplied",
    )
    passed = all(item["passed"] for item in checks)
    return {"status": "accepted" if passed else "rejected", "passed": passed, "checks": checks}


def build_deployment_plan(config: GeoRiskConfig) -> dict[str, Any]:
    """Create a reviewable plan; this function never provisions AWS resources."""
    template = load_template(config.foundation_template_path)
    template_result = validate_foundation_template(template)
    settings_result = validate_deployment_settings(config)
    plan = {
        "status": "ready" if template_result["passed"] and settings_result["passed"] else "blocked",
        "provisioning_performed": False,
        "region": config.aws_region,
        "stack_name": config.aws_stack_name,
        "template_path": str(config.foundation_template_path),
        "parameters": {
            "ProjectBucketName": config.aws_project_bucket_name,
            "GlueRoleArn": config.aws_glue_role_arn,
        },
        "template_validation": template_result,
        "settings_validation": settings_result,
    }
    return plan


def deployment_cli() -> None:
    """Write and print a local AWS deployment readiness plan."""
    config = GeoRiskConfig.from_env()
    plan = build_deployment_plan(config)
    config.deployment_plan_path.parent.mkdir(parents=True, exist_ok=True)
    config.deployment_plan_path.write_text(
        json.dumps(plan, indent=2, sort_keys=True), encoding="utf-8"
    )
    print(json.dumps(plan, indent=2, sort_keys=True))
    if not plan["status"] == "ready":
        raise SystemExit(1)


if __name__ == "__main__":
    deployment_cli()

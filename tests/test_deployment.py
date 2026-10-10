import copy
import pytest

from georisk.config import GeoRiskConfig
from georisk.deployment import (
    build_deployment_plan,
    load_template,
    validate_deployment_settings,
    validate_foundation_template,
)


def _template_path():
    return "infra/aws/georisk-foundation.template.json"


def test_foundation_template_has_required_controls():
    result = validate_foundation_template(load_template(_template_path()))
    assert result["status"] == "accepted"
    assert result["passed"] is True


def test_foundation_template_rejects_public_bucket_configuration():
    template = load_template(_template_path())
    unsafe = copy.deepcopy(template)
    unsafe["Resources"]["DataBucket"]["Properties"][
        "PublicAccessBlockConfiguration"
    ]["BlockPublicPolicy"] = False
    result = validate_foundation_template(unsafe)
    assert result["passed"] is False
    assert any(
        item["name"] == "bucket_public_access_block" and not item["passed"]
        for item in result["checks"]
    )


def test_aws_deployment_settings_require_role_and_bucket():
    config = GeoRiskConfig(
        deployment_mode="aws",
        aws_project_bucket_name="georisk-demo-bucket",
        aws_glue_role_arn="arn:aws:iam::123456789012:role/GeoRiskGlueRole",
    )
    assert validate_deployment_settings(config)["passed"] is True
    invalid = GeoRiskConfig(deployment_mode="aws")
    assert validate_deployment_settings(invalid)["passed"] is False


def test_build_deployment_plan_never_provisions_and_is_ready(tmp_path):
    config = GeoRiskConfig(
        deployment_mode="aws",
        aws_project_bucket_name="georisk-demo-bucket",
        aws_glue_role_arn="arn:aws:iam::123456789012:role/GeoRiskGlueRole",
        foundation_template_path=_template_path(),
        deployment_plan_path=tmp_path / "plan.json",
    )
    plan = build_deployment_plan(config)
    assert plan["status"] == "ready"
    assert plan["provisioning_performed"] is False
    assert plan["parameters"]["ProjectBucketName"] == "georisk-demo-bucket"


def test_template_and_plan_errors_are_explicit(tmp_path):
    missing = tmp_path / "missing.json"
    with pytest.raises(FileNotFoundError):
        load_template(missing)
    invalid = tmp_path / "invalid.json"
    invalid.write_text("not-json", encoding="utf-8")
    with pytest.raises(ValueError, match="valid JSON"):
        load_template(invalid)

# Configuration scaffold

`.env.example` documents names and safe placeholders only. Copy values into local untracked configuration or use an AWS profile/SSO session. Never add credentials, secret values, or generated outputs to Git.

Phase 6 uses `georisk.config.GeoRiskConfig` to validate local runtime
settings. The `georisk-run` command reads the process environment and writes
the full run report to `GEORISK_REPORT_PATH`. Configuration contains no AWS
credentials; authentication must come from an AWS profile, SSO session, or
workload role.

Phase 7 adds `GEORISK_ACCEPTANCE_REQUIRED` as an explicit deployment setting.
Keep it enabled for demonstrations so a run cannot be presented as complete
without passing the acceptance checks.

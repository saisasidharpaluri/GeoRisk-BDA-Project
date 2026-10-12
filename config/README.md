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

The scenario duration defaults to 120 minutes (`GEORISK_SCENARIO_DURATION_MINUTES`)
and sampling to 300 seconds (`GEORISK_TELEMETRY_INTERVAL_SECONDS`). Both must be
positive integers producing 1–1000 samples per vessel. `GEORISK_TELEMETRY_POINTS`
is retained for legacy `generate_events()` callers. Local file replay uses
`GEORISK_STREAM_DIR=output/stream` and `GEORISK_REPLAY_SPEED_FACTOR=0`; zero
disables pacing, and positive finite values scale event-time gaps.

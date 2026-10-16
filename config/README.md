# Runtime configuration

`.env.example` documents names and safe placeholders only. Copy values into local untracked configuration or use an AWS profile/SSO session. Never add credentials, secret values, or generated outputs to Git.

Phase 6 uses `georisk.config.GeoRiskConfig` to validate local runtime
settings. The `georisk-run` command loads `.env`, reads the process environment, and writes
the full run report to `GEORISK_REPORT_PATH`. Configuration contains no AWS
credentials; authentication must come from an AWS profile, SSO session, or
workload role.

`GEORISK_ACCEPTANCE_REQUIRED` controls the local run's acceptance exit status.
Keep it enabled for demonstrations so a run cannot be presented as complete
without passing the acceptance checks.

The scenario duration defaults to 120 minutes (`GEORISK_SCENARIO_DURATION_MINUTES`)
and sampling to 300 seconds (`GEORISK_TELEMETRY_INTERVAL_SECONDS`). Both must be
positive integers producing 1–1000 samples per vessel. `GEORISK_TELEMETRY_POINTS`
is retained for legacy `generate_events()` callers. Local file replay uses
`GEORISK_STREAM_DIR=output/stream` and `GEORISK_REPLAY_SPEED_FACTOR=0`; zero
disables pacing, and positive finite values scale event-time gaps.

`GEORISK_MAX_FUTURE_MINUTES` (default 60, integer 0–10080) bounds how far
telemetry may lie ahead of the processing wall clock. Later events are
quarantined as `future_event` and do not advance the watermark. The
`process`, `run`, and `spark-stream` commands accept `--max-future-minutes`.

`georisk-run` and `georisk-validate` load `GEORISK_ENV_FILE` (default `.env`)
when it exists. Paths are relative to the working directory. Shell values take
precedence. `load_env_file(path, override=False)` returns only applied keys;
`override=True` explicitly replaces existing values. Blank lines and full-line
`#` comments are ignored, whitespace is trimmed, and matching single/double
quotes are removed. Values are literal: no shell execution, interpolation, or
inline-comment expansion. Malformed assignments raise a line-numbered error
before any values are applied. Deployment commands retain their existing behavior.

Every unified `georisk` subcommand uses the same loader. `--env-file` selects a
file, and command-specific flags override both the shell and file values. Use
`georisk <command> --help` to see the supported flags and their environment names.

# Changelog

## Unreleased

### Added

- Five decision records for H3 resolution, coverage, output contracts, late
  events, and density windows.
- Demo runbook, architecture diagram, synthetic delay-model reference,
  limitations, and measured evaluation including a 30,000-event benchmark.
- Pinned development tools, Ruff formatting/lint, Mypy, pre-commit hooks, and
  CI for Python 3.10–3.13 plus a Python 3.11/Java 17 Spark job.
- An 85% statement-coverage gate, omitting the AWS deployment module only.

### Changed (dashboard presentation, 2026-10-16)

- Dashboard reorganised for presentation: header with a plain-language summary
  and scenario badges, a five-stage pipeline strip with this run's counts,
  time-scoped KPI cards including the route decision, readable Inspect cards
  instead of raw JSON, and tabs for hazard exposure, corridor density, delay
  model, route decision, and run quality (acceptance checks, record
  accounting, settings, history).
- Chart axes are labelled in UTC; previously the browser shifted them into
  the viewer's local time zone while the axis said UTC.
- New charts: vessels-in-hazard timeline, density trend, and a
  prediction-vs-label scatter with a reference diagonal. Route results show
  map labels instead of node IDs; the map centres on the route network.

### Fixed (2026-10-16 audit, see `PROJECT_AUDIT.md`)

- Contracts reject NaN/infinite numbers and accept only strict RFC 3339 UTC
  timestamps, identically on Python 3.10–3.13 and in Spark.
- Weather-alert rings must have three distinct positions, non-zero area, and
  no antimeridian-crossing edge; risk events now require identity, location,
  coverage mode, and full hazard provenance; risk/density rows check flag
  consistency, window order, and H3 cell/resolution agreement.
- Alert revisions apply as-of their issue time in local processing, Spark,
  route scoring, and the dashboard instead of retroactively replacing earlier
  versions.
- Alerts whose geometry H3 cannot cover are quarantined (`invalid_geometry`)
  instead of aborting the batch.
- Telemetry beyond `GEORISK_MAX_FUTURE_MINUTES` is quarantined
  (`future_event`) so one bad clock cannot advance the watermark.
- Spark keeps one replaceable rejected-alert snapshot instead of duplicating
  rejections every microbatch, and orders alert revisions deterministically.
- Route reports use comparable penalised costs for baseline and alternative;
  a threshold with no eligible alternative keeps the baseline visible; blocked
  edges are tracked per edge taken; long or pentagon-crossing lanes no longer
  fail H3 path tracing.
- Dashboard counts only alerts active at the latest event time, uses the
  processed run's H3 settings, orders timestamps chronologically, compares
  calendar-adjacent density windows, applies risk filters to the severity
  chart, shows processing lag, and adds delay feature context.
- Event IDs include the seed and event time, so different runs never collide.
- Local Parquet writes are atomic, partition dates are checked, and
  `georisk run` replaces its whole snapshot batch.
- Replay keeps counters on failure (`ReplayError.stats`), skips malformed
  JSON lines, and file batches are fsynced and leave no temp files.
- Both resolution-comparison entry points use the same timed scenario.
- Artifact manifests fail on missing files instead of omitting them.

### Changed

- Updated local implementation status and module descriptions; the original
  project-guide PDF is labeled archival.
- Applied consistent formatting and mechanical typing/import fixes without
  changing pipeline semantics.

### Repository decisions (2026-10-12)

- Keep `prompt.md` in the repository at its existing path.
- Keep the guide Markdown and archival PDF under `output/`.
- Retain `S3_BUCKET` in `.env.example`; removal has not been agreed with the
  AWS workstream. Runtime configuration uses `GEORISK_AWS_PROJECT_BUCKET_NAME`.
- Keep the project unlicensed; no license grant is added.

Generated outputs, tool caches, coverage artifacts, local environments, and
credentials remain ignored. AWS resources have not been deployed.

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

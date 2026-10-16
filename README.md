# GeoRisk-Spark

GeoRisk-Spark is a four-person Big Data Analytics semester project. It processes synthetic vessel telemetry and marine hazard alerts locally with H3 and optional Spark, stores Parquet results, estimates demonstration delays, compares routes, and shows results in a Streamlit dashboard.

The local implementation provides the shared contract validator, deterministic
synthetic event generation, local JSONL replay, H3 indexing, hazard matching,
risk/density processing, Parquet analytics, delay evaluation, risk-aware route
comparison, Spark file streaming and MLlib GBT evaluation, and a local dashboard.
AWS resources have not been deployed.

## Workstreams

| Owner | Area | Module location |
| --- | --- | --- |
| Member 1 | Synthetic event generation, event contracts, Kinesis publishing | `src/georisk/ingestion/` |
| Member 2 | Spark Structured Streaming, H3 indexing, risk and density outputs | `src/georisk/processing/` |
| Member 3 | S3/Parquet lake, Glue Catalog, Athena, Spark MLlib delay baseline | `src/georisk/storage_analytics/` |
| Member 4 | Risk-weighted route comparison and Streamlit dashboard | `src/georisk/route_dashboard/` |

The JSON Schema contracts in `src/georisk/schemas/v1/` are the shared interfaces. Coordinate pairs use latitude/longitude for vessel events and GeoJSON longitude/latitude order for hazard geometry. H3 cell matching is an approximate spatial filter, not exact polygon containment.

## Local setup

Python 3.10–3.13 is supported. On this Windows PC, use Python 3.13:

```powershell
py -3.13 -m venv .venv
.venv\Scripts\Activate.ps1
.venv\Scripts\python -m pip install -e ".[test,dashboard]" -c constraints.txt
.venv\Scripts\python -m pytest -q
```

### Windows Smart App Control

If an import fails with `DLL load failed … An Application Control policy has
blocked this file`, use a Python 3.13 virtual environment and install with
`-c constraints.txt` as shown above. The constraints pin the verified package
versions, including pyarrow 21.0.0.

The shared contract validator is in `src/georisk/contracts.py`. It validates
events against the versioned schemas before later ingestion and processing
stages consume them. Timestamps must be strict RFC 3339 UTC
(`YYYY-MM-DDTHH:MM:SS[.fraction]` ending in `Z`, `+00:00`, or `-00:00`), parsed
identically on every supported Python version; NaN and infinite numbers are
rejected.

## Spark setup (Phase 7)

Use WSL2 Ubuntu on Windows. Spark's native Windows Hadoop helpers can be
blocked by Application Control. Keep the Windows `.venv` separate from the
Linux `.venv-spark` environment. Install Python 3.11 and Java 17 in Ubuntu,
then run from the repository root in WSL:

```bash
python3.11 -m venv .venv-spark
source .venv-spark/bin/activate
python -m pip install -e '.[test,spark]' -c constraints.txt
export PYSPARK_PYTHON="$PWD/.venv-spark/bin/python"
python -m pytest -m spark -q
```

Optionally, keep a private Java runtime in an untracked `.runtime-spark/`
directory (create it yourself; it is not part of the repository). To use it
in a fresh WSL shell, run:

```bash
export JAVA_HOME="$PWD/.runtime-spark/java"
export PATH="$JAVA_HOME/bin:$PATH"
export PYSPARK_PYTHON="$PWD/.venv-spark/bin/python"
source .venv-spark/bin/activate
```

`get_spark()` imports PySpark only when requested and uses UTC, four shuffle
partitions, and a disabled Spark UI. Spark tests skip when PySpark or Java is
missing; runtime errors are test failures. The Spark extra stays on 3.5.x and
is verified on Python 3.11 with Java 17; it is expected to work on 3.10+.
[AWS Glue release notes](https://docs.aws.amazon.com/glue/latest/dg/release-notes.html)
currently list Glue 5.1 (Spark 3.5.6, Python 3.11, Java 17); these local
prerequisites also match Glue 5.0. No AWS deployment is performed.

## Phase 2 local replay

Generate deterministic fixtures and write the two logical streams as JSONL:

```powershell
georisk generate --seed 7 --vessel-count 10 --input-dir output/data
```

The output files are `output/data/vessel-telemetry.jsonl` and
`output/data/weather-alerts.jsonl`. Each run replaces those files. Use
`replay_events()` with `publish_event()` and an injected Kinesis client for
tests, or with the default client when AWS credentials and streams are
configured. For local file-stream batches, run `georisk replay --speed 60`.

## Phase 3 local processing

Process the Phase 2 fixtures into validated risk and density outputs:

```powershell
georisk process --input-dir output/data --output-dir output/processed --resolution 7
```

The processor writes `risk-events.jsonl` and `density-metrics.jsonl`. It
expires alerts using `valid_until`, applies each alert revision only from its
issue time onward, retains the H3 resolution in every row, and deduplicates
vessels by H3 cell and 15-minute event-time window. Telemetry more than
`GEORISK_MAX_FUTURE_MINUTES` (default 60) ahead of the current time is
quarantined as `future_event`.

## Phase 4 storage and analytics

Generate and process the scenario, then write raw and curated outputs to a local
Parquet lake as part of the end-to-end run:

```powershell
georisk run --lake-dir output/lake
```

Datasets use the layout
`output/lake/<zone>/<dataset>/event_date=YYYY-MM-DD/part-<batch_id>.parquet`,
with telemetry and alerts in `raw`, and risk, density, and delay estimates in
`curated`. Use `write_partitioned()` to assign each row to its own UTC date;
`georisk run` passes `replace_batch=True` so a rerun removes its earlier files
from dates no longer present. Local files are written to a hidden temporary
file and atomically renamed. The lake helpers provide
validated writes, reads, risk summaries, and density summaries. Athena query
examples are in `src/georisk/storage_analytics/sql/`.

## Phase 5 routing and dashboard

Run the local dashboard after generating Phase 2 and Phase 3 outputs:

```powershell
py -m pip install -e ".[dashboard]"
georisk dashboard
```

The dashboard displays freshness and explicit no-data states. Its route view
compares the unrestricted fastest path with a threshold-filtered alternative
on the documented synthetic graph. Both total costs include the risk penalty;
when no path avoids the blocked lanes, the baseline remains and the
alternative shows "No eligible route".

## Phase 6 end-to-end run

Run every local stage and write a machine-readable report:

```powershell
georisk run
```

The report is written to `output/reports/phase6-run.json` and includes
generation, processing, Parquet paths, risk/density summaries, delay-model
metrics versus baseline, route comparison, and dashboard freshness. The AWS
foundation template is
`infra/aws/georisk-foundation.template.json`; review account, region, cost,
and teardown settings before deployment.

The local run uses an offshore Chennai scenario with timed hazards and slow
traffic clustered at the chokepoint. `GEORISK_SCENARIO_DURATION_MINUTES=120`
and `GEORISK_TELEMETRY_INTERVAL_SECONDS=300` produce 24 samples per configured
vessel across eight default density windows. `GEORISK_TELEMETRY_POINTS` is
legacy and only applies to `generate_events()`. Routing uses active hazards and
the density window at the latest telemetry time; `route.at_time` and
`route.edge_scores` record that decision's inputs, including per-edge reasons.

## Phase 7 acceptance and reproducibility

Run the final acceptance checks against the persisted report:

```powershell
georisk validate
```

The report now includes deterministic SHA-256 artifact manifests and
cross-phase acceptance checks for event accounting, risk/density outputs,
Parquet files, model-vs-baseline metrics, route comparison, and dashboard
freshness. A rejected validation exits with code 1.

Validation recomputes every manifest entry's SHA-256 digest. Missing or modified
artifacts fail `artifact_integrity`, with the affected paths in its detail.

Each run also executes small deterministic acceptance scenarios for active and
expired hazards, outside positions, duplicate density, late-event quarantine,
and explained detours. Overlap mode additionally checks a boundary cell missed
by center containment. Every expected `scenario_*` check must be present and
pass. `dashboard_labels` checks simulation status, H3 resolution, and units;
existing checks cover timestamps, Parquet outputs, and held-out model metrics.
The guide's S3/Athena criterion is not met: local Parquet and DuckDB SQL tests
cover query semantics only, and Athena acceptance remains unmet until the AWS
path is deployed and verified (out of scope for the local suite).

## Unified local CLI

After updating an existing checkout, reinstall the editable package with
`.venv\Scripts\python -m pip install --no-deps -e .` to register
the new `georisk` command. Use `georisk --help` or `georisk <command> --help`.

| Command | Result |
| --- | --- |
| `georisk generate` | Replace scenario JSONL files in the configured input directory |
| `georisk replay --speed 60` | Append atomic batches to `GEORISK_STREAM_DIR`; 0 disables pacing |
| `georisk process` | Write risk, density, quarantine, and run metadata JSONL/JSON |
| `georisk run` | Generate data and run the complete pipeline, lake, model, and acceptance |
| `georisk validate --report-path output/reports/phase6-run.json` | Recheck report criteria and artifact hashes |
| `georisk compare-resolutions` | Write `resolution-comparison.json` beside the configured report |
| `georisk dashboard` | Launch Streamlit using the current Python environment |
| `georisk spark-stream` | Drain available local stream batches; `--watch` keeps the stream running |

`spark-stream` is offered only when PySpark imports; it requires the configured
Spark/Java environment. Use `--stream-dir`, `--output-dir`, and `--checkpoint-dir`
to isolate a stream run. Dashboard requires the `dashboard` extra.

Configuration precedence is command flags, then shell environment, then `.env`,
then defaults. Select another file with `--env-file path` or `GEORISK_ENV_FILE`.
For example:

```powershell
georisk run --input-dir output/data/demo --processed-dir output/processed/demo --lake-dir output/lake/demo --report-path output/reports/demo.json --route-risk-penalty 9
georisk dashboard --input-dir output/data/demo --processed-dir output/processed/demo --route-risk-penalty 9
```

Commands return 0 on success, 1 for rejected acceptance or operational errors,
2 for argparse usage errors, and 130 when interrupted. Dashboard propagates
Streamlit's exit status. `--no-acceptance-required` permits a run with rejected
acceptance to exit 0; validation still exits 1. Existing `georisk-run`,
`georisk-validate`, and `georisk-compare-resolutions` entry points remain available;
the last uses the same timed scenario as `georisk compare-resolutions`.
`georisk replay` skips malformed JSON lines, reports them on stderr, and counts
them as generated and rejected, so the command exits 1.

## Phase 8 deployment readiness

Phase 8 adds a read-only deployment readiness plan for the AWS foundation.
It validates the CloudFormation template and deployment parameters without
contacting AWS or creating resources:

```powershell
$env:GEORISK_DEPLOYMENT_MODE="aws"
$env:GEORISK_AWS_PROJECT_BUCKET_NAME="your-globally-unique-bucket"
$env:GEORISK_AWS_GLUE_ROLE_ARN="arn:aws:iam::123456789012:role/your-least-privilege-role"
georisk-deployment-check
```

The plan is written to
`output/reports/phase8-deployment-plan.json`. Review the account, region,
bucket, IAM permissions, quotas, cost limits, and teardown procedure before
running any separate AWS deployment command.

## Planned AWS path

Amazon Kinesis Data Streams will ingest vessel and weather events; AWS Glue will run Spark Structured Streaming; Amazon S3 will hold date-partitioned Parquet; and the Glue Data Catalog plus Athena will support historical SQL. Start paid or long-running resources only for short development or demo windows. AWS resources have not been provisioned.

## Collaboration

- Use one feature branch per cohesive task and open a pull request to `main` for teammate review.
- Keep commits focused and push after each integrated milestone. Never force-push `main`.
- Keep AWS credentials in a local profile or SSO session. Never commit credentials or generated data.
- Agree on schema changes with all owners and update `src/georisk/schemas/v1/` and the guide together.

## Project guide

See [the current team project guide](output/GeoRisk-Spark-Team-Project-Guide.md)
for the problem statement, intended cloud architecture, event contracts, AWS
cost guidance, and acceptance criteria. Workstream ownership is listed above;
the guide's suggested schedule is not the implementation checklist. The
[original PDF](output/pdf/GeoRisk-Spark-Team-Project-Guide.pdf) predates the local
implementation and is retained as an archival artifact.

## Decision records

- [0001: H3 resolution](docs/decisions/0001-h3-resolution.md)
- [0002: Coverage mode](docs/decisions/0002-coverage-mode.md)
- [0003: Output contracts](docs/decisions/0003-output-contracts.md)
- [0004: Late events and watermark](docs/decisions/0004-late-events.md)
- [0005: Density window](docs/decisions/0005-density-window.md)

## Project documentation

- [Demo runbook](docs/demo-runbook.md)
- [Delay model](docs/delay-model.md)
- [Limitations](docs/limitations.md)
- [Evaluation](docs/evaluation.md)
- [Architecture](docs/architecture.md)

## Development checks

```powershell
python -m pip install -e ".[test,dashboard,dev]" -c constraints.txt
python -m ruff check .
python -m ruff format --check .
python -m mypy src
python -m pytest -q
python -m pre_commit install
```

Ruff checks Python 3.10-compatible syntax and 100-column formatting. Hooks use
Ruff's pinned check and format versions. AWS-owned `deployment.py` and
`kinesis_producer.py` are excluded from Ruff changes; the S3 key expression in
`lake.py` retains its existing line length. Mypy checks all source modules,
allowing missing third-party stubs. Tests guard behavior through these
mechanical cleanups; optional dependencies have explicit skip reasons.

## Repository status

See [CHANGELOG.md](CHANGELOG.md) for changes and repository decisions. Prompt
notes and the guide retain their current locations. `S3_BUCKET` remains in the
example environment for AWS-team coordination. The project is currently
unlicensed; no license grant is provided.

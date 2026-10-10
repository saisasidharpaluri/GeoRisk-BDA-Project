# GeoRisk-Spark

GeoRisk-Spark is a four-person Big Data Analytics semester project. The planned system will process synthetic vessel telemetry and marine hazard alerts with H3 and Spark, store results on AWS, estimate demonstration delays, compare safer routes, and show results in a dashboard.

Phases 1-5 currently provide the shared contract validator, deterministic
synthetic event generation, local JSONL replay, H3 indexing, hazard matching,
risk/density processing, Parquet analytics, delay evaluation, risk-aware route
comparison, and a local dashboard. AWS infrastructure remains planned for a
later deployment phase.

## Workstreams

| Owner | Area | Scaffold location |
| --- | --- | --- |
| Member 1 | Synthetic event generation, event contracts, Kinesis publishing | `src/georisk/ingestion/` |
| Member 2 | Spark Structured Streaming, H3 indexing, risk and density outputs | `src/georisk/processing/` |
| Member 3 | S3/Parquet lake, Glue Catalog, Athena, Spark MLlib delay baseline | `src/georisk/storage_analytics/` |
| Member 4 | Risk-weighted route comparison and Streamlit dashboard | `src/georisk/route_dashboard/` |

The JSON Schema contracts in `schemas/v1/` are the shared interfaces. Coordinate pairs use latitude/longitude for vessel events and GeoJSON longitude/latitude order for hazard geometry. H3 cell matching is an approximate spatial filter, not exact polygon containment.

## Local setup

Use Python 3.10 or newer:

```powershell
py -m venv .venv
.venv\Scripts\Activate.ps1
py -m pip install -e ".[test]"
py -m pytest
```

The shared contract validator is in `src/georisk/contracts.py`. It validates
events against the versioned schemas before later ingestion and processing
stages consume them.

## Phase 2 local replay

Generate deterministic fixtures and write the two logical streams as JSONL:

```powershell
py -c "from georisk.ingestion.generator import generate_events, write_jsonl; write_jsonl(generate_events(seed=7, vessel_count=10), 'output/data')"
```

The output files are `output/data/vessel-telemetry.jsonl` and
`output/data/weather-alerts.jsonl`. Each run replaces those files. Use
`replay_events()` with `publish_event()` and an injected Kinesis client for
tests, or with the default client when AWS credentials and streams are
configured.

## Phase 3 local processing

Process the Phase 2 fixtures into validated risk and density outputs:

```powershell
py -c "from georisk.processing.stream_job import run_streaming_job; run_streaming_job({'input_dir': 'output/data', 'output_dir': 'output/processed', 'resolution': 7})"
```

The processor writes `risk-events.jsonl` and `density-metrics.jsonl`. It
expires alerts using `valid_until`, retains the H3 resolution in every row,
and deduplicates vessels by H3 cell and 15-minute event-time window.

## Phase 4 storage and analytics

Write curated outputs to a local Parquet lake:

```powershell
py -c "from georisk.storage_analytics.lake import write_dataset; import json; from pathlib import Path; records=[json.loads(line) for line in Path('output/processed/risk-events.jsonl').read_text().splitlines()]; write_dataset(records, 'output/lake', '2026-01-01', dataset_name='risk_events')"
```

Datasets use the layout
`output/lake/<dataset>/event_date=YYYY-MM-DD/`. The lake helpers provide
validated writes, reads, risk summaries, and density summaries. Athena query
examples are in `src/georisk/storage_analytics/sql/`.

## Phase 5 routing and dashboard

Run the local dashboard after generating Phase 2 and Phase 3 outputs:

```powershell
py -m pip install -e ".[dashboard]"
streamlit run src/georisk/route_dashboard/dashboard.py
```

The dashboard displays freshness and explicit no-data states. Its route view
compares the unrestricted fastest path with a threshold-filtered alternative
on the documented synthetic graph.

## Phase 6 end-to-end run

Run every local stage and write a machine-readable report:

```powershell
georisk-run
```

The report is written to `output/reports/phase6-run.json` and includes
generation, processing, Parquet paths, risk/density summaries, delay-model
metrics versus baseline, route comparison, and dashboard freshness. The AWS
foundation template is
`infra/aws/georisk-foundation.template.json`; review account, region, cost,
and teardown settings before deployment.

## Phase 7 acceptance and reproducibility

Run the final acceptance checks against the persisted report:

```powershell
georisk-validate
```

The report now includes deterministic SHA-256 artifact manifests and
cross-phase acceptance checks for event accounting, risk/density outputs,
Parquet files, model-vs-baseline metrics, route comparison, and dashboard
freshness. A rejected validation exits with code 1.

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
- Agree on schema changes with all owners and update `schemas/v1/` and the guide together.

## Project guide

See [the team project guide](output/pdf/GeoRisk-Spark-Team-Project-Guide.pdf) for the problem statement, architecture, role responsibilities, milestones, event contracts, AWS cost guidance, and acceptance criteria.

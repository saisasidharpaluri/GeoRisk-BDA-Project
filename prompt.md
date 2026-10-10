Remember the code should be

- industry standard
- well-documented
- maintainable
- well tested through tests
- production level

Also ensure that all the env variables must be there in .env, if you dont have an idea about env variable values, just put placeholder, I will replace later

Phase 3 implementation note:
- H3 Python dependency is pinned in pyproject.toml.
- Local processing uses H3 cell-center candidate coverage at resolution 7 by default.
- GEORISK_RESOLUTION, GEORISK_DENSITY_WINDOW_MINUTES, GEORISK_INPUT_DIR, and
  GEORISK_OUTPUT_DIR are documented in .env.example.
- The local runner reads Phase 2 JSONL and writes risk-events.jsonl and
  density-metrics.jsonl; AWS Glue integration remains a deployment adapter over
  this validated processing behavior.

Phase 4 implementation note:
- Parquet datasets are written under dataset/event_date=YYYY-MM-DD partitions.
- GEORISK_LAKE_DIR, GEORISK_ATHENA_DATABASE, and GEORISK_ATHENA_OUTPUT_S3 are
  documented in .env.example.
- The delay baseline uses the documented synthetic formula, a chronological
  holdout split, and MAE/RMSE compared with a training-mean baseline.

Phase 5 implementation note:
- Routing uses a deterministic Dijkstra implementation with unrestricted
  baseline and risk-threshold-filtered alternative paths.
- GEORISK_DASHBOARD_DATA_DIR, GEORISK_ROUTE_RISK_THRESHOLD, and
  GEORISK_ROUTE_RISK_PENALTY are documented in .env.example.
- The dashboard is local-first, loads Phase 2/3 JSONL outputs, and explicitly
  displays no-data and freshness states. Streamlit is an optional dependency.

Phase 6 implementation note:
- `georisk.config.GeoRiskConfig` validates all local runtime settings from
  environment variables without handling credentials.
- `georisk-run` executes generation, H3 processing, Parquet storage, delay
  evaluation, routing, dashboard summaries, and writes a JSON run report.
- `infra/aws/georisk-foundation.template.json` defines the low-cost AWS
  foundation with encrypted/lifecycle-managed S3, two Kinesis streams, Glue
  Catalog, CloudWatch logs, and Athena. Use an existing least-privilege Glue
  role and deploy only for bounded demonstrations.

Phase 7 implementation note:
- `georisk.operations` adds SHA-256 artifact manifests and report-driven
  acceptance checks for every project acceptance criterion.
- `georisk-validate` validates a persisted run report and exits non-zero when
  any criterion fails.
- `GEORISK_ACCEPTANCE_REQUIRED` is documented in `.env.example` and `.env`.
- Generated runtime outputs remain ignored and should be retained only as
  evaluation evidence during the approved demonstration window.

Phase 8 implementation note:
- `georisk-deployment-check` performs a local, read-only readiness check for
  the AWS foundation; it never provisions or modifies AWS resources.
- The check validates CloudFormation resource types, encryption, public-access
  blocking, lifecycle, bounded Kinesis retention, log retention, and Athena
  readiness.
- `GEORISK_DEPLOYMENT_MODE`, `GEORISK_AWS_STACK_NAME`,
  `GEORISK_AWS_PROJECT_BUCKET_NAME`, `GEORISK_AWS_GLUE_ROLE_ARN`,
  `GEORISK_DEPLOYMENT_PLAN_PATH`, and `GEORISK_FOUNDATION_TEMPLATE` are
  documented in `.env.example` and `.env`.
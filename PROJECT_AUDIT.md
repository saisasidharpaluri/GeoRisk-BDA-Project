# GeoRisk-Spark — Project Audit (Agenda vs. Implementation)

Audit date: 2026-10-16 · Commit audited: `dddf1b2` (main) · Reference spec:
[output/GeoRisk-Spark-Team-Project-Guide.md](output/GeoRisk-Spark-Team-Project-Guide.md)

Each finding was checked against the code, and each finding marked **(verified)** was also reproduced.
Line numbers refer to commit `dddf1b2`.

---

## 0. Fix status (updated 2026-10-16)

AWS work and anything that can't run locally were out of scope for this pass.
After the fixes:
- ruff, ruff format and mypy are clean.
- 429 tests pass and 4 Spark modules are skipped (no PySpark on this machine).
- A fresh `georisk run` followed by `georisk validate` passes all 17 checks.

| ID | Status | Notes |
|---|---|---|
| P1-1 NaN | ✅ Fixed | Contracts reject non-finite numbers; quarantine and JSONL output stay valid JSON |
| P1-2 Revisions | ✅ Fixed | As-of revision intervals in local processing, Spark, route scoring and the dashboard |
| P1-3 Degenerate geometry | ✅ Fixed | Contract rejects degenerate, zero-area and antimeridian rings; any remaining H3 failure → `invalid_geometry` quarantine |
| P1-4 Future timestamps | ✅ Fixed | `GEORISK_MAX_FUTURE_MINUTES` (default 60) → `future_event` quarantine (local and Spark) |
| P1-5 Spark alert quarantine | ✅ Fixed | Single atomically replaced `rejected-alerts.jsonl` snapshot |
| P1-6 Route costs | ✅ Fixed | Baseline and alternative both use hours + penalty × risk |
| P1-7 No alternative | ✅ Fixed | `alternative: None` with an explanation; baseline stays visible |
| P1-8 Active alert count | ✅ Fixed | Counts alerts active at the latest event time |
| P1-9 Resolution mismatch | ✅ Fixed | Dashboard uses run-metadata settings; scoring rejects density rows at another resolution |
| P1-10 `s3://` path | ⏸ Deferred | AWS-only path, left for the AWS workstream |
| P1-11 Event-ID collisions | ✅ Fixed | IDs include the seed and event time |
| P1-12 Timestamp parsing | ✅ Fixed | One strict RFC 3339 parser shared everywhere, plus the matching Spark regex |
| P1 lower-severity bugs | ✅ Fixed, except the Kinesis items (AWS) | Replay counters, malformed replay lines, H3 path fallback, parallel edges, chronological timestamp ordering, `zip(strict=True)`, strict manifest, Spark tie order |
| P2-1 Contract weaknesses | ✅ Fixed | Required risk fields and cross-field checks |
| P2-5 Dashboard gaps | ◐ Partly fixed | Processing lag, filtered chart, calendar-adjacent windows and delay feature context are done. Cell selection, drawing hazard H3 cells and the Athena adapter remain |
| P2-7 Env variables | ✅ Fixed | `.env` now has every key in `.env.example`. The five unused AWS variables remain, for the AWS team |
| P2-8 Docs | ✅ Fixed | Contradictions corrected; `SUMMARY.md` rewritten |
| P3-3 Lake writes | ✅ Fixed | Atomic writes, partition-date check, `replace_batch` snapshot mode |
| P3-5 File stream | ✅ Fixed | `fsync` before rename; no orphaned temp files |
| P0-1 / P0-2 / P0-3 | ⏸ Open | Architecture and AWS work: Spark as the primary engine, GBT as the reported model, Glue/Athena deployment |
| P2-2 / P2-3 / P2-4, P3-2 / P3-6 / P3-7 | ⏸ Open | Feature or refactor work: route-aware delay, exact refinement, a larger graph, `dashboard.py` split, process items |

Still to do locally: delete the stale `output/{data,processed,lake,reports}`, then run `georisk run` and `georisk validate`.

---

## 1. What the project is supposed to be

GeoRisk-Spark is a Big Data Analytics semester project. It turns **moving vessels and
changing marine hazards into H3 cell events**, then uses **streaming analytics** for four things:

1. Flag vessels that may be exposed to active hazards (H3 candidate match).
2. Measure corridor congestion as 15-minute unique-vessel density per H3 cell.
3. Estimate delay with a **Spark MLlib Gradient-Boosted Tree**, compared against a mean baseline.
4. Compare a baseline route with a risk-aware alternative on a port/sea-lane graph.

All of this is presented in a Streamlit + PyDeck dashboard. The target architecture
is **Kinesis (2 streams) → AWS Glue Spark Structured Streaming → S3 Parquet (raw and
curated, `event_date` partitions) → Glue Data Catalog → Athena**. The guide defines
six acceptance criteria (guide §7):

| # | Acceptance criterion | Status today |
|---|---|---|
| A1 | Telemetry inside active hazard coverage raises an alert; points clearly outside do not | ✅ Local scenario passes. ⚠️ Revisions can break it (P1-2) |
| A2 | Expired hazards stop matching; duplicate messages don't inflate density | ✅ Local scenario passes (the duplicate test is narrow, P3-4) |
| A3 | 15-minute cell density **queryable from S3 with Athena** | ❌ Not met. Only local Parquet + DuckDB; no Glue tables; S3 write path is broken (P0-3, P1-10) |
| A4 | Delay estimates evaluated against a mean baseline on a held-out period | ✅ for linear ridge. ⚠️ The guide's GBT is not the default and has no current evaluation (P0-2) |
| A5 | A route above the risk threshold produces an explainable alternative | ✅ Explanation exists. ⚠️ Cost reporting is wrong; the comparison disappears when no alternative exists (P1-6, P1-7) |
| A6 | Dashboard shows outputs with timestamps, units, freshness, and simulation labels | ✅ Mostly. ⚠️ No processing lag; alert count is wrong (P1-8, P2-5) |

---

## 2. Work done so far

The repository has about 12k lines in 120 tracked files and 73 commits. A fresh run of the
local demo **works and passes all 17 acceptance checks**. I verified this today with
`georisk run` into a clean scratch directory:

| Result | Value |
|---|---|
| Events | 242 generated (240 telemetry + 2 alerts), 0 rejected / quarantined |
| Risk / density rows | 240 risk events (140 alerted, 9 of 10 vessels exposed), 76 density rows |
| Model (linear, 48 held-out rows) | MAE 0.194 h / RMSE 0.232 h vs. baseline 1.349 h / 1.373 h |
| Route | `port_a→chokepoint→port_b` (6.47 h, risk 1.42) → alternative `port_a→outer_lane→port_b` (8.88 h, risk 0.06) |
| Tests (Windows venv) | **335 passed, 7 skipped.** All Streamlit, PyDeck, PySpark, and DuckDB suites were skipped |

**Implemented well:**
- Versioned JSON-Schema contracts with valid and invalid examples.
- Deterministic seeded scenario generation.
- Atomic file-batch replay.
- Overlap and center H3 coverage.
- Event-time windows with half-open validity.
- Quarantine of bad records, plus late-event handling.
- Typed Parquet lake with UTC date partitions.
- SHA-256 artifact manifest with report re-validation.
- Edge risk computed from both severity and density.
- Five decision records, a runbook, and a limitations document that is largely honest.
- CI matrix for Python 3.10–3.13 plus a Spark job.

---

## 3. Findings in priority order

Priority meanings:
- **P0** — threatens the core agenda; it is what an evaluator will question first.
- **P1** — a real bug that produces wrong output or a crash.
- **P2** — a gap against the guide, or docs and config that contradict the code.
- **P3** — hygiene, maintainability, and process.

### P0 — Agenda-level gaps

#### P0-1. "Spark" is a side path; the real pipeline is single-process Python
- `georisk run` processes data with `run_streaming_job` from [stream_job.py](src/georisk/processing/stream_job.py). It is plain Python over in-memory lists, and despite its name it is a single batch ([pipeline.py:59](src/georisk/pipeline.py#L59)).
- The dashboard, report, lake, delay model, and acceptance checks all consume that Python output.
- `georisk spark-stream` ([spark_stream_job.py](src/georisk/processing/spark_stream_job.py)) writes to a **separate sink that nothing reads**. [docs/architecture.md](docs/architecture.md) says so itself.
- Inside the Spark job, Spark's own streaming features are not used:
  - It runs `readStream` + `foreachBatch`, then executes ordinary batch logic.
  - There is no `withWatermark`, no streaming aggregation, and no Spark-managed state.
  - The watermark and density keys are JSON or Parquet files maintained by hand ([spark_stream_job.py:81-109, 169-182](src/georisk/processing/spark_stream_job.py#L81-L109)).
  - Alerts are not streamed; all alert files are re-read with `spark.read` and `collect()`-ed on every micro-batch ([spark_stream_job.py:33](src/georisk/processing/spark_stream_job.py#L33)).
  - The H3 `pandas_udf` runs a per-row Python loop inside ([spark_transforms.py:97-106](src/georisk/processing/spark_transforms.py#L97-L106)).
  - Polygon-to-cell expansion runs on the driver.
- Scale evidence is tiny:
  - The default run has 242 events.
  - The only benchmark (30,000 events, about 6.2k events/s) measures the **Python** path, not Spark ([docs/evaluation.md](docs/evaluation.md)).
- **Impact:** for a Big Data / Spark course, the headline claim "Spark Structured Streaming" is not what produces the demo's results.
- **Fix:**
  1. Make Spark the primary processing path, at least as an option: `georisk run --engine spark`.
  2. Point the dashboard, lake, and report at the Spark sink.
  3. Use native `withWatermark` + `window` + dedup with `dropDuplicatesWithinWatermark`, or explain why not.
  4. Stream alerts as a second source, or use a broadcast state refresh.
  5. Run and document a Spark scale benchmark (≥1M events) next to the Python one.

#### P0-2. The delay model is linear ridge by default, but the guide specifies MLlib GBT
- The default is `GEORISK_DELAY_MODEL=linear` ([config.py:88](src/georisk/config.py#L88), [.env.example](.env.example)). GBT runs only with `--delay-model gbt` and needs a WSL/Java runtime.
- That runtime does not exist on this machine: there is no `.venv-spark` and no `.runtime-spark`, even though [README.md:59](README.md#L59) says it "lives in `.runtime-spark/`".
- [docs/evaluation.md](docs/evaluation.md) records only linear metrics. The single GBT number (MAE 0.268 h) comes from an earlier, separate run and is **worse than linear** (0.194 h).
- Linear MAE ≈ 0.194 h equals the noise floor, 0.25·√(2/π) ≈ 0.1995. The label is an almost-linear function of the same five features, so the model is learning the formula back, not anything about delay. This is documented, but it weakens the analytics story.
- **Fix:**
  1. Make GBT the presented model, or run both and report both.
  2. Record a reproducible GBT evaluation.
  3. Consider a label with interaction or non-linear terms so GBT actually has an advantage.
  4. Split the data so no vessel appears in both training and evaluation.

#### P0-3. The AWS path is not deployable even as code, and A3 (Athena) is unmet
- **Nothing is deployed.** The guide and README say so honestly. But the infrastructure-as-code is also missing key parts:
  - Neither CloudFormation template defines any `AWS::Glue::Table`, partition projection, `AWS::Glue::Job` (the streaming job), or IAM resource ([infra/aws/georisk-foundation.template.json](infra/aws/georisk-foundation.template.json)).
  - There is no `CREATE EXTERNAL TABLE` DDL anywhere.
  - The SQL files query `georisk_risk_events` / `georisk_density_metrics`, which exist in no catalog.
  - The `GlueRoleArn` parameter is never used by any resource. "Least privilege" is only a regex check on the ARN ([deployment.py:136-141](src/georisk/deployment.py#L136-L141)).
- **Kinesis** is a single-record `put_record` adapter ([kinesis_producer.py](src/georisk/ingestion/kinesis_producer.py)):
  - No `PutRecords` batching.
  - No retry or backoff on throttling.
  - No handling of `FailedRecordCount`.
  - No size check.
  - No CLI command uses it.
- **Athena claim contradicts the docs.** [README.md:176](README.md#L176) says the S3/Athena criterion is "covered locally by Parquet/SQL tests". [docs/limitations.md:37](docs/limitations.md#L37) correctly says that DuckDB tests do **not** verify Athena.
- **Fix:**
  1. Add Glue tables with `event_date` partitions (or partition projection) matching `arrow_schemas.py`.
  2. Add a Glue streaming job resource and a scoped IAM role.
  3. Add a Kinesis `PutRecords` publisher with retries, wired to `georisk replay --sink kinesis`.
  4. Perform one bounded real deployment and record the Athena query evidence. If the team won't deploy, state clearly that A3 is not met.

#### P0-4. Half the test suite never runs on the development machine, and CI is unverified
- The Windows `.venv` lacks streamlit, pydeck, pyspark, duckdb, ruff, and mypy. As a result:
  - The dashboard AppTest, map layers, SQL, and **all Spark tests are skipped**, and skipped tests look like passes.
  - Ruff and mypy have never run locally (`No module named ruff`).
- The `georisk` console script is not installed in `.venv`; only the legacy `georisk-run`, `georisk-validate`, and `georisk-deployment-check` are. The runbook's first command, `georisk --help`, fails.
- The CI Spark job was added in the latest commit and has no recorded green run. Missing PySpark or Java makes the Spark tests **skip** instead of fail ([tests/conftest.py](tests/conftest.py)).
- **Fix:**
  1. Reinstall with `pip install -e ".[test,dashboard,dev]" -c constraints.txt`.
  2. Confirm the CI run on GitHub is green.
  3. In the Spark CI job, make a missing PySpark or Java a hard failure, for example with `GEORISK_REQUIRE_SPARK=1`.

---

### P1 — Correctness bugs (concrete input → wrong output)

| ID | Bug | Where | Reproduction / impact | Fix |
|---|---|---|---|---|
| P1-1 | **NaN passes every contract** (verified) | [contracts.py](src/georisk/contracts.py), all `number` fields | `latitude=NaN` → `is_valid_event(...) == True`. `write_jsonl` then writes the literal `NaN`, which is invalid JSON. The Kinesis producer would send it. `FileStreamPublisher` (`allow_nan=False`) raises, so the whole replay aborts instead of quarantining one record. | Reject non-finite numbers in `validate_event` (custom format or a pre-check). Add tests. |
| P1-2 | **Alert revisions are applied retroactively** (verified) | [stream_job.py:130-137](src/georisk/processing/stream_job.py#L130-L137), [spark_transforms.py:128-135](src/georisk/processing/spark_transforms.py#L128-L135) | Alert v1 is issued at 12:00 (polygon P). v2 is issued at 13:00 and moves the polygon. A vessel inside P at 12:30 → `risk_alert=False`, because only the latest revision is kept for the whole batch. This breaks A1. A test locks this behaviour in as expected. | Do an as-of join: for each telemetry event, use the latest revision whose `event_time ≤ telemetry.event_time`. |
| P1-3 | **Degenerate geometry crashes the batch** | [h3_index.py](src/georisk/processing/h3_index.py), [stream_job.py:143](src/georisk/processing/stream_job.py#L143) | A ring of four identical points, or a ring around the pole, passes the contract. In `center` mode `polygon_to_cells` then raises `ValueError` and the whole batch aborts. Spark re-reads all alerts every batch, so it fails on every restart. | Catch errors per alert → quarantine the alert. Reject zero-area or self-intersecting rings in contract semantic checks. |
| P1-4 | **One future timestamp blocks later data** | [stream_job.py](src/georisk/processing/stream_job.py), [spark_stream_job.py:174-182](src/georisk/processing/spark_stream_job.py#L174-L182) | Batch `[2027-01-01, 2026-01-01T12:31, 12:32]` → the two real events are quarantined as late. In Spark the watermark file persists, so every later event is quarantined until the checkpoint is wiped. | Cap event time at processing time plus a skew allowance; quarantine anything beyond the cap as `future_event`. |
| P1-5 | **Spark duplicates alert quarantine on every batch** | [spark_stream_job.py:70-78, 162-163](src/georisk/processing/spark_stream_job.py#L70-L78) | All alerts are re-read per batch, so the same rejected alert is written to a new `batch-N.jsonl` each time. Malformed-record counts become inflated. | Track already-quarantined alert `event_id`s, or quarantine alerts only when their file first arrives. |
| P1-6 | **Report route costs can't be compared** (verified in today's run) | [route_optimizer.py:142-161](src/georisk/route_dashboard/route_optimizer.py#L142-L161) | Baseline `total_cost` = 6.47 = travel hours only, with no penalty. The alternative's 9.12 = 8.88 + 4×0.06 includes the penalty. The report suggests the baseline is "cheaper" on a different metric. The dashboard recomputes correctly, which hides the bug. | Compute both totals with the same penalty, or rename the field to `penalised_cost` and add it to both. |
| P1-7 | **No alternative path → the whole comparison disappears** | [route_optimizer.py:118](src/georisk/route_dashboard/route_optimizer.py#L118), [dashboard.py:867-868](src/georisk/route_dashboard/dashboard.py#L867-L868) | A threshold of 0.0 or 0.05, both allowed by the slider, raises an error → `route=None` → the baseline also vanishes from the table and map. The guide wants the baseline shown with edges marked unavailable. | Return `alternative=None` with a reason; keep rendering the baseline. |
| P1-8 | **`active_alert_count` counts every alert, not active ones** | [dashboard.py:163](src/georisk/route_dashboard/dashboard.py#L163) | Today's report says 2 at 13:55, but only H1 is active (H2 expired at 13:30). This value feeds the report and the KPI. | Filter by `valid_from ≤ t < valid_until` at the latest event time. |
| P1-9 | **Resolution mismatch silently zeroes congestion** | [dashboard.py:226, 859](src/georisk/route_dashboard/dashboard.py#L226), [edge_risk.py:83-86](src/georisk/route_dashboard/edge_risk.py#L83-L86) | Process at resolution 8, then open the dashboard with `.env` set to 7. The header says "res 8", but every edge's `max_density` becomes 0, so the routes differ from the report. | Take the resolution, coverage mode, and window from `run-metadata.json`; reject density rows at a different resolution. |
| P1-10 | **An `s3://` lake path is broken by `Path()`** (verified) | [config.py:199](src/georisk/config.py#L199), [lake.py:107-119](src/georisk/storage_analytics/lake.py#L107-L119) | `str(Path("s3://bucket/lake"))` → `s3:\bucket\lake` → `urlparse` gives an empty bucket → `put_object(Bucket="")`. The pipeline can never write to S3. There are no tests for the S3 branch. | Keep the lake destination as `str`; branch on the scheme before wrapping it in `Path`. Add a moto- or stub-based test. |
| P1-11 | **Event IDs collide across runs** | [generator.py:114](src/georisk/ingestion/generator.py#L114), [scenario.py:102,112](src/georisk/ingestion/scenario.py#L102), [spark_stream_job.py:59-67](src/georisk/processing/spark_stream_job.py#L59-L67) | IDs are `telemetry-{vessel}-{point}` with a fixed start date, identical for every seed. Spark drops IDs already present in the lake, so a second run with a different seed into the same directories **silently writes no new risk rows**. | Include the seed, run ID, or a UUID5 namespace in event IDs. |
| P1-12 | **Timestamp validity depends on the Python version** | [contracts.py:25-31](src/georisk/contracts.py#L25-L31) | `datetime.fromisoformat` on Python 3.11+ accepts non-RFC-3339 forms (`20260101T120000Z`, `2026-W01-4T12Z`). Python 3.10 rejects valid `…00.1Z`. Spark's `try_to_timestamp` disagrees with both. CI runs 3.10–3.13, so the same record can be valid or invalid depending on the runtime. | Validate with a strict RFC 3339 regex before parsing. |

Lower-severity bugs:
- `kinesis_producer.py:43-45`: an explicit `""` partition key silently falls back to the default, the `if not key` guard is dead code, and there is no 256-character key limit.
- `replay_events` loses its counters when the publisher fails.
- CLI `replay` crashes on one bad JSON line ([cli.py:154-158](src/georisk/cli.py#L154-L158)).
- H3 `grid_path_cells` raises `H3FailedError`, which the dashboard doesn't catch.
- Parallel edges confuse the blocked-edge report.
- The dashboard compares timestamps as strings.
- Spark revision ties and `dropDuplicates` keep an arbitrary row, unlike Python.
- `zip(..., strict=False)` in [pipeline.py:149](src/georisk/pipeline.py#L149) hides length mismatches.
- `artifact_manifest` silently skips missing files ([operations.py:39](src/georisk/operations.py#L39)).

---

### P2 — Gaps against the guide, and docs/config that contradict the code

#### P2-1. Contract weaknesses
- In [risk_event.schema.json](src/georisk/schemas/v1/risk_event.schema.json):
  - The guide requires `event_id` and coordinates, but they are **optional**.
  - In each match, `valid_from`, `valid_until`, and `hazard_type` are optional.
  - ADR 0003 says writers "always emit" these fields, but nothing enforces it.
- There are no cross-field checks outside weather alerts. All of these validate:
  - `risk_alert=true` with no matches;
  - `window_end < window_start`;
  - `h3_index: "not-h3"`;
  - bow-tie (self-intersecting) polygons;
  - antimeridian-spanning rings;
  - alert `event_time` years after `valid_until`.
- v1 received **breaking** changes (S13 ring closure, S14 speed and heading becoming required) without a version bump, which contradicts [schemas/README.md](src/georisk/schemas/README.md).
- The weather alert has no revision or cancel field, and no generator ever emits a revision.

#### P2-2. The delay estimate is not route-aware
- The guide asks for "estimated delay for an active route". The implementation estimates delay **per telemetry ping**, using straight-line (haversine) distance to `port_b` ([delay_features.py:55-67](src/georisk/storage_analytics/delay_features.py#L55-L67)).
- Route comparison never produces a delay estimate for the baseline or the alternative.
- Full-window density leaks positions from later in the same window into the features (this is documented).

#### P2-3. No exact geometry refinement
- The guide says candidates "can be refined with exact point-in-polygon checks". This is not done.
- In overlap mode, a point 0.005° outside the polygon edge is flagged.
- **Fix:** add an optional `refine=True` shapely/pure-Python ray-casting pass and report the counts before and after refinement.

#### P2-4. The routing graph is trivial
- The graph has four nodes and four directed edges, so there are exactly two possible paths ([route_optimizer.py:41-53](src/georisk/route_dashboard/route_optimizer.py#L41-L53)). The "alternative" is always the other one.
- The risk penalty is applied only when the baseline is blocked, so a cheaper safe route can lose to a riskier one below the threshold.
- GraphFrames was never evaluated or documented as the guide suggests; the fallback is a plain `heapq` Dijkstra.
- **Fix:** use a graph of 8–12 nodes with reverse edges, plus a short ADR on GraphFrames.

#### P2-5. Dashboard gaps against guide §5
- **Freshness:** there is no processing lag. "Age" is wall-clock time since processing, not processing time minus event time ([views.py:187](src/georisk/route_dashboard/views.py#L187)).
- **Risk summary:** the type, severity, and time filters change only the table; the KPIs and the severity chart ignore them.
- **Corridor density:**
  - You cannot select a single cell.
  - "Adjacent windows" means neighbouring non-empty windows ([views.py:123-131](src/georisk/route_dashboard/views.py#L123-L131)), so a gap hour is skipped.
- **Delay view:** no feature context and no per-route view. The schema stores the features, but `latest_delay_estimates` drops them.
- **Live map:**
  - The hazard's own H3 candidate cells are never drawn.
  - The risk-cell layer shows the whole run, while the hazards show only the latest time, with no label saying so.
  - Polygon holes are not drawn.
- **Missing pieces:**
  - The Athena query adapter for cloud-backed views is **absent** (only a docstring).
  - The dashboard can't read the Spark sink.
  - There is no `st.cache_data`, so every 10-second auto-refresh re-reads all JSONL files and scans the whole lake.

#### P2-6. Infrastructure template problems
- `georisk-lake-foundation.template.json` has these problems:
  - It is **rejected by the project's own preflight**, because it has no Kinesis streams.
  - It is undocumented.
  - It hard-codes the same Glue DB, workgroup, and log-group names as the main template, so both can't be deployed in one account and region.
- In the main template:
  - The 30-day bucket-wide expiry would delete streaming checkpoints.
  - Kinesis has no `StreamEncryption`.
  - Athena has no `BytesScannedCutoffPerQuery` and no enforced workgroup config.
  - The log group is unused.
- The preflight in [deployment.py](src/georisk/deployment.py) cannot catch any of the P0-3 gaps, yet "accepted" is presented as readiness.

#### P2-7. Environment variable inconsistencies
- [prompt.md](prompt.md) requires every variable to be in `.env`. Local `.env` is **missing 14 of the variables** listed in `.env.example`:
  - `GEORISK_ALLOWED_LATENESS_MINUTES`
  - `GEORISK_DELAY_MODEL`
  - `GEORISK_DELAY_NOISE_STD_HOURS`
  - `GEORISK_ENV_FILE`
  - `GEORISK_H3_COVERAGE_MODE`
  - `GEORISK_LOG_LEVEL`
  - `GEORISK_REPLAY_SPEED_FACTOR`
  - `GEORISK_SCENARIO_DURATION_MINUTES`
  - `GEORISK_SPARK_CHECKPOINT_DIR`
  - `GEORISK_SPARK_OUTPUT_DIR`
  - `GEORISK_STALE_AFTER_MINUTES`
  - `GEORISK_STREAM_DIR`
  - `GEORISK_STRICT_VALIDATION`
  - `GEORISK_TELEMETRY_INTERVAL_SECONDS`
- Five variables in `.env.example` and `.env` are **never read by any code**: `GEORISK_ATHENA_DATABASE`, `GEORISK_ATHENA_OUTPUT_S3`, `S3_BUCKET`, `KINESIS_TELEMETRY_STREAM`, `KINESIS_HAZARD_STREAM`. Stream names are hard-coded in [kinesis_producer.py:10-13](src/georisk/ingestion/kinesis_producer.py#L10-L13).
- `GEORISK_ATHENA_OUTPUT_S3` conflicts with the template's `s3://${ProjectBucketName}/athena-results/`.
- `S3_BUCKET` duplicates `GEORISK_AWS_PROJECT_BUCKET_NAME`.
- `GEORISK_ENV_FILE` set inside the env file has no effect.

#### P2-8. Documentation that contradicts code or other docs

| Doc | Statement | Reality |
|---|---|---|
| [SUMMARY.md](SUMMARY.md) (untracked, written 2026-10-12, before the S01–S62 commits) | `speed_knots`/`heading_deg` optional; cell-center coverage only; three points per vessel; linear-only model; dashboard is counts plus a table; "late-event/watermark policy, quarantine storage" listed as future work; `schemas/v1/` at the repo root; `part-00000` at `<root>/<dataset>/…` | All superseded: speed/heading required (S14), overlap is the default (S16), 120-min timed scenario, GBT option, a full PyDeck dashboard, watermark and quarantine implemented, schemas under `src/georisk/schemas/v1/`, lake at `<root>/<zone>/<dataset>/…`. **This is the document most likely to be used for viva prep, and it is the most outdated.** |
| [README.md:49](README.md#L49), [demo-runbook.md:3](docs/demo-runbook.md#L3), [evaluation.md:67](docs/evaluation.md#L67) | "run from `project/`" | There is no `project/` directory; the repo root is `GeoRisk/` |
| [README.md:59](README.md#L59) | "The isolated runtime … lives in `.runtime-spark/`" | Does not exist on this machine |
| [README.md:176](README.md#L176) | The S3/Athena criterion is "covered locally" | [limitations.md:37](docs/limitations.md#L37) says it is not; P0-3 |
| [README.md:112-113](README.md#L112) | Curated zone = risk + density | `delay_estimates` is also curated |
| [demo-runbook.md:52-54](docs/demo-runbook.md#L52), [limitations.md:26](docs/limitations.md#L26) | "Stale is expected even immediately after processing" | Freshness is measured from `processed_at` (wall clock), so a new run shows **Fresh**, and [test_dashboard_app.py:73](tests/test_dashboard_app.py#L73) asserts "Fresh". The presenter would contradict the screen. |
| [processing/README.md:10](src/georisk/processing/README.md) | "using Spark Structured Streaming in AWS Glue"; lists `stream_job.py` as the stream entry point | Not in Glue; `stream_job.py` is batch; the Spark modules are not listed |
| [storage_analytics/README.md:3,7,33](src/georisk/storage_analytics/README.md) | The module owns Glue Catalog tables and "registers" the layout; the model "can be mapped to MLlib" | No catalog code exists; GBT is already implemented in the same module |
| [route_dashboard/README.md:14 vs 33](src/georisk/route_dashboard/README.md) | "unrestricted fastest baseline" vs "an eligible fastest baseline is retained" | Self-contradictory |
| [evaluation.md:7-9 vs 70](docs/evaluation.md) | Results recorded under `output/reports/phase10/` | Reproduction command writes to `output/reports/evaluation/` |
| `georisk compare-resolutions` vs `georisk-compare-resolutions` | Same output file name | They use different generators (`generate_scenario` vs `generate_events`), so they write different data to `resolution-comparison.json` |
| [ingestion/README.md](src/georisk/ingestion/README.md) | Example uses `generate_events`; sorting is by `(event_time, vessel_id)` | The CLI uses `generate_scenario`, which sorts by `(event_time, event_id)` |
| [spark_session.py:17](src/georisk/spark_session.py#L17) | Spark requires Python 3.11 | `pyproject` allows `>=3.10` for the `spark` extra |
| Guide §7 "Current code status" | "tested dashboard panels"; "Local Spark supports …" | The tests exist but were all skipped on the dev machine (P0-4) |

---

### P3 — Hygiene, maintainability, process

- **P3-1. Local outputs are stale.** `output/` holds a pre-S01 run: 30 telemetry rows, no `run-metadata.json`, no delay estimates, and only `density_metrics` and `risk_events` in the lake. `georisk validate` against the default report is **rejected with 8 failing checks** (verified). A dashboard opened on it shows "Unavailable" freshness. **Before any demo:** delete or rename the old `output/{data,processed,lake,reports}`, then run `georisk run` followed by `georisk validate`.
- **P3-2. `dashboard.py` is 979 lines.** It mixes CSS and HTML, data loaders that the core pipeline imports (`pipeline.py` → `route_dashboard.dashboard`, so the pipeline depends on the UI module), and 16 render functions. Other duplication and hard-coding:
  - `_utc` and the latest-revision logic are duplicated in `views.py` and `edge_risk.py`.
  - The colour palette is defined in three places.
  - Route endpoints are hard-coded to `port_a` and `port_b`.
  - **Fix:** move the loaders to `georisk/dashboard_data.py` and share a utilities module.
- **P3-3. Lake write semantics.**
  - The pipeline always writes `part-00000`. Reruns that produce different dates leave old partitions behind, which are then counted again.
  - Local Parquet writes go straight to the final path, so a crash leaves a corrupt file.
  - `write_dataset` accepts an `event_date` that doesn't match the records.
- **P3-4. Test quality.**
  - The invalid-example tests only check that *an* error is raised, not the reason.
  - The duplicate-density scenario uses identical event IDs and never tests one vessel with *different* IDs in the same cell and window, which is the actual guide rule.
  - The expiry test covers only the exact boundary.
  - The Spark/Python parity tests use ordered inputs, which hides ordering differences.
  - One test locks in the retroactive-revision bug (P1-2).
  - No tests for: NaN, degenerate or antimeridian geometry, the no-alternative UI path, resolution mismatch, the S3 lake branch, or the Kinesis default client.
- **P3-5. Kinesis and file stream details.**
  - A new boto3 client is created per call.
  - `file_stream.py` has no `fsync` before `os.replace`.
  - Orphaned `.part-*.tmp` files are never cleaned up.
- **P3-6. Collaboration process doesn't match the README.**
  - [README.md:239-240](README.md#L239) asks for one feature branch and pull request per task. All 73 commits went directly to `main`; there are no other branches.
  - The four-member workstream table doesn't match the commit history (64 of 73 commits are by one author). If contribution is graded, make sure each member's work is visible.
- **P3-7. Repository clutter.**
  - `prompt.md` (AI prompt notes) is committed.
  - `SUMMARY.md` is untracked and stale.
  - The archival PDF sits under `output/` next to ignored generated data.
  - The project is unlicensed.
  - Either fold `SUMMARY.md` into `docs/`, or delete it and regenerate it from current code.

---

## 4. Recommended order of work

| Step | Work | Closes |
|---|---|---|
| 1 | Refresh the environment: reinstall all extras, delete stale `output/`, rerun and validate, confirm CI is green | P0-4, P3-1 |
| 2 | Rewrite `SUMMARY.md` and fix the doc contradictions in P2-8 (`project/`, freshness, Athena wording) | P2-8 |
| 3 | Fix the cheap correctness bugs: NaN, route cost parity, active alert count, keep the baseline when no alternative exists, `s3://` path, per-alert geometry quarantine, future-timestamp cap | P1-1, 3, 4, 6, 7, 8, 10 |
| 4 | Promote Spark: `--engine spark` for `georisk run`, native watermark/window, dashboard reads the Spark sink, Spark scale benchmark | P0-1 |
| 5 | Make GBT the reported model, with a recorded evaluation | P0-2 |
| 6 | As-of alert revision join, run-unique event IDs, strict RFC 3339 timestamps | P1-2, 11, 12 |
| 7 | Add Glue tables, Glue job, and IAM to CloudFormation, plus a Kinesis `PutRecords` publisher; deploy once for a short window and capture Athena evidence (or state that A3 is unmet) | P0-3, P2-6, P2-7 |
| 8 | Dashboard completeness (processing lag, filtered KPIs, cell selection, delay feature context), larger route graph, optional exact refinement | P2-2 to P2-5 |
| 9 | Refactor and add tests | P3-2 to P3-5 |

---

## 5. Method

- Read the guide, README, SUMMARY, prompt notes, CHANGELOG, all `docs/`, all module READMEs, the configuration, CI, and both CloudFormation templates.
- Reviewed every module under `src/georisk/` and its tests, split into four areas reviewed in parallel: contracts and ingestion, processing and Spark, storage, SQL, and AWS, and routing and dashboard.
- Ran `pytest`: 335 passed, 7 skipped.
- Ran a full `georisk run` into an isolated directory and validated it.
- Ran `georisk validate` against the stale default report.
- Reproduced the NaN, `s3://` path, route-cost, alert-count, and revision findings directly.
- Spark tests and Spark behaviour were **not executed**, because PySpark and Java are not installed on this machine. Spark findings come from reading the code.

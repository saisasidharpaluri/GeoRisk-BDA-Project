# GeoRisk-Spark Project Summary

Last aligned with the code on 2026-10-16. For open gaps and their priority,
see [PROJECT_AUDIT.md](./PROJECT_AUDIT.md).

## 1. One-minute explanation

**GeoRisk-Spark** is a Big Data Analytics project. It flags vessels that may
be exposed to marine hazards, measures congestion in shipping corridors,
estimates delay, and compares a fastest route with a lower-risk alternative.

Vessel GPS events and weather-alert polygons become validated records. Both
are mapped to H3 hexagonal cells. Vessels are matched with hazards active at
the vessel's event time, and unique vessels are counted per cell in 15-minute
windows. Results are stored as date-partitioned Parquet, used to evaluate a
delay model, and shown in a Streamlit + PyDeck dashboard.

The repository is a complete, deterministic **local demonstration**. Spark
Structured Streaming runs locally over file batches, and a Spark MLlib GBT
delay model is available. AWS Kinesis, Glue, S3, and Athena are the planned
deployment path; templates and adapters exist, but nothing is deployed.

## 2. What, why, and how

| Part | What it is | Why it is needed | How this project does it |
| --- | --- | --- | --- |
| Vessel telemetry | Time-stamped positions with speed and heading | Input for risk and congestion | Seeded scenario: vessels on inner/outer lanes, a slow convoy at the chokepoint, 120 minutes every 5 minutes |
| Weather alerts | Time-bounded hazards (storm, high waves, …) | A vessel is at risk only inside an active hazard | Severity 1–5 GeoJSON Polygon/MultiPolygon with `valid_from`/`valid_until`; hazards appear and expire during the scenario |
| Event contracts | Versioned JSON Schemas plus semantic checks | Modules must agree on fields and meaning | `schemas/v1/` + `contracts.py`: strict RFC 3339 UTC, finite numbers, valid rings, valid H3 cells |
| H3 indexing | Hexagonal grid IDs for points and polygons | Repeated point-in-polygon checks are expensive at scale | Point → one cell; polygon → candidate cells (overlap by default, center optional) |
| Risk detection | Candidate exposure signal | Operators need to know who may be affected | Vessel cell ∈ hazard cells, validity half-open, using the alert revision current at that time |
| Corridor density | Unique vessels per cell and window | Message volume is not congestion | Deduplicate `(window, cell, vessel)`, then count |
| Data lake | Historical analytical storage | History, SQL, and model training | Typed Parquet under `raw/` and `curated/`, partitioned by UTC `event_date`; DuckDB runs the SQL locally |
| Delay model | Demonstration delay estimate | Turn risk and congestion into an operational number | Documented synthetic label; linear ridge (default) or Spark MLlib GBT; chronological split; MAE/RMSE vs mean baseline |
| Route comparison | Fastest vs risk-aware route | A risk signal should support a decision | Edge risk from hazard severity and density; Dijkstra with blocked edges and a risk penalty |
| Dashboard | Presentation layer | One place to inspect outputs | Live map, risk summary, density, delay, route what-if, freshness, history |
| Acceptance | Proof that a run is consistent | A zero exit code is not evidence | 17 named checks, scenario tests, and SHA-256 artifact hashes |

## 3. Problem statement and motivation

Marine supply chains depend on vessels moving through ports, chokepoints, and
lanes. Storms, waves, and congestion cause delays. When positions, alerts,
congestion, and route choices live in separate tools, nobody sees the whole
picture. Repeated point-in-polygon checks are costly for fast GPS streams, so
GeoRisk-Spark uses H3 cells as a compact join key and keeps the candidate
match explainable.

The output is **decision support for a synthetic academic demonstration**,
not a certified navigation or safety system.

## 4. Architecture

### Planned cloud architecture

```text
Synthetic vessel events ──┐
                          ├─> Kinesis (two streams)
Synthetic weather alerts ─┘
                                  │
                                  v
                    AWS Glue Spark Structured Streaming
                    validate · H3 · match active hazards · density
                                  │
                                  v
                    S3 raw/curated Parquet (event_date partitions)
                                  │
                    Glue Data Catalog + Athena SQL
                                  │
                delay model · route comparison · dashboard
```

### Implemented local architecture

```text
generate_scenario()  ──> output/data/{vessel-telemetry,weather-alerts}.jsonl
        │                         │
        │                  georisk replay ──> output/stream/ (atomic batches)
        │                         │
        v                         v
run_streaming_job()          georisk spark-stream (Spark file stream,
(local batch processor)       checkpoint + watermark, separate Parquet sink)
        │
        ├─> processed/risk-events.jsonl, density-metrics.jsonl,
        │   quarantine.jsonl, run-metadata.json
        ├─> lake: raw/ and curated/ Parquet (incl. delay_estimates)
        ├─> delay model (linear or Spark GBT) + delay-estimates.jsonl
        ├─> route comparison (scored demo graph)
        └─> report + acceptance checks + artifact hashes
                    │
                    v
            Streamlit dashboard (reads JSONL snapshots and the lake)
```

`georisk run` uses the local Python processor; the Spark stream is an
alternative path whose sink the dashboard does not read yet.

## 5. Processing stages

### Contracts and configuration

`contracts.py` validates each record against its JSON Schema and then checks
relationships the schema cannot express:

- Timestamps are strict RFC 3339 UTC (`…T…Z`, `+00:00`, or `-00:00`), parsed
  the same way on every Python version.
- NaN and infinity are rejected anywhere in a record.
- Alert rings must be closed, have three distinct positions and non-zero area,
  and must not cross the antimeridian (split those into a MultiPolygon);
  `valid_until` must follow `valid_from`.
- Risk events must carry event ID, coordinates, coverage mode, and full
  hazard provenance; `risk_alert` must match the hazard list; H3 cells must be
  valid and match the stated resolution.

`config.py` reads typed, validated settings. Precedence is CLI flags, shell
environment, `.env`, then defaults.

### Synthetic ingestion

`generate_scenario()` (seed 7 by default) creates lane traffic, an eight-vessel
convoy near the chokepoint, and two timed hazards: H1 (storm, severity 4,
minutes 0–150) and H2 (high waves, severity 3, minutes 30–90). Event IDs
combine the seed, vessel, and event time, so reruns are idempotent and
different seeds never collide. Replay validates each record, counts
`generated`/`published`/`rejected`, and writes atomic, fsynced file batches.

### Spatial risk and density

1. Validate telemetry and alerts; invalid records go to quarantine.
2. Quarantine alerts whose geometry H3 cannot cover (`invalid_geometry`).
3. Order alert revisions: each applies from its issue time until the next
   revision; the first also covers earlier telemetry.
4. Map each vessel to an H3 cell (resolution 7 by default) and each alert to
   candidate cells (overlap coverage by default).
5. Quarantine telemetry more than 60 minutes in the future and telemetry older
   than the watermark (latest event time minus 10 minutes).
6. Match when the cell is covered, `valid_from <= t < valid_until`, and the
   revision is current at `t`.
7. Count unique vessels per cell in epoch-aligned 15-minute windows.
8. Validate outputs and write JSONL plus `run-metadata.json`.

H3 matching is a **candidate screen**: overlap coverage can flag a vessel just
outside a polygon, and center coverage can miss boundary cells. No exact
point-in-polygon refinement is performed.

### Storage and SQL

`lake.py` writes `<root>/<zone>/<dataset>/event_date=YYYY-MM-DD/part-<batch>.parquet`:
telemetry and alerts in `raw/`, risk, density, and delay estimates in
`curated/`. Writes are atomic, each row lands on its own UTC date, and
`georisk run` replaces its whole snapshot batch so reruns leave no stale
partitions. Five SQL queries (risk summary, density history, top congested
cells, exposure by vessel, hourly alerts) run against the lake with DuckDB in
tests. They are written for Athena but have not been run there.

### Delay analytics

Features: `baseline_hours`, `density`, `hazard_severity`, `speed_knots`,
`route_distance_nm` (straight-line distance to Port B).

```text
delay_hours = max(0, 0.05·baseline_hours + 0.015·density
                     + 0.45·hazard_severity + 0.002·route_distance_nm
                     − 0.01·speed_knots + noise)      noise ~ N(0, 0.25 h)
```

The last 20% of rows by time are held out. Default run: linear ridge MAE
0.194 h / RMSE 0.232 h versus mean-baseline MAE 1.349 h / RMSE 1.373 h. The
label is built from the same features, so the model mostly recovers the
formula and its error sits at the noise floor; this demonstrates the
workflow, not real predictive skill. Spark MLlib GBT runs with
`--delay-model gbt` where PySpark and Java are installed.

### Routing

The demo graph has Port A, a chokepoint, an outer lane, and Port B. Each
lane's risk is `min(1, 0.7·severity/5 + 0.3·min(1, density/5))`, using alerts
active at the latest telemetry time and that window's density. The baseline
is the fastest path. If it uses a lane above the threshold (0.5), the
alternative is the cheapest path over the open lanes, with cost
`hours + penalty × risk` (penalty 4). Both routes report that same total
cost. If no path avoids blocked lanes, the baseline stays visible and the
alternative is reported as unavailable. Default run: baseline Port A →
chokepoint → Port B (6.47 h, risk 1.42, cost 12.15) versus alternative via the
outer lane (8.88 h, risk 0.06, cost 9.12).

### Dashboard

`georisk dashboard` shows:

- A live map with vessels, hazard zones, risk cells, density extrusions, and
  routes, plus click-to-inspect.
- A risk summary whose table and severity chart follow the type, severity,
  and time filters.
- Corridor density by window, compared with the adjacent windows.
- Delay estimates with model and baseline MAE, feature context, and an
  optional synthetic ground-truth column.
- A route what-if panel (threshold, penalty, and hazard scenario).
- Data freshness: processed time, latest event, watermark, age, and
  processing lag.
- Daily lake history.

It reads H3 settings from the processed run's metadata, and every view is
labelled as simulated.

### Acceptance

`georisk run` writes `output/reports/phase6-run.json`, and `georisk validate`
re-checks it. There are 17 checks:

- Seven behaviour scenarios: inside, outside, expired, duplicate, late,
  explained detour, and overlap boundary.
- Dashboard labels.
- SHA-256 artifact integrity.
- Run status and generation accounting.
- Non-empty risk and density outputs.
- Parquet outputs.
- Finite metrics, and the model is no worse than the baseline.
- Route comparison.
- Dashboard timestamp.

## 6. Data contracts (v1)

| Record | Required fields |
| --- | --- |
| Vessel telemetry | schema_version, event_id, event_time, vessel_id, latitude, longitude, speed_knots, heading_deg |
| Weather alert | schema_version, event_id, alert_id, event_time, valid_from, valid_until, hazard_type, severity (1–5), geometry (lon/lat order) |
| Risk event | schema_version, event_id, event_time, vessel_id, latitude, longitude, coverage_mode, h3_index, h3_resolution, risk_alert, matching_hazards[alert_id, hazard_type, severity, valid_from, valid_until] |
| Density metric | schema_version, window_start, window_end, h3_index, h3_resolution, unique_vessel_count |
| Delay estimate | identity, split, model, prediction, baseline, synthetic label, and the five features |

## 7. Repository map

| Location | Purpose |
| --- | --- |
| [`src/georisk/contracts.py`](./src/georisk/contracts.py) | Schema loading, semantic checks, strict UTC parsing |
| [`src/georisk/config.py`](./src/georisk/config.py), [`cli.py`](./src/georisk/cli.py) | Typed settings and the unified `georisk` CLI |
| [`src/georisk/ingestion/`](./src/georisk/ingestion/) | Scenario generator, replay, file stream, Kinesis adapter |
| [`src/georisk/processing/`](./src/georisk/processing/) | H3 helpers, local processor, Spark stream and transforms |
| [`src/georisk/storage_analytics/`](./src/georisk/storage_analytics/) | Parquet lake, SQL, delay features, linear and GBT models |
| [`src/georisk/route_dashboard/`](./src/georisk/route_dashboard/) | Edge scoring, routing, view helpers, map layers, Streamlit app |
| [`src/georisk/pipeline.py`](./src/georisk/pipeline.py), [`operations.py`](./src/georisk/operations.py), [`scenarios.py`](./src/georisk/scenarios.py) | End-to-end run, acceptance checks, behaviour scenarios |
| [`src/georisk/schemas/v1/`](./src/georisk/schemas/v1/) | JSON Schemas and valid/invalid examples |
| [`infra/aws/`](./infra/aws/) | CloudFormation templates (not deployed) |
| [`docs/`](./docs/) | Architecture, decisions, runbook, evaluation, limitations |
| [`tests/`](./tests/) | Unit, scenario, SQL, dashboard (AppTest), and Spark tests |

## 8. How to run locally (Windows, Python 3.13)

```powershell
py -3.13 -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install -e ".[test,dashboard,dev]" -c constraints.txt
python -m pytest -q
georisk run
georisk validate
georisk dashboard
```

Spark tests and `--delay-model gbt` need PySpark and Java 17 (WSL2 or Linux;
see the README).

## 9. Current status and honest limitations

- Implemented and tested locally:
  - contracts and the scenario generator;
  - replay and quarantine;
  - H3 risk and density;
  - the Parquet lake and SQL;
  - the linear delay model;
  - routing and the dashboard;
  - acceptance checks.
- The Spark stream and GBT model exist and have CI jobs, but they did not run
  on the Windows development machine.
- `georisk run` uses the local Python processor, not Spark. Spark's output
  sink is not connected to the dashboard.
- AWS is not deployed, so the guide's "query density with Athena" criterion
  is not met yet.
- All data, labels, routes, and metrics are synthetic. H3 matching is
  approximate, and the route graph is a four-node demonstration.

## 10. Viva-ready answers

**Why H3?** It turns coordinates and polygons into compact hierarchical cell
IDs that are easy to join, group, and map. It speeds up candidate lookup but
is not an exact polygon test.

**Overlap vs center coverage?** Center coverage keeps cells whose centers lie
inside the polygon and can miss boundary cells. Overlap keeps every
intersecting cell, so it misses fewer but flags some outside points. Overlap
is the default because a missed hazard is worse than a false candidate.

**Why event time?** Hazard validity and density windows describe when things
happened, not when they were processed.

**What if an alert is revised?** Each revision applies from its issue time
until the next one. Earlier telemetry keeps the version that was current
then, so a later revision never rewrites history.

**Late and future data?** Events older than the watermark (latest event minus
10 minutes) are quarantined as late. Events more than 60 minutes ahead of the
processing clock are quarantined as future, so they cannot push the
watermark forward.

**Why unique vessels?** Counting messages measures reporting frequency, not
congestion.

**Why JSON Schema plus semantic checks?** Schemas catch shape and range
errors. The extra checks catch problems a schema can't see: NaN values,
degenerate polygons, invalid H3 cells, and inconsistent flags.

**Why Parquet?** It is columnar, compressed, and typed, and date partitions
let queries skip data.

**Why a time-based split?** It trains on the past and tests on the future,
which is how the model would be used.

**MAE vs RMSE?** Both are in hours. RMSE penalises large errors more.

**Why a baseline?** A model is only useful if it beats predicting the
training mean.

**How is the route chosen?** The baseline is the fastest path. If it uses a
lane above the threshold, the alternative is the cheapest path over the open
lanes, where cost is hours plus penalty × risk. Both routes report the same
cost measure, so the trade-off is visible.

**Is this real-time production?** No. It is a reproducible local
demonstration of a streaming architecture; the AWS deployment is planned.

## 11. Suggested demonstration order

1. Problem, and the four contracts (UTC; lat/lon vs GeoJSON lon/lat order).
2. Run `georisk run`, then `georisk validate` (17 checks).
3. Open the dashboard: the SIMULATED badge, H3 settings, and freshness.
4. Live map: hazards, risk cells, and inspecting a vessel and an alert.
5. Risk summary filters (the chart and the table move together).
6. Corridor density windows and the adjacent-window comparison.
7. Delay panel: model vs baseline, feature context, synthetic ground truth.
8. Route what-if: the default detour, "Close inner lane", a threshold of 1.0
   (baseline kept), and a threshold near 0 (no eligible alternative).
9. History from the Parquet lake, and the resolution 7 vs 8 comparison.
10. Close with the limits: synthetic data, approximate H3 matching, and
    AWS not yet deployed.

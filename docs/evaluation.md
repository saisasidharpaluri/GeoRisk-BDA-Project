# Evaluation recorded 2026-10-12

Measured on Windows 11 build 26300, Python 3.13.12, h3 4.5.0, pyarrow 21.0.0.
The fresh default run used seed 7, ten vessels, 120 minutes sampled every five
minutes, H3 resolution 7, overlap coverage, 15-minute windows, ten-minute
lateness, a 20% evaluation split, and 0.25 h noise. Paths were isolated under
`output/reports/phase10/` so old lake partitions could not inflate counts.
The report is `output/reports/phase10/run.json`; the measurement summary is
`output/reports/phase10/evaluation.json` (both ignored).

## Default run

242 records generated and published: 240 telemetry, two alerts; zero rejected,
quarantined, duplicate, or late records. Latest event: 2026-01-01T13:55:00Z;
watermark: 13:45:00Z. All **17 acceptance checks passed**:

- Seven scenarios: active hazard inside, clearly outside, expired hazard,
  duplicate density, late quarantine, explained detour, overlap boundary cell.
- Dashboard simulation/resolution/unit labels and artifact SHA-256 integrity.
- Successful run, generation accounting, nonempty risk/density, Parquet files.
- Finite model metrics, model beats baseline, route comparison, dashboard timestamp.

The freshness acceptance checks timestamp presence, not that seeded January
events are current. AWS S3/Athena deployment was not tested.

| Held-out model (48 of 240 rows) | MAE (hours) | RMSE (hours) |
| --- | ---: | ---: |
| Linear ridge | 0.193912 | 0.231672 |
| Training-mean baseline | 1.348688 | 1.373330 |

These are synthetic-label metrics, not real-world accuracy. The earlier Phase 7
WSL GBT verification recorded MAE 0.2678 h versus 1.3487 h baseline; that is a
separate model/runtime experiment, not the default run measured here.

## Resolution comparison

The current CLI ten-vessel timed scenario, measured through the S21 comparison
API, produces the following. All counts use identical seeded inputs; covered
hazard cells are a union across retained alerts regardless of active time.

| Metric | Resolution 7 | Resolution 8 |
| --- | ---: | ---: |
| Average cell area (km²) | 5.161293 | 0.737328 |
| Hazard cells | 224 | 1,369 |
| Alerted events | 140 | 124 |
| Exposed vessels | 9 | 9 |
| Distinct density cells | 56 | 114 |
| Peak unique vessels per cell/window | 8 | 5 |
| Elapsed seconds | 0.0566 | 0.0531 |

The S21 legacy generator (`generate_events(7, 10)`, 30 telemetry records)
gives 459 / 2,980 hazard cells, 12 / 12 alerted events, 4 / 4 exposed vessels,
17 / 26 density cells, peak counts 4 / 2, and 0.0087 / 0.0171 seconds at
resolutions 7 / 8. Do not compare these fixture counts with the timed scenario
as if only resolution changed. The original 291-center versus 333-overlap
observation predates offshore geography and is historical boundary evidence.

## 30,000-event processing benchmark

`generate_events(7, 10000)` supplies three samples per vessel plus two alerts.
One `process_events` call accepted all 30,000 telemetry rows in **4.8480 s**,
about **6,188 telemetry events/s**. Timing excludes generation, file I/O, lake
writes, model training, and rendering; validators had been warmed by the
default run. It includes validation, coverage, matching, density, and sorting.
This is one local observation, not a throughput guarantee.

Reproduce from an activated `project/` environment in PowerShell (timings vary):

```powershell
georisk run --seed 7 --vessel-count 10 --duration-minutes 120 --interval-seconds 300 --delay-model linear --input-dir output/reports/evaluation/data --processed-dir output/reports/evaluation/processed --lake-dir output/reports/evaluation/lake --report-path output/reports/evaluation/run.json
georisk validate --report-path output/reports/evaluation/run.json
georisk compare-resolutions --seed 7 --vessel-count 10 --duration-minutes 120 --interval-seconds 300 --report-path output/reports/evaluation/run.json
python -c "
from time import perf_counter
from georisk.ingestion.generator import generate_events
from georisk.processing.stream_job import process_events
warm = generate_events(7, 10)
process_events(warm['telemetry'], warm['weather_alerts'])
events = generate_events(7, 10000)
start = perf_counter()
result = process_events(events['telemetry'], events['weather_alerts'])
elapsed = perf_counter() - start
print(len(result['risk_events']), elapsed, len(events['telemetry']) / elapsed)
"
```

Use default spatial/model settings as listed above without environment
customizations, and a new lake path per evaluation. The runbook lists explicit
flags for a default presentation. Do not mix old-schema lake files into a run.

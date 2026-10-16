# Resolution comparison

`georisk compare-resolutions` compares the timed scenario used by `georisk generate`
at resolutions 7 and 8. It honors `.env`, shell values, and CLI flags such as
`--vessel-count`, `--duration-minutes`, `--coverage-mode`, and `--report-path`.
The output is `resolution-comparison.json` beside the configured report.

The legacy `georisk-compare-resolutions` entry point (or
`python -m georisk.analysis.resolution_comparison`) uses the same timed
scenario, built with `generate_scenario()` from GEORISK_SEED,
GEORISK_VESSEL_COUNT, GEORISK_SCENARIO_DURATION_MINUTES,
GEORISK_TELEMETRY_INTERVAL_SECONDS, and GEORISK_H3_COVERAGE_MODE, so both entry
points write identical comparisons. It writes `resolution-comparison.json`
beside GEORISK_REPORT_PATH (default `output/reports/resolution-comparison.json`).

Each row reports average H3 cell area in square kilometres, distinct covered
hazard cells, alerted events, distinct exposed vessels, distinct density cells,
maximum unique vessels per cell/window, and elapsed seconds. Hazard-cell
counts use the union of each alert's latest valid revision, regardless of
validity relative to telemetry; alerted and exposure counts come from the
processor's as-of revision matching with event-time validity. Density uses the processor's default
15-minute window and telemetry uses its default 10-minute allowed lateness.

Runtime covers processing and the additional hazard-cell counting pass. It
varies between runs and is not a reproducible artifact. Other metrics are
deterministic for the same inputs. Resolution 8 has smaller cells and generally
more hazard cells; compare exposure and density before choosing a resolution.

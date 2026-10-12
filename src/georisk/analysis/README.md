# Resolution comparison

`georisk compare-resolutions` compares the timed scenario used by `georisk generate`
at resolutions 7 and 8. It honors `.env`, shell values, and CLI flags such as
`--vessel-count`, `--duration-minutes`, `--coverage-mode`, and `--report-path`.
The output is `resolution-comparison.json` beside the configured report.
The legacy command below retains its original fixture generator.

Run `georisk-compare-resolutions` after installing the package, or run
`python -m georisk.analysis.resolution_comparison`. The command generates the
same seeded telemetry and alerts at resolutions 7 and 8, using configured
GEORISK_SEED, GEORISK_VESSEL_COUNT, GEORISK_TELEMETRY_POINTS, and
GEORISK_H3_COVERAGE_MODE. It writes `resolution-comparison.json` beside
GEORISK_REPORT_PATH (default `output/reports/resolution-comparison.json`).

Each row reports average H3 cell area in square kilometres, distinct covered
hazard cells, alerted events, distinct exposed vessels, distinct density cells,
maximum unique vessels per cell/window, and elapsed seconds. Hazard coverage
uses the union of the latest valid revisions, matching processor semantics.
It includes retained alerts regardless of their validity relative to telemetry;
exposure counts use event-time validity. Density uses the processor's default
15-minute window and telemetry uses its default 10-minute allowed lateness.

Runtime covers processing and the additional hazard-cell counting pass. It
varies between runs and is not a reproducible artifact. Other metrics are
deterministic for the same inputs. Resolution 8 has smaller cells and generally
more hazard cells; compare exposure and density before choosing a resolution.

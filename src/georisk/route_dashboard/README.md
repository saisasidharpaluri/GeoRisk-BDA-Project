# Member 4 - Routing and dashboard

Own the small port/sea-lane graph, threshold-based lower-risk alternative route, and Streamlit dashboard.

## Starter modules

- `route_optimizer.py`: compare a baseline route with a risk-weighted alternative.
- `dashboard.py`: render vessel positions, hazard zones, alerts, density, estimates, and detours.
- `assets/`: map and presentation assets when needed.

Use a small, documented graph for the semester demonstration. Make travel cost and risk penalty visible in the comparison. Label predictions and route recommendations as synthetic demonstrations.

`route_optimizer.py` provides a deterministic graph, unrestricted fastest
baseline, and threshold-filtered risk-aware alternative. Results include paths,
travel hours, risk scores, total cost, deltas, and a `synthetic_demo` marker.

`dashboard.py` provides a local Streamlit UI and testable JSONL loaders. It
shows telemetry, alert/risk counts, exposed vessels, density cells, freshness,
no-data states, and the route comparison. Run it with:

```powershell
py -m pip install -e ".[dashboard]"
georisk dashboard
```

## Handoff

Coordinate with Member 3 on Athena query results and field names. Coordinate with Member 2 on alert and cell outputs. Keep the dashboard runnable locally before adding cloud query access.

Route comparisons report all edges above the risk threshold, identify those on the baseline path, and explain the detour with travel-time and total-risk differences.

`demonstration_nodes()` returns offshore Chennai coordinates. Each graph edge
includes `distance_nm` and travel hours at 14 knots. An eligible fastest baseline
is retained; when it is blocked, the alternative minimizes travel hours plus
the configured risk penalty among eligible edges.

`edge_risk.score_graph()` scores a deep copy using the H3 grid path between each
edge's coordinates. At the requested time it combines maximum active hazard
severity (70%) and maximum density in the containing window (30%, saturated at
5 vessels). Empty data scores zero. Each edge includes `risk_detail` with the
severity, contributing alert IDs, and density. Hazard revisions use the latest
version available at that time; validity and density windows exclude their end.
The end-to-end pipeline uses these scores at the latest telemetry timestamp
and persists both `route.at_time` and the scored graph in `route.edge_scores`.

`dashboard_summary(data, resolution=7)` includes `simulated=True`, the configured
`h3_resolution`, and units (delay in hours, speed in knots, distance in nm).
Pipeline acceptance requires these labels alongside the latest event timestamp.

The dashboard reads `GeoRiskConfig.from_env()` for input/processed directories,
H3 settings, route threshold, and risk penalty. Sidebar fields start from those
values. `load_dashboard_data(root=None, input_dir=None, processed_dir=None)`
lets explicit directories override the legacy `root/data` and `root/processed`
layout. The pipeline and dashboard share `dashboard_route()`, scoring the same
hazards and density at the latest telemetry timestamp with the selected penalty.

`georisk dashboard` loads `.env` and passes flag overrides to Streamlit through
the subprocess environment. It launches the installed dashboard module with the
current Python interpreter, so it also works outside the repository directory.

`views.py` holds the dashboard's pure helpers, so the UI stays thin and every
transformation is unit-tested: active alerts (latest revision, half-open
validity), severity distribution, risk filters, density windows and adjacent
window comparison, latest positions, delay summaries, route what-if alert sets,
lane status, and run freshness. `GEORISK_STALE_AFTER_MINUTES` (default 30)
sets when the last processing run is shown as stale.

`map_layers.py` builds the PyDeck layers without Streamlit: latest vessel
positions (red when inside an active hazard), hazard polygons in GeoJSON
lon/lat order, alerted H3 risk cells, extruded corridor density (200 m per
unique vessel), baseline and alternative routes, and labelled port/waypoint
markers. `build_deck()` frames `DEMO_BBOX` on a Carto basemap (no token) with a
shared tooltip. Colours follow nautical-chart convention: magenta hazards,
navy traffic, green safe passage.

The live map uses `st.pydeck_chart` with one sidebar checkbox per layer (all on
by default) and a legend that lists only the visible layers. Clicking a vessel
or hazard zone selects it in the "Inspect" panel, which shows the vessel's
latest position, risk flag, and matched hazards, or the alert's type, severity,
validity, and H3 cell count. No-data states are explicit.

The KPI row shows active alerts at the latest event time, vessels exposed now
(with an exposure sparkline), alerted positions, and the latest event time in
UTC. The "Risk" tab charts alerted positions by severity and filters the risk
table by hazard type, minimum severity (1–5), and event-time range; the table
includes `coverage_mode` and `h3_resolution`.

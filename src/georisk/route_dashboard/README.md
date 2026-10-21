# Member 4 - Routing and dashboard

Own the small port/sea-lane graph, threshold-based lower-risk alternative route, and Streamlit dashboard.

## Starter modules

- `route_optimizer.py`: compare a baseline route with a risk-weighted alternative.
- `dashboard.py`: render vessel positions, hazard zones, alerts, density, estimates, and detours.
- `assets/`: map and presentation assets when needed.

Use a small, documented graph for the semester demonstration. Make travel cost and risk penalty visible in the comparison. Label predictions and route recommendations as synthetic demonstrations.

`route_optimizer.py` provides a deterministic graph, unrestricted fastest
baseline, and threshold-filtered risk-aware alternative. Results include paths,
travel hours, risk scores, total cost, deltas, top-level `blocked_edges`,
`alternative_available`, and a `synthetic_demo` marker. Both `total_cost`
values are `travel_hours + risk_penalty × risk_score`, so they are comparable.

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

`demonstration_graph()` has five nodes and three routes between the ports:
the inner lane (Port A → Junction → Chokepoint → Port B), the outer lane
(Port A → Outer waypoint → Port B), and a cut-across from the junction to the
outer waypoint that passes clear of both default hazards. In the default
scenario the storm keeps the inner lane closed, so the detour takes the outer
lane, switches to the cut-across while high waves cover the first outer leg
(12:30–13:30), and returns afterwards.

`demonstration_nodes()` returns offshore Chennai coordinates. Each graph edge
includes `distance_nm` and travel hours at 14 knots. The baseline is always the
unrestricted fastest path. When every edge it takes is within the threshold,
the alternative equals it. Otherwise the alternative minimizes travel hours
plus the configured risk penalty among eligible edges; blocking is checked per
edge actually taken, so parallel edges are handled. The penalty therefore only
shapes the alternative when the baseline uses a blocked lane. If no path avoids
the blocked edges, `alternative` is `None`, `alternative_available` is false,
`route_changed` is false, both deltas are `None`, and the explanation says so;
the baseline is still reported and the dashboard shows "No eligible route". A
destination unreachable even without restrictions raises `ValueError`.

`edge_risk.score_graph()` scores a deep copy using the H3 grid path between each
edge's coordinates (`edge_cells()` falls back to sampling the straight segment
where H3 cannot trace a path, such as near pentagons or over long edges). It
raises `ValueError` when density rows use a different H3 resolution than the
scoring resolution. At the requested time it combines maximum active hazard
severity (70%) and maximum density in the containing window (30%, saturated at
5 vessels). Empty data scores zero. Each edge includes `risk_detail` with the
severity, contributing alert IDs, and density. Hazard revisions use the
processor's as-of rule (the revision current at that time; the first revision
also covers earlier times); validity and density windows exclude their end.
The end-to-end pipeline uses these scores at the latest telemetry timestamp
and persists both `route.at_time` and the scored graph in `route.edge_scores`.

`dashboard_summary(data, resolution=7)` includes `simulated=True`, the configured
`h3_resolution`, and units (delay in hours, speed in knots, distance in nm).
`active_alert_count` counts alerts valid at the latest event time;
`alert_record_count` counts all alert records. Latest times are compared as
parsed timestamps, not strings.
Pipeline acceptance requires these labels alongside the latest event timestamp.

The dashboard reads `GeoRiskConfig.from_env()` for input/processed directories,
H3 settings, route threshold, and risk penalty. Sidebar fields start from those
values. `effective_settings()` then prefers the processed run's
`run-metadata.json` `h3_resolution`, `coverage_mode`, and `window_minutes` for
labels, route scoring, and the inspector, falling back to the configured values. `load_dashboard_data(root=None, input_dir=None, processed_dir=None)`
lets explicit directories override the legacy `root/data` and `root/processed`
layout. The pipeline and dashboard share `dashboard_route()`, scoring the same
hazards and density at the latest telemetry timestamp with the selected penalty.

`georisk dashboard` loads `.env` and passes flag overrides to Streamlit through
the subprocess environment. It launches the installed dashboard module with the
current Python interpreter, so it also works outside the repository directory.

`views.py` holds the dashboard's pure helpers, so the UI stays thin and every
transformation is unit-tested: active alerts (as-of revision, half-open
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

The page reads top to bottom as a presentation:

1. **Header** – title, a one-sentence description, and badges for simulated
   data, the scenario's UTC time span, H3 resolution with average cell area,
   coverage mode, and density window (from `run-metadata.json`, falling back to
   the configured settings). Beside it, **Data freshness** shows processed time,
   latest event, watermark, and processing lag in readable units; "Fresh" or
   "Stale" is wall-clock age since processing, so a new `georisk run` is fresh.
   For the seeded historical data a note explains the large processing lag.
2. **Pipeline strip** – one card per stage (ingest, H3 index, risk match,
   corridor density, analytics) with the counts this run produced.
3. **Scenario clock** – a slider over the scenario's report times (default:
   the latest). The sidebar's "Play scenario" toggle advances it one report
   every 1.5 s (`st.fragment(run_every=…)`) and loops. It replays the processed
   run; it does not process new data. The KPIs, map, inspector, and route
   decision follow the clock; the pipeline strip and the tabs cover the whole
   run. While `georisk live` is streaming (`run-metadata.json` has a `live`
   block updated within the last two minutes), the page refreshes every 2 s,
   shows a LIVE STREAM badge, pins the clock to the newest batch, and shows the
   step, batches, throughput, and latency under Data freshness; the delay
   model and run report are marked as pending until the stream finishes.
4. **KPI row** – active alerts and vessels inside a hazard at the clock time,
   alerted positions up to that time, and the route decision (detour, keep
   baseline, or no safe route) scored at that time.
5. **Live map and Inspect** – PyDeck layers with one sidebar checkbox each and
   a legend for the visible layers: vessel positions and their tracks so far,
   hazard zones active at the clock time, H3 cells alerted so far, and unique
   vessels per cell in the density window containing the clock time. Clicking a vessel or
   zone fills the Inspect panel with a readable card: status pill, position,
   speed, heading, H3 cell, current hazards, and exposure history for a
   vessel; state and time left, severity, validity, H3 coverage, and vessels
   inside for an alert.
6. **Tabs**, one per analytic product:
   - *Hazard exposure* ("Risk summary") – filters for hazard type, minimum
     severity, and time; a vessels-in-hazard timeline; a severity chart that
     follows the filters; and the alerted-position table.
   - *Corridor density* – busiest-cell and occupied-cell trend, a window
     slider for the tables, the window's busiest cells, and a
     comparison with the calendar-adjacent windows (a quiet neighbour shows 0;
     blank only outside the processed range).
   - *Delay model* ("Delay estimates") – model and baseline MAE, error
     reduction, a prediction-vs-label scatter with a y = x reference line, the
     label formula, and each vessel's latest estimate with feature context.
     "Demo mode: show synthetic ground truth" adds the label column.
   - *Route decision* ("Route comparison") – the explanation, side-by-side
     baseline and alternative cards (sailing hours, risk, penalised cost, with
     deltas), and lane status with risk drivers. Node IDs are shown with their
     map labels (Port A, Junction, Chokepoint, Outer waypoint, Port B).
   - *Run quality* – acceptance checks from the run report (path under
     "Data sources", default `GEORISK_REPORT_PATH`), record accounting
     (positions, alerts, quarantined, duplicates, late, future), quarantine
     reasons, the settings used, and the "History" expander with daily totals
     from the curated lake.

All chart axes use UTC labels (`views.chart_time_labels`) because browsers
would otherwise shift datetimes into the viewer's local time zone. Timestamps
in text use `format_time` / `format_datetime`, and durations use
`humanize_minutes`.

The sidebar holds a "Presenter guide", the "Scenario replay" toggle, the
"Route what-if" group (hazard
scenario, risk threshold, penalty), map layers, "Auto-refresh every 10 s"
(reruns the data-dependent body with `st.fragment(run_every=10)`), and a
collapsed "Data sources" expander.

`styles.py` holds the CSS and legend HTML; `theme.toml` holds the Streamlit
theme (chart-paper background, navy sidebar, Archivo / Public Sans / IBM Plex
Mono). `georisk dashboard` applies the theme by setting `STREAMLIT_THEME_BASE`;
when launching `streamlit run` directly, pass
`--theme.base src/georisk/route_dashboard/theme.toml`. The dashboard extra
requires `streamlit>=1.50,<2`.

`views.daily_history(lake_dir)` summarises the curated lake per `event_date`
(risk events, alerted events, exposed vessels, peak density) for the History
expander, which shows a no-data message for an empty lake.

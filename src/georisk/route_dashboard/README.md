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
streamlit run src/georisk/route_dashboard/dashboard.py
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

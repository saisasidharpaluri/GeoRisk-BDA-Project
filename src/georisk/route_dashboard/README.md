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

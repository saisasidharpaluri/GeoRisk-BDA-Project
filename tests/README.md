# Validation plan scaffold

No executable tests are included yet. Add validation with each implementation milestone:

- Contract validation for valid and invalid telemetry and hazard records.
- H3 coverage cases for clearly inside, outside, and boundary-near coordinates.
- Stream-window deduplication and expired-alert behavior.
- S3/Parquet and Athena query checks.
- Time-based held-out model comparison against a mean baseline.
- Risk-threshold route change and dashboard rendering checks.
- Streamlit AppTest coverage of every dashboard panel, filters, route what-if
  scenarios, no-data states, and auto-refresh (`test_dashboard_app.py`; skipped
  when Streamlit, numpy, or pydeck cannot be imported).

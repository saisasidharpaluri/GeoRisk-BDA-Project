# Executable validation suite

Run `.venv\Scripts\python -m pytest -q` from `project/`. The suite covers:

- Contract validation for valid and invalid telemetry and hazard records.
- H3 coverage cases for clearly inside, outside, and boundary-near coordinates.
- Stream-window deduplication and expired-alert behavior.
- Local Parquet round trips and portable Athena SQL executed in DuckDB.
- Time-based held-out model comparison against a mean baseline.
- Risk-threshold route change and dashboard rendering checks.
- Streamlit AppTest coverage of every dashboard panel, filters, route what-if
  scenarios, no-data states, and auto-refresh (`test_dashboard_app.py`; skipped
  when Streamlit, numpy, or pydeck cannot be imported).

CI runs the local suite on Python 3.10–3.13 with an 85% statement-coverage gate
and a separate Python 3.11/Java 17 Spark job. Only `deployment.py` is omitted
from coverage; unexecuted Spark modules still count in the local job. To
reproduce the gate, run `python -m pytest -m "not spark" --cov=georisk
--cov-fail-under=85` on one line. The workflow is in `.github/workflows/ci.yml`;
publishing it does not deploy AWS resources.

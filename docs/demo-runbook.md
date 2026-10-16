# Local demonstration runbook

Start in the repository root on Windows with Python 3.13. All commands below are local;
no AWS account is required. Use a fresh checkout or fresh output directories
for a clean lake history. The generator uses seed 7 and historical event times.

## Install and verify

```powershell
py -3.13 -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install -e ".[test,dashboard,dev]" -c constraints.txt
python -m pytest -q
georisk --help
```

If PowerShell blocks activation, invoke `.venv\Scripts\georisk.exe` in place
of `georisk`. Python DLL Application Control errors require the constrained
Python 3.13 environment. Existing shell variables and `.env` override defaults;
inspect local configuration before presenting. Never display credentials.

## Generate the complete demonstration

```powershell
georisk run --seed 7 --vessel-count 10 --duration-minutes 120 --interval-seconds 300 --resolution 7 --coverage-mode overlap --window-minutes 15 --allowed-lateness-minutes 10 --delay-model linear --evaluation-fraction 0.2 --delay-noise-std-hours 0.25 --route-risk-threshold 0.5 --route-risk-penalty 4
georisk validate
georisk compare-resolutions --seed 7 --vessel-count 10
```

Expect acceptance `passed: true` (17 checks), 240 telemetry rows, two weather
alerts, and held-out MAE near 0.194 h versus baseline 1.349 h. The default
report is `output/reports/phase6-run.json`. Its hashes validate persisted files;
modifying an artifact makes validation fail. CLI errors return nonzero.

## Replay and open the dashboard

```powershell
georisk replay --speed 60
georisk dashboard
```

Replay publishes atomic files to `output/stream` with event gaps divided by 60;
the two-hour scenario takes about two minutes. Use `--speed 0` for an unpaced
rehearsal. Replay does not itself process files or update the dashboard. The
dashboard presents the completed snapshots from `georisk run`; in another
activated terminal, a rerun of that command refreshes them. Optional
`georisk spark-stream` drains the replay into its separate Parquet sink when
PySpark/Java are installed; it is not a live JSONL dashboard feed.

## Presentation sequence and clicks

1. Show the SIMULATED DATA badge, H3 resolution, overlap mode, window, and Data
   freshness panel. Right after `georisk run` the run shows Fresh (wall-clock
   age since processing). January 2026 event times are intentionally
   historical, so Processing lag (processing time minus latest event) is large.
2. On Live map, toggle Vessels, Hazard zones, Risk cells, Corridor density, and
   Routes in the sidebar. Use Inspect vessel and Inspect alert, or click a
   plotted object, to show identity, validity, severity, and position.
3. Open Risk summary, select a hazard type, raise Minimum severity, and narrow
   the event-time range. Show how the table changes. The named acceptance
   checks demonstrate inside/outside and expired-hazard behavior; map hazards
   reflect the latest event time, not animated playback of each arrival.
4. On Corridor density, change Window (UTC); compare previous/current/next
   counts (a quiet adjacent window shows 0; blank only outside the processed
   range) and the map extrusion. Explain that repeated messages do not increase
   unique-vessel counts and a vessel may occupy several cells during a window.
5. On Delay estimates, compare Model MAE (h) and Baseline MAE (h). Toggle
   Demo mode: show synthetic ground truth and explain the synthetic formula.
6. Under sidebar Route what-if, choose No hazards, then Close inner lane.
   Inspect the Route comparison table, blocked lanes, explanation, and redraw.
   Set Route risk threshold to 1.0 to show the baseline retained. A threshold
   near 0 blocks every path: the baseline stays and the alternative reads "No
   eligible route". Return to As generated and 0.5 afterward.
7. Expand History for daily local Parquet totals. Show the resolution comparison
   JSON and explain the coverage/detail trade-off. Local Parquet and DuckDB
   tests show query semantics only; the guide's S3/Athena step remains unmet
   because AWS is undeployed.
8. Toggle Auto-refresh every 10 s to demonstrate rereading snapshots. Stop
   Streamlit with Ctrl+C in its terminal when finished.

For missing-data states, first check input/processed paths in `.env` and the
shell. Use `georisk run` to rebuild outputs, then validate before presenting.
Do not delete an existing lake or checkpoint to troubleshoot without retaining
any results you need. See [limitations](limitations.md) for streaming recovery.

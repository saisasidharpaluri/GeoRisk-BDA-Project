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

The sidebar's "Presenter guide" lists the same steps.

1. **Header and pipeline strip.** Point out the SIMULATED DATA badge, the
   scenario time span, H3 resolution and cell size, and coverage mode. Walk the
   five pipeline cards left to right: ingest → H3 index → risk match → density
   → analytics. Data freshness shows Fresh right after `georisk run`; the large
   processing lag is explained in the panel (seeded January 2026 data).
2. **KPIs and Live map.** The KPIs describe the latest report (13:55 UTC in
   the default run) and the whole scenario. On the map, red vessels are inside
   an active hazard, magenta hexagons are cells alerted at any time, and blue
   columns are the selected density window. Toggle layers in the sidebar, then
   click a vessel or hazard zone to show its Inspect card.
3. **Hazard exposure.** Show the vessels-in-hazard timeline, then select a
   hazard type, raise Minimum severity, and narrow the time range; the severity
   chart and table follow the filters. The acceptance scenarios (Run quality)
   cover inside/outside and expired-hazard behaviour.
4. **Corridor density.** Explain unique vessels per cell and window, move the
   Window slider, and show that the map's columns follow it. Compare the
   previous/next windows (a quiet window shows 0; blank only outside the
   processed range).
5. **Delay model.** Compare Model MAE (h) with Baseline MAE (h) and the error
   reduction. Use the scatter and its diagonal to explain that the remaining
   error is the label noise; show the formula, then toggle "Demo mode: show
   synthetic ground truth".
6. **Route decision.** In the sidebar Route what-if, choose No hazards, then
   Close inner lane, and compare the baseline and alternative cards and the
   lane table. Set the threshold to 1.0 to keep the baseline; a threshold near
   0 blocks every path and shows "No eligible route". Return to As generated
   and 0.5 afterwards.
7. **Run quality.** Show "17 of 17 acceptance checks passed", the record
   accounting, the settings used, and History. Mention the resolution
   comparison JSON and that local Parquet/DuckDB tests show query semantics
   only; the guide's S3/Athena step remains unmet because AWS is undeployed.
8. Optionally toggle Auto-refresh every 10 s to show snapshots being reread.
   Stop Streamlit with Ctrl+C when finished.

For missing-data states, first check input/processed paths in `.env` and the
shell. Use `georisk run` to rebuild outputs, then validate before presenting.
Do not delete an existing lake or checkpoint to troubleshoot without retaining
any results you need. See [limitations](limitations.md) for streaming recovery.

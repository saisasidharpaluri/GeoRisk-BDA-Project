"""CSS and static HTML fragments for the Streamlit dashboard.

Colours follow the nautical-chart palette in ``theme.toml`` and ``map_layers``:
navy ink for traffic, chart magenta for hazards, buoy green for the safe
passage, and sea blue for density.
"""

from __future__ import annotations

CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Archivo:wdth,wght@62..125,500..800&family=IBM+Plex+Mono:wght@400;500;600&family=Public+Sans:wght@400;500;600;700&display=swap');
:root { --gr-ink:#14232E; --gr-paper:#F2F5F4; --gr-card:#FFFFFF; --gr-magenta:#A8166F;
        --gr-green:#0E7C66; --gr-slate:#5B6B78; --gr-sea:#1D5C80; --gr-rule:#C9D6DA;
        --gr-red:#E03A1E; --gr-amber:#B7791F; }
.block-container { padding-top: 3.6rem; max-width: 1480px; }
/* Presentation mode: keep the header bar from covering the title. */
header[data-testid="stHeader"] { background: transparent; }
.stAppDeployButton, [data-testid="stAppDeployButton"] { display: none; }

/* Header */
.gr-eyebrow { font: 600 .72rem/1.3 'IBM Plex Mono', monospace; letter-spacing: .16em;
  text-transform: uppercase; color: var(--gr-slate); margin: 0; }
.gr-title { font-family: 'Archivo', sans-serif; font-stretch: 72%; font-weight: 800;
  font-size: clamp(2.2rem, 4vw, 3rem); line-height: .95; letter-spacing: .005em;
  text-transform: uppercase; color: var(--gr-ink); margin: .3rem 0 .5rem; padding: 0; }
.gr-lede { font: 1rem/1.5 'Public Sans', sans-serif; color: var(--gr-ink); max-width: 62rem;
  margin: 0 0 .75rem; }
.gr-badges { display: flex; flex-wrap: wrap; align-items: center; gap: .4rem; }
.gr-badge { font: 500 .76rem/1.2 'IBM Plex Mono', monospace; color: var(--gr-ink);
  background: #E3ECEE; border: 1px solid var(--gr-rule); border-radius: 3px; padding: .25rem .5rem; }
.gr-badge.stamp { color: var(--gr-magenta); border-color: var(--gr-magenta); background: transparent;
  font-weight: 600; letter-spacing: .1em; }

/* Pipeline strip */
.gr-flow-title { font: 600 .74rem/1.3 'IBM Plex Mono', monospace; letter-spacing: .14em;
  text-transform: uppercase; color: var(--gr-slate); margin: .4rem 0 .5rem; }
.gr-flow { display: grid; grid-template-columns: repeat(5, minmax(0, 1fr)); gap: .6rem;
  margin-bottom: 1rem; }
.gr-step { position: relative; background: var(--gr-card); border: 1px solid var(--gr-rule);
  border-top: 3px solid var(--gr-sea); border-radius: 4px; padding: .65rem .8rem .7rem; }
.gr-step.risk { border-top-color: var(--gr-magenta); }
.gr-step.out { border-top-color: var(--gr-green); }
.gr-step:not(:last-child)::after { content: "→"; position: absolute; right: -.62rem; top: 42%;
  font: 700 1rem 'IBM Plex Mono', monospace; color: var(--gr-slate); background: var(--gr-paper);
  line-height: 1; padding: .1rem 0; z-index: 1; }
.gr-step-name { font: 600 .7rem/1.2 'IBM Plex Mono', monospace; letter-spacing: .08em;
  text-transform: uppercase; color: var(--gr-slate); margin: 0; }
.gr-step-value { font: 600 1.25rem/1.25 'Public Sans', sans-serif; color: var(--gr-ink);
  margin: .3rem 0 .15rem; }
.gr-step-detail { font: .8rem/1.4 'Public Sans', sans-serif; color: var(--gr-slate); margin: 0; }
@media (max-width: 900px) { .gr-flow { grid-template-columns: 1fr 1fr; }
  .gr-step::after { display: none; } }

/* KPI cards */
[data-testid="stMain"] [data-testid="stMetricLabel"] p { font-size: .74rem; letter-spacing: .07em;
  text-transform: uppercase; color: var(--gr-slate); }
[data-testid="stMetricValue"] { font-family: 'IBM Plex Mono', monospace; font-weight: 500; }

/* Key-value lists (freshness, inspector) */
.gr-kv { display: grid; grid-template-columns: max-content 1fr; gap: .25rem .9rem; margin: .4rem 0 0;
  font: .82rem/1.45 'Public Sans', sans-serif; color: var(--gr-ink); }
.gr-kv dt { color: var(--gr-slate); }
.gr-kv dd { margin: 0; font-variant-numeric: tabular-nums; }
.gr-note { font: .76rem/1.45 'Public Sans', sans-serif; color: var(--gr-slate); margin: .5rem 0 0; }

/* Status pills */
.gr-pill { display: inline-block; font: 600 .72rem/1.2 'IBM Plex Mono', monospace;
  letter-spacing: .06em; text-transform: uppercase; border-radius: 999px; padding: .2rem .6rem; }
.gr-pill.alert { background: #FBE3DD; color: #A3270F; }
.gr-pill.ok { background: #DDF0EA; color: #0A5A4A; }
.gr-pill.muted { background: #E3ECEE; color: var(--gr-slate); }
.gr-pill.warn { background: #F7ECD6; color: #7A5212; }
.gr-card-title { font: 600 1rem/1.3 'Public Sans', sans-serif; color: var(--gr-ink); margin: .5rem 0 0; }
.gr-sev { display: inline-flex; gap: 3px; vertical-align: -1px; }
.gr-sev i { width: 10px; height: 10px; border-radius: 2px; background: #E3ECEE; display: block; }
.gr-sev i.on { background: var(--gr-magenta); }

/* Map legend */
.gr-legend { display: flex; flex-wrap: wrap; gap: .3rem 1.1rem; margin: .5rem 0 0;
  font: 500 .76rem/1.4 'Public Sans', sans-serif; color: var(--gr-ink); }
.gr-legend span { white-space: nowrap; }
.gr-legend b { font: 600 .68rem/1.4 'IBM Plex Mono', monospace; letter-spacing: .1em;
  text-transform: uppercase; color: var(--gr-slate); margin-right: -.4rem; }
.gr-key { display: inline-block; width: 12px; height: 12px; margin-right: 6px; vertical-align: -1px; }
.gr-key.dot { border-radius: 50%; box-shadow: 0 0 0 1.5px var(--gr-paper) inset; }
.gr-key.zone { background: rgba(168,22,111,.16); border: 2px solid var(--gr-magenta); }
.gr-key.hex { clip-path: polygon(25% 4%,75% 4%,100% 50%,75% 96%,25% 96%,0 50%); }
.gr-key.line { height: 4px; width: 20px; vertical-align: 3px; border-radius: 2px; }
.gr-hint { font: .8rem/1.45 'Public Sans', sans-serif; color: var(--gr-slate); margin: 0 0 .4rem; }

/* Tabs */
.stTabs button[role="tab"] p, .stTabs [data-baseweb="tab"] p {
  font-family: 'Archivo', sans-serif; font-stretch: 87.5%; font-weight: 650; font-size: 1.02rem;
  letter-spacing: .02em; }
.st-key-freshness h3 { font-size: .8rem; letter-spacing: .12em; text-transform: uppercase;
  font-family: 'IBM Plex Mono', monospace; font-weight: 600; padding-top: 0; }
@media (prefers-reduced-motion: reduce) { * { animation: none !important;
  transition: none !important; } }
</style>
"""

LEGEND = {
    "vessels": '<span><i class="gr-key dot" style="background:#14232E"></i>Vessel</span>'
    '<span><i class="gr-key dot" style="background:#E03A1E"></i>Vessel inside a hazard</span>',
    "hazards": '<span><i class="gr-key zone"></i>Active hazard zone</span>',
    "risk_cells": '<span><i class="gr-key hex" style="background:#A8166F"></i>'
    "Alerted H3 cell (whole scenario)</span>",
    "density": '<span><i class="gr-key hex" '
    'style="background:linear-gradient(90deg,#9CC9DB,#1D5C80)">'
    "</i>Unique vessels per cell (height, selected window)</span>",
    "route": '<span><i class="gr-key line" style="background:#5B6B78"></i>Baseline (fastest)</span>'
    '<span><i class="gr-key line" style="background:#0E7C66"></i>Alternative (risk-aware)</span>',
}

# 0001: Default to H3 resolution 7

## Context
The dashboard needs compact hazard coverage and useful corridor aggregation.
The S21 comparison measures identical inputs at resolutions 7 and 8.

## Decision
Use resolution 7 by default; retain the configurable resolution in every output.
Re-running the S21 API on its legacy fixture (`generate_events(7, 10)`) on
2026-10-12 gives 459 vs 2,980 hazard cells, 17 vs 26 density cells, and 12
alerted events / 4 exposed vessels at either resolution. Average cell areas
are 5.1613 vs 0.7373 square kilometres; timings were 0.0087 vs 0.0171 seconds.
The later ten-vessel timed scenario has 224 vs 1,369 hazard cells and 140 vs
124 alerted events. See [evaluation](../evaluation.md) for fixture distinctions.

## Consequences
Resolution 7 reduces coverage size and gives coarser density aggregation.
Resolution 8 improves spatial detail but is not exact containment. Neither
these timings nor synthetic exposure counts establish production accuracy.

## Status
Accepted; configurable through `GEORISK_RESOLUTION` (default 7).

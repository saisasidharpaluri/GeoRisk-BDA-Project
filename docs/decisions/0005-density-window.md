# 0005: Fifteen-minute unique-vessel density

## Context
Counting messages inflates density when vessels report repeatedly. The demo
needs a stable cell-level concentration metric and adjacent-window comparison.

## Decision
Count distinct vessel IDs per H3 cell and UTC epoch-aligned, half-open window.
Use fifteen minutes by default, with configurable integer durations from 1 to
1440 minutes that divide 1440. An event at the end belongs to the next window.

## Consequences
Retries and repeated vessel positions do not inflate a cell/window count.
A vessel traversing several cells can contribute to each; summing counts is
not a global distinct-vessel total. Shorter windows change feature density and
route risk, so retain settings in run metadata and compare like configurations.
Spark recomputes affected dates from retained distinct keys.

## Status
Accepted; `GEORISK_DENSITY_WINDOW_MINUTES=15`. Covered by local and Spark
window-boundary, duplicate, and multi-hour-window tests.

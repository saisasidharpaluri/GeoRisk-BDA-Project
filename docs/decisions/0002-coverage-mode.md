# 0002: Use overlap coverage for candidate risk

## Context
The original gap-analysis fixture covered 291 cells with center containment
and 333 with overlap at resolution 7: 42 boundary cells, about 13% of the
overlap cover, were omitted by center mode. Those are historical fixture
numbers, not the current offshore scenario's coverage counts.

## Decision
Default to `overlap` using the H3 experimental overlap API. Retain `center`
as an explicit option and write the chosen mode on risk rows and metadata.
The boundary acceptance scenario verifies a cell missed by center mode.

## Consequences
Intersecting cells are included even when their centers lie outside the
polygon. A vessel outside the polygon but inside such a cell may be flagged.
This is a candidate screen; no exact point-in-polygon refinement is performed.
The experimental API is isolated in `processing/h3_index.py` and h3 is pinned.

## Status
Accepted; `GEORISK_H3_COVERAGE_MODE=overlap`. Historical evidence is recorded in
the implementation checklist's S16 rationale; current measurements are in
[evaluation](../evaluation.md).

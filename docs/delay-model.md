# Synthetic delay model

The default linear ridge model and optional Spark MLlib GBT model predict a
synthetic label in hours. These metrics are not real-world accuracy.

## Label and features

`delay = max(0, 0.05 * baseline_hours + 0.015 * density
+ 0.45 * hazard_severity + 0.002 * route_distance_nm
- 0.01 * speed_knots + noise_hours)`

`build_delay_rows()` joins accepted risk events to telemetry by `event_id` and
to density by H3 cell and configured UTC window. Speed is floored at 1 knot;
distance is the haversine distance to `port_b`, not the graph route length.
Baseline hours equal distance / speed. Hazard severity is the maximum matched
severity, or zero. Density is unique vessels in that cell/window. Noise comes
from `random.Random(seed).gauss(0, noise_std_hours)`, default seed 7 and standard
deviation 0.25 hours. Labels are clipped at zero; linear predictions are not.

## Training and evaluation

Rows are sorted by event time; the last `ceil(n * evaluation_fraction)` rows
are held out (default 20%). Equal timestamps retain input order, so the split
can divide a timestamp group. This is a chronological row split, not a
vessel-disjoint experiment. The default 240 rows split into 192 training and
48 evaluation rows. Features and synthetic labels share a generating formula,
and full-window density can include later positions in the same window;
this is an offline demonstration, not a causal online forecasting benchmark.

The linear implementation solves least squares with a tiny ridge penalty
(1e-8 on feature weights, none on the intercept). GBT uses the same five
features, a VectorAssembler, 50 iterations, max depth 4, and seed 7. The
baseline always predicts the training-label mean. Evaluation reports MAE and
RMSE in hours on held-out rows only. Training rows also receive estimates,
but the dashboard metrics filter to the evaluation split.

## Artifacts and execution

Run `georisk run --delay-model linear`, or `georisk run --delay-model gbt` in
the configured Spark/Java environment. `output/processed/delay-estimates.jsonl`
contains event identity, model, split, prediction, baseline, and synthetic
label. The linear `delay-model.json` stores coefficients and feature order;
`delay-model-gbt/` stores the Spark pipeline. Estimates also enter the curated
Parquet lake. The report and artifact manifest link these outputs.

[Evaluation](evaluation.md) records measured model and baseline errors.
Changing seed, noise, split, geometry, or window changes the experiment.

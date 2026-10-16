# Member 3 - Storage and analytics

Own the S3 data lake, Parquet layout, Athena examples, and the delay models. Glue Data Catalog table registration is planned; no code registers tables yet.

## Modules

- `lake.py`: write and read raw and curated Parquet datasets locally or to S3.
- `delay_model.py`: train and evaluate the linear delay model on synthetic history.
- `spark_delay_model.py`: the optional Spark MLlib GBT delay model.
- `sql/`: portable Athena/Trino and DuckDB queries for risk summaries, density,
  congested cells, vessel exposure, and hourly alert counts (see `sql/README.md`).

Partition by event date; keep H3 IDs as queryable columns rather than high-cardinality partitions. Make training/evaluation splits time-based and compare the model with a simple mean baseline. Document how synthetic labels are generated.

`lake.write_partitioned(records, root, dataset_name=..., batch_id="00000")`
validates the complete batch and writes Parquet under
`<root>/<zone>/<dataset>/event_date=YYYY-MM-DD/part-<batch_id>.parquet`.
Telemetry and weather alerts use the `raw` zone; risk events, density metrics, and delay estimates
use `curated`. Each row is partitioned by its own UTC `event_time` date, or
`window_start` for density. Empty batches return an empty list without files.
Replaying the same batch replaces its file in each affected partition; other
batch IDs and dates remain untouched. Use a different batch ID to append data.
`replace_batch=True` also removes this batch's files from local date partitions
absent from the new records; `georisk run` uses it so reruns leave no stale
partitions. The default (`False`) keeps incremental behavior. Local files are
written to a hidden temporary file, then atomically renamed with `os.replace`.

`write_dataset()` remains available for callers supplying an explicit date; it
rejects records whose UTC date differs from that `event_date`.
Both writers support the same zone layout for `s3://` destinations when `boto3`
is available. `read_dataset()`, `risk_summary()`, and `density_summary()` read
the local zone paths. The pipeline writes all five datasets and reports a list
of paths per dataset in `lake.paths`, including every file in its manifest.
Acceptance requires all five datasets and every listed file to exist.

`delay_model.py` documents the synthetic label formula in
`make_synthetic_label()`, performs a time-ordered split, fits a regularized
linear regression, and reports MAE/RMSE alongside a training-mean baseline.
The linear model is the local deterministic default; the Spark MLlib GBT model
in `spark_delay_model.py` (below) uses the same feature columns. Running either
in AWS Glue is planned.

`delay_features.build_delay_rows()` joins each processed risk event to its
telemetry by `event_id`. Speed is clamped to at least 1 knot; distance is the
haversine distance in nautical miles to demo `port_b`, and `baseline_hours` is
distance / speed. Density comes from the event's own UTC window and H3 cell
(zero when absent); hazard severity is the maximum matched severity or zero.
Rows retain the event ID, vessel ID, and event time. Missing telemetry raises
an error; duplicate telemetry IDs use the first record, matching processing.

The synthetic label in hours is:
`max(0, 0.05 * baseline_hours + 0.015 * density + 0.45 * hazard_severity
+ 0.002 * route_distance_nm - 0.01 * speed_knots + noise_hours)`.
Noise is drawn from a seeded Gaussian with standard deviation configured by
`GEORISK_DELAY_NOISE_STD_HOURS=0.25` (finite and non-negative; zero disables it).
The seed and input order reproduce the labels. The later 20% of rows are held
out by default. The comparison baseline predicts the training-label mean;
it is distinct from the distance/speed feature. These are synthetic
demonstration metrics, not evidence of real-world forecasting accuracy.

Each run saves every training and evaluation row to
`processed/delay-estimates.jsonl` and `curated/delay_estimates/` in the lake,
validated against the `delay_estimate` contract. Rows include the five features,
event identity/time, split (`train` or `evaluation`), `model_name`, predicted
delay, training-mean baseline delay, and synthetic label (all delays in hours).
Predictions are the raw linear output and can be negative; labels are clamped
at zero. Only evaluation rows contribute to the reported MAE/RMSE.

`processed/delay-model.json` stores `model_name: "linear"`, the ordered
`features`, and `coefficients` (intercept first, followed by one weight per
feature). Load the JSON and call `delay_model.predict(model["coefficients"],
rows)` to reproduce predictions; rows need features but no labels. Both JSON
artifacts and the delay Parquet files are included in the run manifest; a
listed file that is missing makes `artifact_manifest()` raise
`FileNotFoundError`.

## Handoff

Consume Member 2's risk and density schemas. Provide Member 4 with stable Athena queries or result files and the field names they can render.

Parquet uses explicit nullable schemas; weather geometry is stored as JSON and
restored on read. Legacy files directly under `<root>/<dataset>/` are not read
by the zone-aware reader. Regenerate them into a fresh lake directory.

## Spark MLlib delay model

Set `GEORISK_DELAY_MODEL=gbt` to train a Spark MLlib pipeline containing a
VectorAssembler over the same five FEATURES and a seeded GBTRegressor (50
iterations, maximum depth 4). The default remains `linear`, which does not
import or start Spark. The chronological split and noisy synthetic labels are
shared by both models. MAE and RMSE are evaluated on held-out rows against the
training-mean baseline; these demonstrate synthetic behavior, not real-world
forecast accuracy.

`train_and_evaluate_gbt()` returns the linear result keys plus `model_name=gbt`
and the fitted `model`. `coefficients` is null because trees have no linear
coefficient vector. `predict_gbt()` accepts feature-only rows and preserves their
input order. `save_gbt_model()` saves the entire pipeline, loadable with
`pyspark.ml.PipelineModel.load()`.

The end-to-end pipeline writes `processed/delay-model-gbt/`, includes its files
in the artifact manifest, and writes GBT predictions to the usual JSONL and
Parquet delay-estimate outputs. The run report includes `model.model_name` for
both model choices. From an activated WSL Spark environment:

```bash
GEORISK_DELAY_MODEL=gbt georisk-run
```

Spark model metadata contains generated identifiers and timestamps, so model
file hashes can vary between equivalent training runs. The caller owns the
Spark session lifecycle when invoking the pipeline as a library.

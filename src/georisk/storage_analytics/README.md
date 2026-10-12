# Member 3 - Storage and analytics

Own the S3 data lake, Parquet layout, Glue Data Catalog tables, Athena examples, and a Spark MLlib delay baseline.

## Starter modules

- `lake.py`: write raw and processed datasets to S3 and register their layout.
- `delay_model.py`: train and evaluate a delay model on synthetic history.
- `sql/`: portable Athena/Trino and DuckDB queries for risk summaries, density,
  congested cells, vessel exposure, and hourly alert counts (see `sql/README.md`).

Partition by event date; keep H3 IDs as queryable columns rather than high-cardinality partitions. Make training/evaluation splits time-based and compare the model with a simple mean baseline. Document how synthetic labels are generated.

`lake.write_partitioned(records, root, dataset_name=..., batch_id="00000")`
validates the complete batch and writes Parquet under
`<root>/<zone>/<dataset>/event_date=YYYY-MM-DD/part-<batch_id>.parquet`.
Telemetry and weather alerts use the `raw` zone; risk events and density metrics
use `curated`. Each row is partitioned by its own UTC `event_time` date, or
`window_start` for density. Empty batches return an empty list without files.
Replaying the same batch replaces its file in each affected partition; other
batch IDs and dates remain untouched. Use a different batch ID to append data.

`write_dataset()` remains available for callers supplying an explicit date.
Both writers support the same zone layout for `s3://` destinations when `boto3`
is available. `read_dataset()`, `risk_summary()`, and `density_summary()` read
the local zone paths. The pipeline writes all four datasets and reports a list
of paths per dataset in `lake.paths`, including every file in its manifest.
Acceptance requires all four datasets and every listed file to exist.

`delay_model.py` documents the synthetic label formula in
`make_synthetic_label()`, performs a time-ordered split, fits a regularized
linear regression, and reports MAE/RMSE alongside a training-mean baseline.
The model is intentionally a local deterministic baseline; the same feature
columns can be mapped to Spark MLlib for AWS Glue execution.

## Handoff

Consume Member 2's risk and density schemas. Provide Member 4 with stable Athena queries or result files and the field names they can render.

Parquet uses explicit nullable schemas; weather geometry is stored as JSON and
restored on read. Legacy files directly under `<root>/<dataset>/` are not read
by the zone-aware reader. Regenerate them into a fresh lake directory.

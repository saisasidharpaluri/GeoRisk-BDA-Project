# Member 3 - Storage and analytics

Own the S3 data lake, Parquet layout, Glue Data Catalog tables, Athena examples, and a Spark MLlib delay baseline.

## Starter modules

- `lake.py`: write raw and processed datasets to S3 and register their layout.
- `delay_model.py`: train and evaluate a delay model on synthetic history.
- `sql/`: Athena queries for risk, vessel density, and route comparisons.

Partition by event date; keep H3 IDs as queryable columns rather than high-cardinality partitions. Make training/evaluation splits time-based and compare the model with a simple mean baseline. Document how synthetic labels are generated.

The local implementation uses `lake.write_dataset()` to validate and write
Parquet under `<root>/<dataset>/event_date=YYYY-MM-DD/`. It supports the four
contract datasets and can upload a single Parquet object to an `s3://` URI when
`boto3` is available. `read_dataset()`, `risk_summary()`, and
`density_summary()` provide dashboard-ready local reads.

`delay_model.py` documents the synthetic label formula in
`make_synthetic_label()`, performs a time-ordered split, fits a regularized
linear regression, and reports MAE/RMSE alongside a training-mean baseline.
The model is intentionally a local deterministic baseline; the same feature
columns can be mapped to Spark MLlib for AWS Glue execution.

## Handoff

Consume Member 2's risk and density schemas. Provide Member 4 with stable Athena queries or result files and the field names they can render.

# Member 3 - Storage and analytics

Own the S3 data lake, Parquet layout, Glue Data Catalog tables, Athena examples, and a Spark MLlib delay baseline.

## Starter modules

- `lake.py`: write raw and processed datasets to S3 and register their layout.
- `delay_model.py`: train and evaluate a delay model on synthetic history.
- `sql/`: Athena queries for risk, vessel density, and route comparisons.

Partition by event date; keep H3 IDs as queryable columns rather than high-cardinality partitions. Make training/evaluation splits time-based and compare the model with a simple mean baseline. Document how synthetic labels are generated.

## Handoff

Consume Member 2's risk and density schemas. Provide Member 4 with stable Athena queries or result files and the field names they can render.

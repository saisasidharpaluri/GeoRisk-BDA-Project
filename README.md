# GeoRisk-Spark

GeoRisk-Spark is a four-person Big Data Analytics semester project. The planned system will process synthetic vessel telemetry and marine hazard alerts with H3 and Spark, store results on AWS, estimate demonstration delays, compare safer routes, and show results in a dashboard.

This repository currently contains **scaffold only**: workstream folders, starter module interfaces, shared event schemas, configuration examples, and the project guide. The modules raise `NotImplementedError` until their owners implement them. No streaming job, AWS resources, model, route optimizer, dashboard, or sample-data processor is implemented yet.

## Workstreams

| Owner | Area | Scaffold location |
| --- | --- | --- |
| Member 1 | Synthetic event generation, event contracts, Kinesis publishing | `src/georisk/ingestion/` |
| Member 2 | Spark Structured Streaming, H3 indexing, risk and density outputs | `src/georisk/processing/` |
| Member 3 | S3/Parquet lake, Glue Catalog, Athena, Spark MLlib delay baseline | `src/georisk/storage_analytics/` |
| Member 4 | Risk-weighted route comparison and Streamlit dashboard | `src/georisk/route_dashboard/` |

The JSON Schema contracts in `schemas/v1/` are the shared interfaces. Coordinate pairs use latitude/longitude for vessel events and GeoJSON longitude/latitude order for hazard geometry. H3 cell matching is an approximate spatial filter, not exact polygon containment.

## Planned AWS path

Amazon Kinesis Data Streams will ingest vessel and weather events; AWS Glue will run Spark Structured Streaming; Amazon S3 will hold date-partitioned Parquet; and the Glue Data Catalog plus Athena will support historical SQL. Start paid or long-running resources only for short development or demo windows. AWS resources have not been provisioned.

## Collaboration

- Use one feature branch per cohesive task and open a pull request to `main` for teammate review.
- Keep commits focused and push after each integrated milestone. Never force-push `main`.
- Keep AWS credentials in a local profile or SSO session. Never commit credentials or generated data.
- Agree on schema changes with all owners and update `schemas/v1/` and the guide together.

## Project guide

See [the team project guide](output/pdf/GeoRisk-Spark-Team-Project-Guide.pdf) for the problem statement, architecture, role responsibilities, milestones, event contracts, AWS cost guidance, and acceptance criteria.

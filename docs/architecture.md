# Architecture

The same packaged JSON contracts connect generation, processing, storage, and
presentation. Local mode is implemented and tested; the cloud path is planned
and has not been deployed.

```mermaid
flowchart TD
  subgraph local[Implemented local mode]
    G[Seeded vessel and hazard scenario] --> J[Validated JSONL snapshots]
    J --> P[Python batch: H3 risk and window density]
    J --> R[Paced replay: atomic file batches]
    R --> S[Spark Structured Streaming]
    S --> CP[Checkpoint snapshots and watermark]
    S --> SP[Separate Spark Parquet sink]
    P --> O[Risk, density, quarantine and metadata]
    J --> L[Raw local Parquet]
    O --> C[Curated local Parquet]
    O --> M[Linear or Spark GBT delay model]
    M --> C
    M --> E[Estimates and persisted model]
    C --> SQL[DuckDB portable SQL and history]
    O --> RT[Hazard and density edge scoring]
    RT --> D[Streamlit and PyDeck dashboard]
    E --> D
    J --> D
    SQL --> D
    O --> A[Report, scenarios, artifact hashes and validation]
    E --> A
  end
  subgraph cloud[Planned AWS deployment]
    K[Two Kinesis streams] --> GL[AWS Glue Spark processing]
    GL --> S3[S3 raw and curated Parquet plus checkpoints]
    S3 --> GC[Glue Data Catalog]
    GC --> AT[Athena queries]
    AT --> CD[Future cloud dashboard adapter]
    CW[CloudWatch logs and metrics] -.-> GL
  end
  G -. planned publishing .-> K
```

Local lake datasets are partitioned by each row's UTC event date; raw contains
telemetry/alerts, curated contains risk/density/delay estimates. Explicit Arrow
schemas preserve types. SQL and JSON Schemas ship as package resources.
Configuration precedence is flags, shell, dotenv, defaults. The dashboard
loads configured JSONL outputs and lake history; Spark's separate sink is not
connected to the current dashboard loader. The report records settings,
acceptance results, and hashes. Quarantine and wall-clock metadata are excluded
from the reproducible artifact manifest.

The processing and storage module READMEs specify checkpoint recovery,
partition replacement, revision behavior, and model artifacts. See
[limitations](limitations.md) before interpreting the diagram as production
architecture.

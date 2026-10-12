# Limits of the demonstration

All vessel tracks, hazards, delays, and labels are synthetic. Successful
acceptance checks establish implementation behavior on fixtures, not maritime
safety or real-world prediction quality. No operational navigation advice is
provided. Routes use a small artificial graph without charts, bathymetry,
regulations, currents, fuel constraints, or live weather feeds.

H3 coverage is approximate. Overlap includes intersecting boundary cells and
can flag vessels outside a polygon; center mode can omit intersecting cells.
No exact containment refinement is performed. Density counts distinct vessels
per cell/window, not occupancy time or a globally additive fleet count.

Local batch processing advances the watermark per arrival; Spark evaluates an
unordered microbatch against the previous committed maximum. These policies
can disagree within a batch. Local event-ID deduplication resets each run;
Spark checks persisted date partitions and assumes IDs keep their original
event date. Latest alert revisions replace older versions; Spark reloads them
when telemetry arrives and does not revise past outputs for alert-only arrivals.

Spark file streaming uses one writer, checkpoint snapshots, and retained density
keys. There is no bounded retention, multi-writer transaction protocol, or
production capacity claim. Checkpoints and sink output must be preserved
together. The default dashboard reads local JSONL snapshots, not the Spark
Parquet stream; auto-refresh rereads files but does not run processing.
The seeded event dates are historical, so freshness correctly reports stale.

The delay label is derived from the same features used for learning. Its
chronological split is not vessel-disjoint and density is computed over whole
windows. See [delay model](delay-model.md); low synthetic errors must not be
interpreted as external predictive validation.

AWS would add managed ingestion, distributed Glue execution, S3 persistence,
Glue Catalog metadata, Athena querying, IAM controls, and cloud monitoring.
Templates and adapters exist, but resources are not deployed and service
integration, scaling, quotas, cost, permissions, recovery, and teardown need
separate verification. Local DuckDB SQL tests do not verify Athena deployment.

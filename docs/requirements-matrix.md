# Traceability to PROJECT NEXUS capstone brief (10-page DOCX)

This package focuses on the **no-Azure streaming track first**. A working Docker/K8s run has not been executed in the authoring environment.

| Capstone requirement | In this package | Notes |
|---|---|---|
| BR-06 Kafka event ingestion | Implemented source | Python publishes supplied JSONL, Spark Kafka subscription |
| BR-07 Event timestamps and windows | Implemented source | Configurable 5-minute event-time windows, 30-minute watermark |
| BR-02/03 validation and duplicate handling | Implemented source | Invalid events to rejected, within-watermark event-ID dedup |
| BR-08 lineage | Implemented source | Kafka key, topic, partition, offset, timestamp, raw JSON |
| BR-09 restart checkpoint | Implemented source | Persistent per-query checkpoints; test on target machine |
| BR-13 Iceberg aggregate tables | Implemented optional publisher | Local Hadoop catalog; MERGE, snapshots, schema evolution, time travel demo |
| BR-14 containers and Kubernetes | Included manifests and Dockerfiles | Single-node local KRaft broker, Python producer, Spark consumer, PV claims; test on target cluster |
| BR-11 Git/CI | Included | GitHub Actions build/tests and producer/streaming image builds; not run remotely |
| BR-01/04/05 historical batch and sales | Deferred | Uploaded historical datasets retained for Azure phase and star schema |
| BR-10 ADLS+Databricks | Deferred/blocker | No Azure account; local processing documented as permissible fallback |
| Cloud Delta raw/curated/rejected | Deferred | Local streaming outputs are Parquet; migrate to Delta with ADLS/Databricks phase |
| Full order-state reconciliation | Deferred | ORDER_UPDATED and ORDER_CANCELLED retained, not treated as fresh recognized revenue |

## Design decisions / assumptions

- `ORDER_CREATED` counts as *new-order activity*, not recognized revenue; treat final sales as a later reconciled metric.
- The supplied event times are 2026-11-19. Replaying them today models arrival, but does not convert them to current business event time.
- If events arrive after a watermark has advanced past their time, Spark may drop them. Event-ID deduplication is bounded, not forever/global.
- Historical product/customer tables and surrogate keys for `Dim_Customer`, `Dim_Product` and `Dim_Date` are not needed for the ingest path. The star schema is the target analytical model.
- One order row currently represents one product, but future order lines require a composite line identifier/grain.
- Local Parquet+checkpoint persistence demonstrates processing but is not an exactly-once transactional lakehouse design; Iceberg MERGE makes published analytics upsertable.
- Window update files are latest-value updates by `(window_start_utc, window_end_utc, batch_id)`, not additive changes to the previous value.
- Kafka broker and storage in included Kubernetes manifests are development-grade single-node deployments, not a production HA cluster.

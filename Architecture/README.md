# PROJECT NEXUS Architecture

## Pipeline A: Historical Batch Processing

```mermaid
flowchart LR
  A[Historical CSV Files] --> B[ADLS Raw]
  B --> C[Databricks Bronze Delta]
  C --> D[Silver Validation and Deduplication]
  D --> E[Curated Delta]
  D --> F[Rejected and Duplicate Audit]
  E --> G[Gold Dimensions and Fact Sales]
  G --> H[Sales Aggregations and KPIs]
  H --> I[Data Quality Checks]
```

## Pipeline B: Real-Time Processing

```mermaid
flowchart LR
  A[JSON Order Events] --> B[Python Producer]
  B --> C[Kafka]
  C --> D[Scala Spark Streaming]
  D --> E[Validated and Rejected Parquet]
  D --> F[Event-Time Windows]
  F --> G[Iceberg MERGE Publisher]
  G --> H[Iceberg Analytics Tables]
```

## Deployment

Batch: Azure Databricks + ADLS Gen2 + Delta Lake.

Streaming: local Docker/Kubernetes + Kafka + Spark + Iceberg.

These are separate demonstrated pipelines; automatic integration
of historical and real-time order state is a future enhancement.

# PROJECT NEXUS — Documentation

This directory contains technical specifications, architecture comparisons, design mappings, and operational runbooks for **PROJECT NEXUS**.

## Table of Contents

| Document | Description |
|:---|:---|
| **[Local Streaming Guide](file:///docs/local-streaming-guide.md)** | Step-by-step developer guide for running Kafka, Spark Structured Streaming, and Iceberg locally via Docker and sbt. |
| **[Delta vs. Iceberg Evaluation](file:///docs/delta-vs-iceberg.md)** | Architectural deep-dive comparing Delta Lake and Apache Iceberg across ACID guarantees, schema evolution, time-travel, and ecosystem integration. |
| **[Star Schema Mapping](file:///docs/star-schema-mapping.md)** | Detailed entity mappings from source relational tables to Gold dimensional model (`Fact_Sales`, `Dim_Customer`, `Dim_Product`, `Dim_Date`). |
| **[Requirements Matrix](file:///docs/requirements-matrix.md)** | Functional and non-functional requirements tracking for the dual-engine lakehouse platform. |
| **[Requirements Traceability](file:///docs/requirements-traceability.md)** | Traceability matrix mapping system components, pipelines, and validation suites back to business requirements. |
| **[Initial Streaming Requirements](file:///docs/requirements-matrix-streaming-initial.md)** | Baseline streaming specifications, watermark definitions, and windowing rules. |

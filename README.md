# PROJECT NEXUS
## Unified Retail Intelligence & Real-Time Data Platform

An end-to-end retail Data Engineering capstone using Microsoft Azure,
Azure Databricks, Scala, Apache Spark, Apache Kafka, Delta Lake,
Apache Iceberg, Docker, Kubernetes, and GitHub Actions.

## Architecture

PROJECT NEXUS contains two complementary pipelines.

### A. Historical Batch Processing

Historical CSV files
-> Azure ADLS Gen2 Raw
-> Databricks Bronze (Delta)
-> Silver Validation, Deduplication and Rejected Records
-> Gold Dimensions, Fact Sales and Aggregates
-> Data Quality Reports

### B. Real-Time Streaming

JSON Order Events
-> Python Kafka Producer
-> Apache Kafka
-> Scala Spark Structured Streaming
-> Valid, Rejected and Event-Time Window Outputs
-> Apache Iceberg Analytics

See [Architecture](Architecture/README.md).

## Technology Stack

| Component | Technology |
|---|---|
| Cloud | Microsoft Azure |
| Cloud Storage | Azure Data Lake Storage Gen2 |
| Batch Processing | Scala, Apache Spark, Azure Databricks |
| Streaming | Apache Kafka, Spark Structured Streaming |
| Lakehouse | Delta Lake, Apache Iceberg |
| Containerization | Docker, Kubernetes |
| Build | sbt, Scala 2.12 |
| CI/CD | GitHub Actions |
| Testing | ScalaTest, Data Quality Checks |

## Historical Batch Processing

The following four original Scala Databricks notebooks are included:

1. 01_Bronze_Ingestion.scala
2. 02_Silver_Transformation.scala
3. 03_Gold_Transformation.scala
4. 04_Data_Quality_Checks.scala

Location: Batch_Processing/Databricks_Notebooks/

The Azure Databricks workflow PROJECT_NEXUS_ETL_PIPELINE completed
its four dependent tasks successfully in the verified execution.

### Batch Data Quality Results

| Metric | Value |
|---|---:|
| Bronze Records | 230 |
| Silver Records | 146 |
| Rejected Records | 70 |
| Duplicate Extras | 14 |
| Fact Sales Records | 90 |
| Delivered Orders | 51 |
| Revenue for Delivered Orders | INR 253,287.00 |
| Data Quality Checks | 10 PASS |

Reconciliation: 230 = 146 + 70 + 14

The implemented revenue model uses catalog prices and the DELIVERED
order status. It is a capstone business rule, not a full accounting model.

See [Batch Processing](Batch_Processing/README.md).

## Real-Time Processing

The streaming implementation includes:

- Python Kafka producer
- Scala Spark Structured Streaming
- Event-time windows and watermarks
- Validation, rejected events and deduplication
- Persistent checkpoints
- Apache Iceberg MERGE publishing
- Snapshot history, time travel and schema evolution
- Docker and Kubernetes deployment configuration

Actual code is preserved in src/main/scala/com/nexus/.

See [Real-Time Streaming](Real_Time_Streaming/README.md).

## Repository Structure

- Architecture/ - Pipeline diagrams and design
- Batch_Processing/ - Azure Databricks notebooks and evidence
- Real_Time_Streaming/ - Streaming component overview
- src/ - Scala streaming implementation and tests
- producer/ - Python event producer
- data/nexus/batch/ - Historical source datasets
- data/nexus/streaming/ - Sample JSON order events
- docker/ - Container image definitions
- k8s/ - Kubernetes manifests
- docs/ - Technical implementation guides
- scripts/ - Verification utilities
- Testing/ - Test documentation
- CI_CD/ - CI workflow documentation
- Documentation/ - Requirements and project notes
- .github/workflows/ - GitHub Actions CI

## Local Streaming Quick Start

Prerequisites: Java 17, sbt, Python 3.10+, Docker.

```bash
sbt -batch test
docker compose up -d
sbt "runMain com.nexus.StreamingJob"
```

In a second terminal:

```bash
python producer/replay_events.py --delay 0.6
```

The streaming guide documents Kafka topic setup, checkpoint recovery,
Iceberg publishing, Docker image builds and Kubernetes deployment.

See [Local Streaming Guide](docs/local-streaming-guide.md).

## Azure Batch Execution

Import the four Scala notebooks into an Azure Databricks workspace,
configure authorized ADLS Gen2 access, and execute them in order:

Bronze -> Silver -> Gold -> Data Quality.

The notebooks reference the project's Azure storage account.
No storage credentials are committed to this repository.

## CI/CD

The existing .github/workflows/ci.yml performs Scala compilation,
unit tests, sample validation and Docker image builds.

Successful GitHub-hosted CI execution must be checked after the push.
The workflow does not deploy the Azure Databricks notebooks.

## Known Limitations

- Batch and streaming are separately demonstrated pipelines.
- Full cross-pipeline order-state reconciliation is not implemented.
- Streaming sinks are local Parquet and local Iceberg.
- Cloud Iceberg catalog deployment remains outside the demonstrated scope.
- Kubernetes batch Job/CronJob evidence is pending.
- Current historical sales values use catalog prices.
- The project is a capstone, not a production-grade commercial service.

## Project Documentation

See Architecture/, Batch_Processing/, Documentation/,
Testing/ and CI_CD/ for design and execution details.

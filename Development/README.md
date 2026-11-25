# PROJECT NEXUS — Development Environment

This directory houses the core development codebase, build configurations, and container definitions for PROJECT NEXUS.

## Structure

- **`src/main/scala/com/nexus/`**: Core Scala application logic:
  - `StreamingJob.scala`: Main Apache Spark Structured Streaming consumer application.
  - `EventTransforms.scala`: Stateful event transforms, watermarking, and window definitions.
  - `IcebergPublishJob.scala`: Apache Iceberg atomic `MERGE INTO` publisher.
  - `IcebergEvidenceJob.scala`: Iceberg snapshot inspection, time travel, and schema evolution.
  - `InspectOutputs.scala` & `VerifyStreamingOutput.scala`: Local Parquet verification utilities.
- **`src/test/scala/com/nexus/`**: ScalaTest test suites (`EventTransformsSpec.scala`).
- **`build.sbt`**: Scala 2.12 build definition, Spark 3.5 dependencies, Iceberg connectors, and Kafka libraries.
- **`project/build.properties`**: sbt version definition (sbt 1.10.11).
- **`docker/`**: Multi-stage non-root container definitions (`Dockerfile.streaming`, `Dockerfile.producer`).
- **`compose.yaml`**: Local multi-service cluster definition (Kafka, Zookeeper, Spark).
- **`requirements.txt`**: Python dependencies for event replay and verification scripts.

## Quick Start (Local Development)

```bash
# 1. Compile and run test suite
sbt test

# 2. Package application jar
sbt package

# 3. Start local Kafka broker
docker compose up -d
```

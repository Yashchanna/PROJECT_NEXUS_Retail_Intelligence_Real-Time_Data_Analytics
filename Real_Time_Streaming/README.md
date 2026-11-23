# Real-Time Streaming

The working implementation remains in its sbt-compatible locations:

- ../src/main/scala/com/nexus/
- ../src/test/scala/com/nexus/
- ../producer/
- ../docker/
- ../k8s/

Capabilities include Kafka ingestion, event-time windows,
watermarks, validation, deduplication, checkpoints and Iceberg MERGE.

The local Iceberg implementation demonstrates snapshots, time travel
and schema evolution.

See ../docs/local-streaming-guide.md for commands and limitations.

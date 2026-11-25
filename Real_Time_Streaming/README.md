# ⚡ Pipeline B: Real-Time Event-Driven Streaming

High-level directory dedicated to the low-latency real-time streaming pipeline of PROJECT NEXUS.

---

## Architecture Overview

```
JSONL Order Events
        │
Python Replay Producer (producer/replay_events.py)
        │
Apache Kafka (Topic: retail-order-events)
        │
Spark Structured Streaming (src/.../StreamingJob.scala)
  ├── Bronze Raw Parquet (output/bronze/)
  ├── Dead-Letter Quarantine (output/rejected/)
  └── 5-Minute Tumbling Windows (output/window_updates/)
        │
Apache Iceberg Publisher (src/.../IcebergPublishJob.scala)
        │
Apache Iceberg Lakehouse (output/iceberg-warehouse/)
  ├── Table: window_analytics (Atomic MERGE INTO)
  └── Table: product_activity (Time Travel & Schema Evolution)
```

---

## Directory Contents

- **`src/`**: Scala Apache Spark streaming application source code and tests.
- **`producer/`**: Python Kafka event replay producer (`replay_events.py`) simulating live order lifecycle traffic.
- **`docker/`**: Multi-stage non-root container definitions (`Dockerfile.streaming`, `Dockerfile.producer`).
- **`compose.yaml`**: Local Docker Compose cluster running Kafka broker and Zookeeper.
- **`k8s/`**: Kubernetes production manifests (Deployments, Services, ConfigMaps, PVCs).
- **`scripts/`**: Preflight validation (`check_sample.py`) and Docker execution utilities (`run_iceberg_in_docker.sh`).
- **`build.sbt` & `project/`**: Self-contained Scala build definitions for the streaming engine.
- **`requirements.txt`**: Python dependencies for the event producer and validation tools.

---

## Key Streaming Capabilities

1. **5-Minute Tumbling Event Windows:** Continuous aggregation of order volume and financial activity grouped by transaction event time.
2. **10-Minute Watermarking:** Enforces durable watermark thresholds to handle late-arriving and out-of-order events gracefully.
3. **Stateful Deduplication:** Deduplicates events based on `event_id` within the watermark horizon to prevent double-counting.
4. **Apache Iceberg ACID MERGE INTO:** Atomically merges window updates and product metrics without duplicate rows.
5. **Snapshot History & Time Travel:** Supports `VERSION AS OF` point-in-time historical audit queries.
6. **Dynamic Schema Evolution:** Demonstrates adding new columns (`sales_channel`) without table rewriting or service downtime.

---

## Quick Start (Local Streaming)

```bash
# 1. Start Kafka broker
docker compose up -d

# 2. Start Spark Structured Streaming consumer
sbt "runMain com.nexus.StreamingJob"

# 3. In a separate terminal, replay order events
python producer/replay_events.py --delay 0.6

# 4. Publish to Apache Iceberg and verify snapshots
sbt "runMain com.nexus.IcebergPublishJob"
sbt "runMain com.nexus.IcebergEvidenceJob"
```

See [`../docs/local-streaming-guide.md`](../docs/local-streaming-guide.md) for full commands, configurations, and tuning parameters.

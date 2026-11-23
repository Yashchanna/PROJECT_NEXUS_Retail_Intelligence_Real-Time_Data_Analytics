# PROJECT NEXUS — Local Streaming Capstone (Azure-ready approach)

**Scope:** Implements the real-time half of the uploaded 10-page capstone brief first, without an Azure account. Reuses the supplied `data/nexus/` files and the previous local starter. **Not the full capstone yet:** historical Azure batch, Delta persistence, fully reconciled Fact_Order, and cloud deployment remain future phases. See `docs/requirements-matrix.md` for exact status.

## System architecture

```
provided JSONL events → Python event producer → Kafka topic retail-order-events
                                                  ↓
                                     Scala Spark Structured Streaming
                                                  ↓
                         ┌────────────────────────┼──────────────────────┐
                         ↓                        ↓                      ↓
                bronze raw Parquet       rejected Parquet       deduplicated valid Parquet
                                                                      ↓
                                                        5-minute event-time windows
                                                                      ↓
                                                      durable window update Parquet
                                                                      ↓
                                                    IcebergPublishJob (MERGE)
                                                                      ↓
                                    Iceberg window_analytics + product_activity
```

Every streaming query maintains a separate checkpoint under `output/checkpoints/`. Broker offset + source metadata are included in bronze records. Data is on local disk for host runs or a PVC for Kubernetes runs.

The star schema in the chat is a **future analytical model**: `Dim_Customer`, `Dim_Product`, `Dim_Date`, `Fact_Order`. Streaming preserves natural IDs and event-time transaction price rather than inventing surrogate keys before those dimensions are built. Streaming ORDER_UPDATED/CANCELLED events are retained for later order-state reconciliation; do **not** interpret `new_order_value` as recognized revenue.

## Before you start

**On Windows:** use Docker Desktop with WSL2 integration. Run shell commands inside Ubuntu WSL2 where possible. Install:

- Docker Desktop + Compose, and Kubernetes enabled if doing K8s stage.
- OpenJDK **17** and `sbt` (1.10.11 wrapper version in `project/build.properties`).
- Python **3.10+** with pip.
- For Kafka + local Spark, ~8 GB free RAM is recommended; for K8s demo budget ~12-16 GB machine RAM.
- Internet on first launch so sbt/Spark connectors/Docker images can download.

All commands below assume the terminal's working directory is this extracted project root.

## Stage A — Kafka + Spark on host (DO THIS FIRST)

### Terminal 1: Kafka

```bash
docker compose up -d
docker compose ps
```

Create exactly one Kafka topic (single-node demonstration):

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --create --if-not-exists --topic retail-order-events --partitions 1 --replication-factor 1
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --describe --topic retail-order-events
```

### Terminal 2: Validate and run Scala consumer

```bash
sbt test
sbt "runMain com.nexus.StreamingJob"
```

Keep this terminal running. First run can be slow while sbt downloads Spark 3.5.8 and adapters. Four streaming queries will run: bronze, rejected, valid, and windows.

Optional tuning: `NEXUS_KAFKA_BOOTSTRAP`, `NEXUS_KAFKA_TOPIC`, `NEXUS_WINDOW`, `NEXUS_WATERMARK`, `NEXUS_TRIGGER`, `NEXUS_OUTPUT_DIR`, `NEXUS_SHUFFLE_PARTITIONS`.

### Terminal 3: Replay provided sample events

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python producer/replay_events.py --limit 10 --delay 0.6
# Then send remaining events (the first ten WILL replay; event IDs are deduplicated):
python producer/replay_events.py --delay 0.6
```

You can start directly with all 62 events instead of `--limit 10`. The file has **62 physically supplied messages**, including duplicates and invalid records. `event_time` is Nov 19, 2026 even when replayed on another date. JSON messages go into Kafka in **ingest-time order**, preserving late-arrival examples.

### Inspect outputs

After the producer finishes, let the Spark consumer process the backlog; watch its console and inspect `output/` directories.

```bash
# Stop the consumer with Ctrl+C once processing has settled.
sbt "runMain com.nexus.InspectOutputs"
python scripts/check_sample.py
```

The script reports input-file sanity checks, not Spark execution counts. Spark unique count can differ because watermarks drop sufficiently late events. `output/window_updates/batch_id=<id>/...parquet` contains the **latest update per window for each processing microbatch**, not one final row per window.

### Restart / checkpoint test

1. Run the consumer, replay 10 events, then stop the consumer (`Ctrl+C`).
2. Restart the same `sbt "runMain com.nexus.StreamingJob"` without deleting anything in `output/`.
3. Replay more events (or the full sample). Verify the new offsets are processed and duplicates within watermark are not counted twice.
4. Preserve **both output and checkpoint** directories. Never reset just the checkpoint of an existing output sink.

## Stage B — Iceberg analytical tables (run after Stage A output is present)

Stop the streaming consumer first to avoid reading files while they are being written. Then:

```bash
sbt "runMain com.nexus.IcebergPublishJob"
sbt "runMain com.nexus.IcebergEvidenceJob"
```

The publisher uses Iceberg with a local **Hadoop catalog** and warehouse at `output/iceberg-warehouse` (configurable through `NEXUS_ICEBERG_WAREHOUSE`). It runs a `MERGE INTO` on latest window updates and per-product new-order activity. It explicitly does not add changed window values together.

Evidence job prints Iceberg snapshots, runs a `VERSION AS OF` time-travel read, and adds an optional `sales_channel` column if missing. To demonstrate multiple snapshots, publish additional distinct events, allow the consumer to write fresh output, and re-run the publisher (taking care that it reads completed data). Iceberg `MERGE`, snapshots/time travel, and schema evolution are the three feature demonstrations.

**Important:** Local Hadoop catalog is a development substitute. The capstone's Azure target is an appropriately configured Iceberg catalog whose warehouse is on ADLS Gen2, or a catalog service deployed with Kubernetes. Moving a local warehouse directory is not a transparent catalog migration.

## Stage C — Docker + Kubernetes (local cluster)

Start Kubernetes in Docker Desktop, or use kind/minikube. Do **not** run Compose Kafka and Kubernetes Kafka against the same port; the K8s broker is internal to its cluster.

Build two **multi-stage, non-root** images:

```bash
docker build -f docker/Dockerfile.producer -t nexus-producer:0.2.0 .
docker build -f docker/Dockerfile.streaming -t nexus-streaming:0.2.0 .
```

The streaming Dockerfile has a thin Spark jar and pulls version-matched Kafka and Iceberg adapter JARs through `spark-submit --packages` at runtime. The first container start requires internet (or a prepopulated Maven/Ivy cache). Production images should bundle verified dependencies.

If using **kind**, load both images into the kind cluster before applying the manifests:

```bash
kind load docker-image nexus-producer:0.2.0 nexus-streaming:0.2.0
```

Apply the manifests in order:

```bash
kubectl apply -f k8s/00-namespace.yaml
kubectl apply -f k8s/10-infra.yaml
kubectl apply -f k8s/15-secrets-empty.yaml
kubectl -n nexus rollout status deployment/nexus-kafka --timeout=300s
kubectl apply -f k8s/20-workloads.yaml
kubectl -n nexus get pods,pvc,deploy,job
kubectl -n nexus logs deployment/nexus-producer --tail=30
kubectl -n nexus logs deployment/nexus-streaming --tail=80
```

The sample producer publishes once and then idles. Scaling it or restarting its pod replays historical sample IDs. Those records are deliberately replayable, but watermark-bounded dedup is **not** an indefinite exactly-once guarantee.

### Kubernetes checkpoint/recovery demo

```bash
kubectl -n nexus get pods -l app=nexus-streaming
kubectl -n nexus delete pod -l app=nexus-streaming
kubectl -n nexus rollout status deployment/nexus-streaming --timeout=300s
kubectl -n nexus logs deployment/nexus-streaming --tail=100
kubectl -n nexus get pvc
```

Spark consumer uses a PVC named `nexus-stream-output`; do **not** scale streaming replicas beyond one against the same checkpoint. Kafka uses separate PVC `nexus-kafka-data`. Persistent storage + checkpoints are necessary for restart demo, but external side effects must also be idempotent for true end-to-end correctness.

### Configuration, producer scaling, rollout and rollback

```bash
kubectl -n nexus get configmap nexus-config -o yaml
kubectl -n nexus scale deployment/nexus-producer --replicas=2
kubectl -n nexus get pods -l app=nexus-producer
kubectl -n nexus rollout restart deployment/nexus-producer
kubectl -n nexus rollout status deployment/nexus-producer
kubectl -n nexus rollout history deployment/nexus-producer
kubectl -n nexus rollout undo deployment/nexus-producer
kubectl -n nexus describe deployment nexus-producer
kubectl -n nexus get events --sort-by=.lastTimestamp
```

Change `NEXUS_WINDOW`/`NEXUS_WATERMARK` in ConfigMap and restart the stream **with a new checkpoint/output area** if stateful schema or operator configuration changes require a fresh query. Do not casually reuse state from incompatible stateful query plans.

### Optional: Iceberg publisher as a Kubernetes Job

Once the stream has written parquet outputs, and ideally after pausing the streaming producer/consumer:

```bash
kubectl apply -f k8s-optional-publisher.yaml
kubectl -n nexus logs job/nexus-iceberg-publish
```

This uses the same output PVC. If your cluster's storage class does not allow multiple pods to attach the volume at once, scale streaming deployment to 0 before starting the publisher:

```bash
kubectl -n nexus scale deployment/nexus-streaming --replicas=0
kubectl -n nexus apply -f k8s-optional-publisher.yaml
```

For another run, `kubectl -n nexus delete job nexus-iceberg-publish` before applying the Job again.

## CI and quality gates

`.github/workflows/ci.yml` performs Scala unit tests, sample checks, Docker Compose validation, and image builds. Push this extracted project to a GitHub repository to get actual CI evidence. The workflow uses online dependencies and has not been executed in the authoring sandbox.

## Local environment versus final brief

| Component | Current local version | Future Azure version |
|---|---|---|
| Kafka transport | Docker/Kubernetes Kafka | Cloud-reachable Kafka or Event Hubs Kafka endpoint |
| Streaming | Local Spark / Kubernetes container | Kubernetes/AKS or Databricks |
| Raw/clean/rejected sinks | Local Parquet | Delta Lake tables on ADLS Gen2 |
| Streaming window analytics | Local Iceberg | Iceberg warehouse/catalog on cloud storage |
| Checkpoint | Local filesystem/PVC | ADLS/cloud-durable state directory |
| Historic products/customers/orders | Supplied CSVs retained | Databricks Scala Spark Batch + Delta Lake |
| Dim/Fact star model | Planned, see docs | Dim_Customer, Dim_Product, Dim_Date, Fact_Order |

The capstone requires historical files, Delta Lake, Azure batch execution (or documented blocker), and Kubernetes evidence. Completing the local streaming track **does not mean the entire capstone is completed**.

## Known limitations and design assumptions

- **NOT LIVE-VERIFIED:** source/static validation in the authoring environment is possible, but Docker, kubectl and sbt runtimes are absent there; you must run the steps on your laptop.
- Checkpointed Parquet outputs are append-only; `ORDER_UPDATED` and `ORDER_CANCELLED` require future business-state reconciliation before *recognized revenue* can be claimed.
- Window update Parquet uses deterministic `batch_id` overwrite as a local idempotency demonstration; migrate to Delta/Iceberg transactional sinks on cloud object storage.
- `withWatermark(...).dropDuplicatesWithinWatermark("event_id")` only de-duplicates within Spark's state retention assumptions, and may drop late events outside the watermark.
- The first Kubernetes Spark start downloads `--packages`, which can delay availability; increase startup probe threshold as needed.
- Kubernetes broker setup is single-node/dev only, Kafka PLAINTEXT (not authenticated). The demo uses an EMPTY Secret placeholder, with no confidential values stored in Git. Do not expose the Kafka port publicly. When cloud secrets exist, pass them through Kubernetes Secret or a managed identity integration, not Git or Docker images.
- Probe configuration demonstrates health checks, but a production producer needs a business-specific Kafka-aware readiness endpoint.
- The full historical star-schema fact uses `unit_price_at_order`; do not use latest product price to value past purchases.
- `Intelligent` revenue metrics are subject to order status, updates, cancellations, returns, and cross-stream reconciliation.

See `docs/star-schema-mapping.md` and `docs/requirements-matrix.md` for further decisions.

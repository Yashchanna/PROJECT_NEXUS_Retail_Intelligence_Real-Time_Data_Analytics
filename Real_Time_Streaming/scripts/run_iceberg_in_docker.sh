#!/usr/bin/env bash
set -euo pipefail
# Run Iceberg publisher using built streaming image and shared local output volume.
# Stream container should be stopped first to ensure input files are complete.
docker run --rm \
  -v "$PWD/output:/app/output" \
  --entrypoint /opt/spark/bin/spark-submit \
  nexus-streaming:0.2.0 \
  --master 'local[2]' \
  --driver-memory 2g \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.1 \
  --class com.nexus.IcebergPublishJob /opt/nexus/nexus.jar

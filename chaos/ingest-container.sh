#!/usr/bin/env bash
# Run the ingestor as a container on the compose network, so chaos/run.sh can
# partition it with `docker network disconnect`. Stop the host ingestor first:
# both would produce every trade (Flink dedups, but gap metrics double).
# It resumes from the last trade ids in Kafka, so the handover is lossless.
set -euo pipefail
cd "$(dirname "$0")/.."
tar -cf - go.mod go.sum gen schemas ingest replay | docker build -q -t tidemark-go:0.3 -f ingest/Dockerfile - >/dev/null
docker rm -f ingest-chaos >/dev/null 2>&1 || true
docker run -d --name ingest-chaos --network tidemark -p 9112:9102 tidemark-go:0.3 \
  --brokers=redpanda-0:9092 --registry=http://redpanda-0:8081 --record-dir= --debug-endpoints \
  --ingest-id=ingest-chaos
echo "ingest-chaos running; metrics on :9112"

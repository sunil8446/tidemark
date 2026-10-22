#!/usr/bin/env bash
# Probe each service that is running. Services from inactive profiles are skipped.
set -uo pipefail

fail=0
running() { docker ps --format '{{.Names}}' | grep -qx "$1"; }
check() {
  local name=$1 desc=$2; shift 2
  if ! running "$name"; then printf "skip  %-11s (not running)\n" "$name"; return; fi
  if "$@" >/dev/null 2>&1; then printf "ok    %-11s %s\n" "$name" "$desc"
  else printf "FAIL  %-11s %s\n" "$name" "$desc"; fail=1; fi
}

check redpanda-0 "cluster healthy"        sh -c "docker exec redpanda-0 rpk cluster health | grep -Eq 'Healthy:.+true'"
check redpanda-0 "schema registry :18081" curl -fs http://localhost:18081/subjects
check redpanda-1 "joined cluster"         sh -c "docker exec redpanda-0 rpk cluster info -b | grep -q redpanda-1"
check redpanda-2 "joined cluster"         sh -c "docker exec redpanda-0 rpk cluster info -b | grep -q redpanda-2"
check console    "ui :8080"               curl -fs http://localhost:8080/
check minio      "s3 :9000 live"          curl -fs http://localhost:9000/minio/health/live
check postgres   "accepting connections"  docker exec postgres pg_isready -q
check trino      "iceberg catalog"        sh -c "docker exec trino trino --execute 'SHOW SCHEMAS FROM iceberg' | grep -q information_schema"
check clickhouse "query :8123"            sh -c "curl -fs 'http://localhost:8123/ping' | grep -q Ok"
check prometheus "ready :9090"            curl -fs http://localhost:9090/-/ready
check grafana    "health :3000"           curl -fs http://localhost:3000/api/health

exit $fail

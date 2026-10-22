#!/usr/bin/env bash
# Create Tidemark topics. Replication factor follows the number of live brokers:
# RF=1 in the lite profile, RF=3 with min.insync.replicas=2 in the ha profile.
set -euo pipefail

rpk() { docker exec redpanda-0 rpk "$@"; }

brokers=$(rpk cluster info -b 2>/dev/null | awk 'NR>1 && $1 ~ /^[0-9]+\*?$/' | wc -l | tr -d ' ')
if [[ "$brokers" -ge 3 ]]; then rf=3; min_isr=2; else rf=1; min_isr=1; fi
echo "brokers=$brokers rf=$rf min.insync.replicas=$min_isr"

create() {
  local topic=$1 partitions=$2; shift 2
  if rpk topic describe "$topic" >/dev/null 2>&1; then
    echo "exists  $topic"
  else
    rpk topic create "$topic" -p "$partitions" -r "$rf" -c min.insync.replicas="$min_isr" "$@"
  fi
}

# Keyed by venue:product_id so ordering holds per product within a partition.
create trades.v1      6 -c retention.ms=604800000
create book.v1        6 -c retention.ms=259200000
create gaps.v1        1 -c retention.ms=2592000000
create alerts.v1      3 -c retention.ms=2592000000
create ingest.dlq.v1  1 -c retention.ms=2592000000

rpk topic list

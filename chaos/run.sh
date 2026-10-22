#!/usr/bin/env bash
# Repeatable failure injection for the local stack (the plan's cut line:
# docker kill / network disconnect instead of Chaos Mesh; the Chaos Mesh
# equivalents for Kubernetes are in chaos/k8s/).
#
#   chaos/run.sh <experiment>
#
# Experiments:
#   taskmanager-kill   kill the Flink TaskManager; Flink restores from the last checkpoint
#   broker-kill        kill one of three Redpanda brokers (needs `make up-ha` and RF=3 topics)
#   ingest-partition   cut the containerised ingestor off the network for 60 s
#   websocket-drop     force both venue WebSockets to reconnect and drop 20 s of frames
#
# Each run measures time to recovery, then reconciles the affected window in
# Iceberg (completeness + duplicates) and writes chaos/results/<experiment>-<ts>.json.
set -euo pipefail
cd "$(dirname "$0")/.."
exp=${1:?experiment}
out=chaos/results; mkdir -p "$out"
ts() { date -u +%Y-%m-%dT%H:%M:%SZ; }
now() { python3 -c 'import time; print(int(time.time()))'; }

jobs_running() {
  curl -s localhost:8081/jobs/overview | python3 -c "
import sys, json
names = {'tidemark-lakehouse', 'tidemark-analytics', 'tidemark-detectors-live'}
state = {j['name']: j['state'] for j in json.load(sys.stdin)['jobs'] if j['name'] in names and j['state'] != 'CANCELED'}
sys.exit(0 if state and all(s == 'RUNNING' for s in state.values()) else 1)"
}
lakehouse_counts() { # "<restored> <completed>" checkpoint counters of the lakehouse job
  jid=$(curl -s localhost:8081/jobs/overview | python3 -c "import sys,json;print([j['jid'] for j in json.load(sys.stdin)['jobs'] if j['name']=='tidemark-lakehouse' and j['state']!='CANCELED'][0])")
  curl -s "localhost:8081/jobs/$jid/checkpoints" | python3 -c "import sys,json;c=json.load(sys.stdin)['counts'];print(c['restored'], c['completed'])"
}

start=$(now); started_at=$(ts)
case "$exp" in
  taskmanager-kill)
    read -r restored0 _ <<< "$(lakehouse_counts)"
    docker kill taskmanager >/dev/null
    sleep 2
    docker start taskmanager >/dev/null
    # Recovered = the lakehouse job restored from a checkpoint, every job is
    # RUNNING again, and a NEW checkpoint (= Iceberg commit) completed after the restore.
    until read -r r c <<< "$(lakehouse_counts)" && [ "$r" -gt "$restored0" ]; do sleep 2; done
    read -r _ completed_at_restore <<< "$(lakehouse_counts)"
    until jobs_running && read -r _ c <<< "$(lakehouse_counts)" && [ "$c" -gt "$completed_at_restore" ]; do sleep 2; done
    recovered=$(now)
    ;;
  broker-kill)
    docker exec redpanda-0 rpk cluster info -b | grep -q redpanda-2 || { echo "needs 3 brokers: make up-ha"; exit 1; }
    docker kill redpanda-2 >/dev/null
    sleep 60
    docker start redpanda-2 >/dev/null
    until docker exec redpanda-0 rpk cluster health | grep -Eq 'Healthy:.+true'; do sleep 2; done
    recovered=$(now)
    ;;
  ingest-partition)
    docker inspect ingest-chaos >/dev/null 2>&1 || { echo "needs the containerised ingestor (chaos/ingest-container.sh)"; exit 1; }
    docker network disconnect tidemark ingest-chaos
    sleep 60
    docker network connect tidemark ingest-chaos
    # Recovered = no gap still open after reconnect.
    sleep 5
    until [ "$(curl -s localhost:9112/metrics | awk '/^tidemark_ingest_gaps_open/{s+=$2} END{print s+0}')" = "0" ]; do sleep 2; done
    recovered=$(now)
    ;;
  websocket-drop)
    for v in coinbase kraken; do
      curl -s -XPOST "localhost:9102/debug/drop/$v?for=20s" >/dev/null
      curl -s -XPOST "localhost:9102/debug/reconnect/$v" >/dev/null
    done
    sleep 25
    until [ "$(curl -s localhost:9102/metrics | awk '/^tidemark_ingest_gaps_open/{s+=$2} END{print s+0}')" = "0" ]; do sleep 2; done
    recovered=$(now)
    ;;
  *) echo "unknown experiment $exp"; exit 2 ;;
esac

# Let the lakehouse commit what happened (60 s checkpoints), then check the window.
sleep 150
window=$(( $(now) - start + 600 ))s
report=$(go run ./ingest/cmd/reconcile --window "$window" --settle 2m --repair=false --record=false 2>/dev/null)
python3 - "$exp" "$started_at" "$(( recovered - start ))" "$window" "$out" <<PY
import json, sys, time
exp, started, rto, window, out = sys.argv[1:]
rep = json.loads('''$report''')
res = {
    "experiment": exp, "started_at": started, "recovery_seconds": int(rto),
    "reconcile_window": window,
    "expected_ids": rep["expected_ids"], "present_ids": rep["present_ids"],
    "completeness_pct": rep["completeness_pct_before_repair"], "duplicate_rows": rep["duplicate_rows"],
}
path = f"{out}/{exp}-{started.replace(':', '')}.json"
json.dump(res, open(path, "w"), indent=2)
print(json.dumps(res, indent=2))
PY

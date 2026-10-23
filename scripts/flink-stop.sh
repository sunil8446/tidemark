#!/usr/bin/env bash
# Stop a Flink job with a savepoint via the REST API and print the savepoint path.
# (The CLI gives up after its 60 s client timeout, even when the savepoint
# completes, which happens on a loaded laptop.)
#   scripts/flink-stop.sh <job-name>
set -euo pipefail
name=${1:?job name}
api=${FLINK_REST:-http://localhost:8081}
jid=$(curl -s "$api/jobs/overview" | python3 -c "import sys,json;r=[j['jid'] for j in json.load(sys.stdin)['jobs'] if j['name']=='$name' and j['state']=='RUNNING'];print(r[0] if r else '')")
[ -n "$jid" ] || { echo "no running job named $name" >&2; exit 1; }
trigger=$(curl -s -X POST -H 'Content-Type: application/json' \
  -d '{"targetDirectory":"s3://checkpoints/savepoints","drain":false}' "$api/jobs/$jid/stop" | python3 -c "import sys,json;print(json.load(sys.stdin)['request-id'])")
for _ in $(seq 1 180); do
  out=$(curl -s "$api/jobs/$jid/savepoints/$trigger")
  status=$(echo "$out" | python3 -c "import sys,json;print(json.load(sys.stdin)['status']['id'])")
  if [ "$status" = "COMPLETED" ]; then
    echo "$out" | python3 -c "import sys,json;o=json.load(sys.stdin)['operation'];print(o.get('location') or ('FAILED: '+str(o.get('failure-cause',{}).get('stack-trace',''))[:300]))"
    exit 0
  fi
  sleep 5
done
echo "savepoint for $name did not complete in 15 minutes" >&2
exit 1

#!/usr/bin/env bash
# Score the detectors against labelled injections.
#
#   bench/detector_eval.sh <seed> <per-pattern> [extra DetectorsJob args...]
#
# 1. recreate the *.v1.inj topics
# 2. start an evaluation detector job with LOW score thresholds; every other
#    parameter is the production default, so the threshold sweep reflects
#    what production would do
# 3. replay all recordings with labelled injections into *.v1.inj
# 4. wait for the job to drain, then sweep thresholds with `evaluate`
#
# Output: ~/.tidemark-data/eval/detectors-seed<seed>.md
set -euo pipefail
seed=${1:?seed}; per=${2:?per-pattern}; shift 2
cd "$(dirname "$0")/.."
out=~/.tidemark-data/eval; mkdir -p "$out"
group="tidemark-detectors-inj-s${seed}-$(date +%s)"

for jid in $(curl -s localhost:8081/jobs/overview | python3 -c "import sys,json;print(' '.join(j['jid'] for j in json.load(sys.stdin)['jobs'] if j['name']=='tidemark-detectors-inj' and j['state']=='RUNNING'))"); do
  docker exec jobmanager flink cancel "$jid" >/dev/null
done
for t in trades book gaps alerts labels; do
  docker exec redpanda-0 rpk topic delete "$t.v1.inj" >/dev/null 2>&1 || true
  docker exec redpanda-0 rpk topic create "$t.v1.inj" -p 3 -r 1 >/dev/null
done
curl -s -u tidemark:tidemark-local-dev http://localhost:8123/ --data-binary "ALTER TABLE tidemark.alerts DELETE WHERE stream = 'inj'" >/dev/null

docker exec jobmanager flink run -d -c dev.tidemark.flink.DetectorsJob /opt/flink/usrlib/tidemark-flink.jar \
  --topic-suffix .inj --stream inj --group "$group" \
  --spike.z 2 --spoof.depth-share 0.1 --dislocation.z 2 --wash.min-score 0.5 "$@" | grep JobID

~/.tidemark-data/bin/replay --metrics-addr "" --speed 0 --topic-suffix .inj \
  --inject-per-pattern "$per" --inject-seed "$seed" --inject-gap 60s \
  ~/.tidemark-data/recordings/*.zst 2>&1 | grep -E "injected|replayed"

# Drained = every non-empty partition has a committed offset equal to its end
# offset. (An unknown group reports lag 0, so "lag == 0" alone is not enough.)
drained() {
  docker exec redpanda-0 rpk group describe "$group" 2>/dev/null | awk '
    $1 ~ /\.v1\.inj$/ && $5 > 0 { total++; if ($3 == $5) done++ }
    END { exit !(total > 0 && done == total) }'
}
until drained; do sleep 10; done
sleep 45 # final checkpoint flushes the alert sinks
go build -o ~/.tidemark-data/bin/evaluate ./replay/cmd/evaluate
~/.tidemark-data/bin/evaluate --topic-suffix .inj --out "$out/detectors-seed${seed}.md" ${EVAL_ARGS:-}

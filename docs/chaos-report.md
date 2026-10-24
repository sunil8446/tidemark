# Chaos report

Failures injected on purpose into the running local stack, one at a time and never during an evaluation run (a TaskManager kill overlapping an eval invalidated one early run). Each experiment is followed by an independent completeness check: `reconcile --repair=false` compares the trade ids in `iceberg.market.raw_trades` with each venue's continuous id sequence over a window covering the experiment plus 10 minutes. Reproduce with `chaos/run.sh <experiment>`; raw results are in [chaos/results/](../chaos/results/).

## Planned experiments (2026-10-24)

| Experiment | Injected at (UTC) | Recovery time | Gaps repaired | Trades in window | Completeness | Duplicates | Pass |
|---|---|---:|---|---:|---:|---:|---|
| Drop both venue WebSockets and discard 20 s of frames | 15:51:47 | 27 s | 6 / 6, 214 trades over REST in 3.2–4.0 s | 11,490 | 100% | 0 | ✅ |
| Kill the Flink TaskManager | 15:55:04 | 69 s | n/a (Flink restores from checkpoint) | 11,472 | 100% | 0 | ✅ |
| Cut the ingestor off the network for 60 s | 15:59:47 | 69 s (9 s after reconnect) | 6 / 6, 1,280 trades over REST in 3.1–4.0 s | 11,625 | 100% | 0 | ✅ |
| Kill one of three Redpanda brokers | not run | | | | | | — |

**Recovery time** is measured from injection to: both feeds reconnected and no gap open (WebSocket drop, partition); or every Flink job RUNNING again *and* a new checkpoint (an Iceberg commit) completed after the restore (TaskManager kill). For the WebSocket drop and the partition it includes the injected outage itself (20 s and 60 s).

**Why the broker kill was not run.** The local stack runs one Redpanda broker with replication factor 1, so killing it is a full outage rather than a failover, which the incident below already measured. A real failover test needs `make up-ha` (three brokers) and RF=3 topics; on this 16 GB laptop the two extra brokers do not fit beside the rest of the stack. The Chaos Mesh version for Kubernetes is in [chaos/k8s/experiments.yaml](../chaos/k8s/experiments.yaml) and runs with the EKS path.

## Unplanned: Docker VM froze for 3 h 26 m (2026-10-23)

The most informative failure was not injected. The host disk filled (2.9 GB free on 228 GB, 13 GB of it stale Flink TaskManager working directories), the Docker VM hit write I/O errors at 21:27 UTC and froze: broker, Flink, lake, Trino and Prometheus all stopped. The ingestor runs on the host and kept going.

| | |
|---|---|
| Recovery | Docker restarted by hand at 00:53 UTC; Flink jobs restored from retained checkpoints |
| Automatic repair | 12 gaps detected on reconnect, **123,253 trades refilled over REST**, none unrecoverable |
| What it missed | For the first 50 minutes produce calls failed after the tracker had counted ids as seen: **20,235 trades** never reached Kafka and no gap was reported. The hourly reconcile also missed them (fixed 2 h look-back) |
| Found by | The gap report's independent Kafka audit (97.66% complete) |
| Final | `reconcile --window 8h` refetched all 20,237: the 24 h run is **864,522 / 864,522 (100%)** |
| Fixes | `tidemark_ingest_trades_lost_total` + `IngestTradesLost` alert; reconcile `--catch-up`; TaskManager clears stale `/tmp/tm_*` on start |

Details: [bench/gaps.md](../bench/gaps.md).

## Earlier: exactly-once through failure (Week 2)

A TaskManager kill and a savepoint upgrade of the lakehouse job during live ingestion: 40,492 / 40,492 trades in Iceberg, 0 duplicates.

## What these show, and what they do not

- Every injected failure ended at 100% completeness with 0 duplicates, without manual repair. The ingestor's REST backfill closes feed-side gaps in about 4 s; Flink's checkpoints keep the lake exactly-once through a worker crash.
- They are single, short failures on one host. The outage shows the weak point: a failure of the *broker* while the ingestor keeps accepting data. That path is now detected (lost-trade counter, alert) and repaired by reconcile's catch-up, but it is repaired after the fact, not prevented: preventing it needs a replicated broker, or an ingestor that stops acknowledging the feed while produce is failing.

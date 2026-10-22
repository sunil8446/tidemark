# ADR-0004: Use Flink (1.20 LTS) rather than Spark Structured Streaming

- **Status:** Accepted
- **Date:** 2026-10-22
- **Week:** 2

## Context

The stream layer must dedup on (venue, product, trade_id), bucket trades by **event time** with late corrections from REST backfill, sample a cross-venue spread every second, run Week 3 detectors with per-key timers (for example "order added, then removed within T seconds"), and land everything in Iceberg exactly once. It runs on a laptop now and on Kubernetes later.

## Decision

Apache Flink 1.20 (the 1.x LTS line), DataStream API in Java, RocksDB state with incremental checkpoints to S3/MinIO, the Flink Kafka connector, and Iceberg's `FlinkSink`.

## Alternatives considered

| Option | Why not |
|---|---|
| Spark Structured Streaming | Micro-batch: event-at-a-time timers and keyed process functions (needed for the spoofing detector) are awkward, and `flatMapGroupsWithState` is a poor fit. Latency floor is the trigger interval. Iceberg support is excellent, but that is not the bottleneck here. |
| Kafka Streams | No native Iceberg sink. State and exactly-once stay inside Kafka; the lake would need a second system. |
| Flink 2.x | Newer, but Iceberg's Flink runtime and the Kafka connector lag behind the 2.x minor releases. 1.20 has the most mature connector matrix today. Moving to 2.x is a contained upgrade later. |
| Flink SQL only | Great for bars, but dedup with TTL, as-of spread and the Week 3 detectors are clearer as DataStream code with unit-testable functions. |

## Consequences

- **Good:** true event time with watermarks, idleness and allowed lateness. Late REST-backfilled trades correct already-emitted bars, which ClickHouse then upserts. Per-key state and timers. Exactly-once into Iceberg via checkpoint-aligned commits. Savepoints made the schema-evolution upgrade a stop-and-restore, not a reprocess.
- **Bad:** JVM memory (about 2 GB for a TaskManager on a laptop). Classloading has sharp edges: the JDBC driver had to move to `lib/` after a restart failed (see [schema-evolution.md](../schema-evolution.md)).
- **Revisit if:** detectors move to SQL-only logic, or the team runs Spark for everything else.

## Evidence

- Checkpoint duration: about 74 ms (lakehouse) and 216 ms (analytics) at live load (Prometheus `flink_jobmanager_job_lastCheckpointDuration`, 2026-10-22).
- Exactly-once across a savepoint upgrade and a TaskManager kill: 40,492 / 40,492 trade ids, 0 duplicates ([schema-evolution.md](../schema-evolution.md)).
- Unit tests prove dedup state survives restore from a checkpoint (`DedupTradesTest`).

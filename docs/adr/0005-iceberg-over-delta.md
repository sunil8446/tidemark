# ADR-0005: Use Apache Iceberg rather than Delta Lake

- **Status:** Accepted
- **Date:** 2026-10-22
- **Week:** 2

## Context

The lakehouse is written by Flink (streaming, exactly-once), read by Trino (interactive SQL and dbt), maintained by Airflow, and must evolve schemas and support time travel. Local storage is MinIO; cloud is S3. The catalog must work without Spark or Databricks.

## Decision

Apache Iceberg (format v2, Parquet + zstd) with a JDBC catalog in Postgres, shared by Flink and Trino. Tables use hidden partitioning on `day(event_ts)` and `venue`.

## Alternatives considered

| Option | Why not |
|---|---|
| Delta Lake | Strongest with Spark and Databricks. The Flink connector and Trino's write support exist but trail Spark's. Partitioning is Hive-style (explicit partition columns that queries must filter on directly). |
| Apache Hudi | Strong upsert/CDC story, but heavier operationally (table services, timeline). This workload is append-mostly. |
| Plain Parquet + Hive Metastore | No atomic commits: Flink's exactly-once would need a hand-rolled commit protocol. No schema evolution by field id, no time travel. |

## Consequences

- **Good:**
  - **Hidden partitioning:** queries filter on `event_ts` and Iceberg prunes day partitions; nobody writes or knows a `dt` column, and the partition spec can change later without rewriting queries.
  - **Schema evolution by field id:** adding `venue_sequence` was a metadata-only commit, and every old file stayed in place.
  - **Engine-neutral:** Flink writes, Trino reads, dbt builds marts, Airflow runs `optimize` / `expire_snapshots` / `remove_orphan_files` through Trino, all against one JDBC catalog.
  - **Time travel** for reproducible benchmarks (`FOR VERSION AS OF`).
- **Bad:** small files from frequent commits need scheduled compaction ([bench/compaction.md](../../bench/compaction.md)). The JDBC catalog is simple but not a REST catalog, so access control lives in Postgres and S3 IAM rather than the catalog.
- **Revisit if:** the platform standardises on Databricks, or needs catalog-level governance (then move to an Iceberg REST catalog such as Polaris).

## Evidence

- Schema-evolution drill: [docs/schema-evolution.md](../schema-evolution.md).
- Compaction: 98 → 2 data files on `raw_trades` ([bench/compaction.md](../../bench/compaction.md)).
- Reconciliation ledger in `iceberg.ops.reconciliation_runs`: 99.5992% → 100% after REST repair, 0 duplicates.

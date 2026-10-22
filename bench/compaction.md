# Compaction impact

Source: `iceberg.ops.maintenance_runs`, written by the Airflow DAG `tidemark_iceberg_maintenance` (Trino `optimize`, `expire_snapshots`, `remove_orphan_files`). Query latency is the median of 5 runs of a fixed query, after one warm-up run.

## Run 1: 2026-10-22 08:30 UTC (about 40 minutes of streaming at 30–60 s checkpoints)

| Table | Data files before → after | Median query before → after |
|---|---|---|
| `raw_trades` | **98 → 2** | 445.8 → 94.3 ms |
| `book_deltas` | **49 → 1** | 432.9 → 163.7 ms |
| `gap_events` (control: nothing to compact) | 2 → 2 | 296.1 → 243.5 ms |

**Caveat:** the control table got about 18% faster with no change at all, so part of every "after" number is warm caches and JIT. A second run straight afterwards (with nothing left to compact) showed the same pattern. At this size (0.5 MB of trades), the file-count reduction is the solid result; the latency gain is real but overstated.

## Planned: after 24 hours of accumulation

About 1,440 checkpoints × 2 partitions puts roughly 2,800 small files in `raw_trades`. That run will use a stricter method:
- interleave before/after queries against a snapshot taken before `optimize` (Iceberg time travel: `FOR VERSION AS OF`), so both sides see the same caches;
- 20 runs per side;
- report median and p95.

## Why it matters

Each Flink checkpoint commits at least one file per partition it touched. A 60 s checkpoint means 1,440 commits a day. Without compaction, planning and opening thousands of tiny Parquet files dominates query time, and table metadata grows without bound. `expire_snapshots` and `remove_orphan_files` keep metadata and storage bounded.

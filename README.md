# Tidemark

Real-time crypto market surveillance: live trade and order-book feeds from Coinbase and Kraken, provable data completeness under failure, manipulation-pattern detection, an Iceberg lakehouse, and an AI investigator measured against labeled ground truth.

> **Status:** Week 2 in progress (v0.1 tag pending its 24 h run): ingestion with gap repair, plus an exactly-once Flink → Iceberg lakehouse, ClickHouse hot aggregates, dbt marts and hourly reconciliation. Every number below links to the script or panel that produced it.

## What works today

- **Live ingestion** from Coinbase (`matches`, `heartbeat`) and Kraken v2 (`trade`, `book` depth 10, `instrument`) into Redpanda, as schema-registered Protobuf keyed by `venue:product`.
- **Provable completeness.** A per-product trade-id tracker opens a gap on any jump (or when a Coinbase heartbeat is ahead of the last trade seen), backfills it over REST, and reports `recovered`, `absent_at_source` or `unrecoverable` on `gaps.v1`. After a restart it resumes from the last trade id in Kafka.
- **Order-book integrity.** Every Kraken book update is checked against the venue's CRC32. On a mismatch the ingestor resubscribes for a fresh snapshot.
- **Fault injection** (`POST /debug/drop/<venue>?for=20s`, `/debug/reconnect/<venue>`) to prove recovery on demand.
- **Record and replay.** Raw frames go to hourly zstd NDJSON files and replay at N× speed through the same parsers.
- **Kafka audit** (`go run ./ingest/cmd/audit`): checks `trades.v1` itself for missing and duplicate ids.
- **Exactly-once lakehouse.** Flink dedups on (venue, product, trade_id) in checkpointed state and commits to Iceberg (`market.raw_trades`, `book_deltas`, `gap_events`) only when a checkpoint completes. Hidden partitioning on `day(event_ts)` and `venue`.
- **Hot aggregates.** 1 s and 1 m OHLCV bars, rolling 5 m VWAP and an as-of Coinbase–Kraken spread in ClickHouse `ReplacingMergeTree`. Late REST-backfilled trades re-fire their windows and upsert the corrected bar.
- **dbt on Trino.** Staging, intermediate and marts with enforced contracts; custom tests for monotonic trade ids and the OHLC invariant (28 / 28 passing).
- **Reconciliation** (`make reconcile`, hourly in Airflow): finds missing trade-id runs in Iceberg, re-fetches them over REST, and records every run in `iceberg.ops.reconciliation_runs`.
- **Iceberg maintenance** in Airflow: `optimize`, `expire_snapshots`, `remove_orphan_files`, measured before and after.

## Measured so far

| Metric | Value | Denominator / conditions | Source |
|---|---|---|---|
| Injected-gap recovery | 12 / 12 gaps, 334 / 334 trades | 20 s frame drops on both venues + 20 s process downtime | [bench/gaps.md](bench/gaps.md) |
| Kafka completeness after injection | 1,819 / 1,819 ids, 0 duplicates | 6 products, ~7 min window | [bench/gaps.md](bench/gaps.md) |
| Kraken book checksums | 3,336 / 3,336 passed | 40 s live dry run | [ADR-0002](docs/adr/0002-go-for-ingestion.md) |
| Replay → Kafka throughput | ~125k records/s acked | 1 broker, acks=all, M2 laptop | [bench/throughput.md](bench/throughput.md) |
| Lakehouse completeness after reconciliation | 99.5992% → **100%** (82 / 82 ids refetched), 0 duplicates | 3 h window, 20,460 ids | `iceberg.ops.reconciliation_runs` |
| Exactly-once across savepoint upgrade + TaskManager kill | 40,492 / 40,492 ids, 0 duplicates | 1 h window | [docs/schema-evolution.md](docs/schema-evolution.md) |
| Flink checkpoint duration | ~74 ms lakehouse, ~216 ms analytics | live load | Prometheus `flink_jobmanager_job_lastCheckpointDuration` |
| Compaction | 98 → 2 files on `raw_trades` | 40 min of 30–60 s commits | [bench/compaction.md](bench/compaction.md) |
| 24 h unattended gap report | *running* | started 2026-10-22 07:58 UTC | [bench/gaps.md](bench/gaps.md) |

## Architecture (target)

```
Coinbase WS ─┐                         ┌─> Iceberg (MinIO/S3) ─> Trino ─> dbt marts
             ├─> Go ingest ─> Redpanda ─┤
Kraken WS ───┘   (gap detect,           └─> Flink ─> detectors ─> alerts ─> AI investigator
                  REST backfill)                 └─> ClickHouse (hot aggregates)
```

## Run it locally

Prerequisites: Docker Desktop (8 GB memory or more for the full stack), Go, Java 17+, Maven, Python with `uv`.

```bash
make up          # lite: 1 Redpanda broker, MinIO, Postgres, Prometheus, Grafana
make topics      # create topics (RF follows broker count)
make smoke       # check every running service answers
make up-query    # add Trino + ClickHouse
make up-ha       # add brokers 2 and 3 for replication drills
make down        # stop, keep data

make ingest      # live ingestor (metrics on :9102, fault injection enabled)
make test        # unit tests, race detector on
make test-integration   # Kafka round trip + schema rules in a throwaway Redpanda
make replay FILES="$HOME/.tidemark-data/recordings/*.zst" SPEED=10
python3 bench/gap_report.py --window 24h   # writes bench/gaps.md

make up-orchestrate   # + Trino, ClickHouse, Flink, Airflow
make flink-build && make flink-submit                                  # lakehouse job
make flink-submit JOB=dev.tidemark.flink.AnalyticsJob                  # bars / VWAP / spread
make dbt-build        # marts + tests
make reconcile        # completeness check + REST repair
```

Grafana dashboard **Tidemark · Ingestion** is provisioned automatically.

| Service | URL | Notes |
|---|---|---|
| Redpanda Kafka | `localhost:19092` | brokers 2/3 on `29092`, `39092` with `ha` |
| Schema registry | http://localhost:18081 | |
| Redpanda Console | http://localhost:8080 | |
| MinIO | http://localhost:9001 | buckets `lake`, `checkpoints` |
| Postgres (Iceberg JDBC catalog) | `localhost:5433` | |
| Trino | http://localhost:8088 | `query` profile |
| ClickHouse | http://localhost:8123 | `query` profile |
| Flink | http://localhost:8081 | `stream` profile |
| Airflow | http://localhost:8090 | `orchestrate` profile; no login (local only) |
| Prometheus | http://localhost:9090 | |
| Grafana | http://localhost:3000 | anonymous viewer, admin password in `.env` |

Credentials in `.env.example` are for local development only.

### Where data lives

The repo sits in a synced OneDrive folder, so **no runtime data is written inside it**. Container state lives in named Docker volumes, and recorded feeds go to `~/.tidemark-data` (both ignored by git).

## Repo layout

| Path | Contents |
|---|---|
| `ingest/` | Go WebSocket clients, gap tracker, REST backfill, Kafka sink, resume, audit |
| `schemas/` | Protobuf definitions (`buf lint`, BACKWARD compatibility in the registry) |
| `replay/` | Recorder, Nx-speed replayer, anomaly injector |
| `flink/` | Java jobs: dedup, bars, detectors, Iceberg sink |
| `dbt/` | staging / intermediate / marts on Trino |
| `airflow/` | maintenance, reconciliation, dbt DAGs |
| `agent/`, `mcp-server/`, `evals/` | AI investigator, read-only tools, eval harness |
| `infra/` | Terraform, Helm, ArgoCD |
| `chaos/` | Chaos Mesh experiments |
| `bench/` | Benchmark scripts and results |
| `deploy/` | Config mounted into the local compose stack |
| `docs/` | [ADRs](docs/adr/), [feed notes](docs/feeds.md), SLOs, reports |

## Known limitations

- Public feeds carry no trader identity, so wash-trade detection is evaluated only on injected trades with synthetic account IDs.
- The local object store is a frozen MinIO build ([ADR-0001](docs/adr/0001-local-object-store.md)); cloud deployments use S3.

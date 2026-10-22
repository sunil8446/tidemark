# Tidemark

Real-time crypto market surveillance: live trade and order-book feeds from Coinbase and Kraken, provable data completeness under failure, manipulation-pattern detection, an Iceberg lakehouse, and an AI investigator measured against labeled ground truth.

> **Status:** Week 0, scaffolding. Nothing below is measured yet; numbers appear only once a script in [`bench/`](bench/) or a Grafana panel produces them.

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
```

| Service | URL | Notes |
|---|---|---|
| Redpanda Kafka | `localhost:19092` | brokers 2/3 on `29092`, `39092` with `ha` |
| Schema registry | http://localhost:18081 | |
| Redpanda Console | http://localhost:8080 | |
| MinIO | http://localhost:9001 | buckets `lake`, `checkpoints` |
| Postgres (Iceberg JDBC catalog) | `localhost:5433` | |
| Trino | http://localhost:8088 | `query` profile |
| ClickHouse | http://localhost:8123 | `query` profile |
| Prometheus | http://localhost:9090 | |
| Grafana | http://localhost:3000 | anonymous viewer, admin password in `.env` |

Credentials in `.env.example` are for local development only.

### Where data lives

The repo sits in a synced OneDrive folder, so **no runtime data is written inside it**. Container state lives in named Docker volumes, and recorded feeds go to `~/.tidemark-data` (both ignored by git).

## Repo layout

| Path | Contents |
|---|---|
| `ingest/` | Go WebSocket clients, gap detection, REST backfill |
| `schemas/` | Protobuf definitions |
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

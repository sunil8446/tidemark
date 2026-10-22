# ADR-0002: Use Go for ingestion

- **Status:** Accepted
- **Date:** 2026-10-22
- **Week:** 1

## Context

The ingestor holds long-lived WebSocket sessions to two venues, tracks per-product trade ids, maintains local order books, calls REST for backfill, produces to Kafka, and exposes metrics. It must reconnect forever without leaking, run unattended for days, and ship as a small container. Latency matters (it is the first hop of every SLO), but the volume is modest: a few hundred messages per second live, and much more during replay.

## Decision

Write the ingestor and replay tool in Go, using `coder/websocket`, `franz-go` (Kafka and schema registry), and `prometheus/client_golang`.

## Alternatives considered

| Option | Why not |
|---|---|
| Python (asyncio + websockets + confluent-kafka) | Fine for the volume, but the GIL and dynamic typing make the concurrent backfill/tracker code harder to get right under load. The replay benchmark would measure Python, not the pipeline. |
| Java/Kotlin | Shares a language with Flink, but a JVM per ingestor costs more memory on a laptop and a larger image. Slower edit/run loop. |
| Rust | Best raw performance, but slower to write and review inside a 4-week plan. The bottleneck is network and Kafka, not CPU. |

## Consequences

- **Good:** one goroutine per venue session plus a small backfill pool is simple to reason about. A static binary of about 15 MB. `go test -race` caught no races in the tracker and pipeline. franz-go gives an idempotent producer by default and handles the Confluent wire header.
- **Bad:** a second language next to Java (Flink) and Python (agent). Protobuf code is generated twice (Go and Java).
- **Revisit if:** a single ingestor can't keep up with replay at the target rate, or the team standardises on one JVM language.

## Evidence

- Live dry run: 3,336 Kraken book updates all passed the CRC32 checksum, with zero parse errors (2026-10-22).
- Replay of a 40 s recording at max speed: 3,816 records in 38 ms (dry run, no Kafka). Kafka-bound throughput goes in `bench/throughput.md`.

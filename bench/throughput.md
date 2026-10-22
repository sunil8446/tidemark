# Throughput

## Producer side: replay → Kafka (2026-10-22, Week 1)

What it measures: frames read from a recording, parsed by the live parsers, framed with the schema-registry header, produced, and **acknowledged** by Redpanda. Elapsed time includes the final flush. No consumer is attached, so this is the ingest ceiling, not end-to-end capacity.

| Setting | Value |
|---|---|
| Machine | Apple M2, 8 cores, 16 GB; Docker Desktop with 7.7 GB |
| Broker | 1 × Redpanda v25.2.4, `--smp 1 --memory 1G`, dev-container mode |
| Producer | franz-go, idempotent, `acks=all`, zstd, 5 ms linger |
| Topics | `*.v1.bench`, 6 partitions, RF=1 |
| Input | 40 s live recording (3,810 frames: 480 trades, 3,336 Kraken book updates), looped |
| Mode | `--raw` (gap tracking off so looped trade ids are not dropped as duplicates) |

| Loops | Records acked | Elapsed | Records/s |
|---:|---:|---:|---:|
| 20 | 76,320 | 0.615 s | 124,055 |
| 100 | 381,600 | 3.041 s | 125,473 |
| 1 (gap tracking on) | 3,816 | 0.039 s | 98,897 (too short to be meaningful) |

Live load during the same session was about 100 records/s (trades plus book updates for 6 products), so the single-broker producer path has roughly three orders of magnitude of headroom.

Reproduce:

```bash
make build
for t in trades book gaps; do docker exec redpanda-0 rpk topic create $t.v1.bench -p 6 -r 1; done
./bin/replay --metrics-addr "" --speed 0 --raw --loop 100 --topic-suffix .bench ~/.tidemark-data/recordings/*.zst
```

## End to end (consumer lag): Week 2

"Max replay throughput before consumer lag grows" needs the Flink job as the consumer. It will be added here once that exists.

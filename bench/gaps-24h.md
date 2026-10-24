# Gap report: the 24h ending 2026-10-24T01:56:00Z

Generated 2026-10-24 04:17 UTC by `bench/gap_report.py --window 24h --end 2026-10-24T01:56:00Z`.

- Ingestor scrape availability over the window: **99.92%** (Prometheus runs in Docker: an outage of Docker itself leaves no samples and does not lower this figure)
- Kafka produce errors: **144622**; trades lost on produce (counted from 2026-10-24): **0**. The gap tracker counts a trade as seen when it arrives, so trades lost on produce are not gaps in the tables below; the Kafka audit at the end is the ground truth.

## Gaps (ingestor view, Prometheus)

| venue | kind | detected | recovered | absent at source | unrecoverable |
|---|---|---:|---:|---:|---:|
| coinbase | checksum_mismatch | 0 | 0 | 0 | 0 |
| coinbase | heartbeat_ahead | 0 | 0 | 0 | 0 |
| coinbase | trade_id_jump | 38 | 38 | 0 | 0 |
| kraken | checksum_mismatch | 0 | 0 | 0 | 0 |
| kraken | heartbeat_ahead | 0 | 0 | 0 | 0 |
| kraken | trade_id_jump | 6 | 6 | 0 | 0 |
| **all** | | **44** | **44** | **0** | **0** |

Gaps still open at report time are counted as detected but not yet resolved.

## Trades

| venue | via WebSocket | via REST backfill | ids missing in gaps | ids filled | p50 / p99 recovery |
|---|---:|---:|---:|---:|---|
| coinbase | 554400 | 94757 | 94757 | 94757 | 4.71 s / 600.00 s |
| kraken | 183017 | 32382 | 32382 | 32382 | 20.00 s / 600.00 s |

## Exchange → Kafka latency (live WebSocket trades)

| venue | p50 | p99 |
|---|---|---|
| coinbase | 45 ms | 8.87 s |
| kraken | 84 ms | 6.22 s |

Measured from the exchange's event timestamp to the broker ack, so it includes network time from the venue and any clock skew between the venue and this host. Histogram buckets bound the precision.

## Reconnects

| venue | reason | count |
|---|---|---:|
| coinbase | dial | 8 |
| coinbase | read_error | 4 |
| coinbase | silence | 13 |
| kraken | dial | 2 |
| kraken | read_error | 11 |
| kraken | silence | 8 |

## Kafka audit (independent check of `trades.v1`)

Audit of `trades.v1` at 2026-10-24T01:56:00Z (records scanned: 1696614; window: 24h0m0s; settle: 30s)

| venue:product | first event (UTC) | last event (UTC) | expected ids | present | missing | duplicates | via REST | completeness |
|---|---|---|---:|---:|---:|---:|---:|---:|
| coinbase:BTC-USD | 10-23 01:56:00 | 10-24 01:55:59 | 430168 | 430168 | 0 | 0 | 66900 | 100.0000% |
| coinbase:ETH-USD | 10-23 01:56:01 | 10-24 01:55:58 | 149412 | 149412 | 0 | 0 | 31169 | 100.0000% |
| coinbase:SOL-USD | 10-23 01:56:00 | 10-24 01:55:57 | 69555 | 69555 | 0 | 0 | 12578 | 100.0000% |
| kraken:BTC-USD | 10-23 01:56:00 | 10-24 01:55:57 | 134944 | 134944 | 0 | 2 | 22385 | 100.0000% |
| kraken:ETH-USD | 10-23 01:56:01 | 10-24 01:55:56 | 50932 | 50932 | 0 | 0 | 8574 | 100.0000% |
| kraken:SOL-USD | 10-23 01:56:00 | 10-24 01:55:58 | 29511 | 29511 | 0 | 0 | 5475 | 100.0000% |
| **total** | | | 864522 | 864522 | 0 | 2 | | **100.0000%** |

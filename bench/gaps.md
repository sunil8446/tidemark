# Gap report

The Week 1 definition of done is a 24-hour unattended run. The first attempt (from 2026-10-22 07:58 UTC) was stopped after about 7 hours because the laptop was overheating under the full stack plus evaluation runs. The run was restarted at **2026-10-23 01:56 UTC** with CPU caps on the heaviest containers.

## 24-hour run: 2026-10-23 01:56 → 2026-10-24 01:56 UTC

**Result: 864,522 of 864,522 trade ids in Kafka (100.0000%), 0 missing; 2 duplicate records (removed by Flink's dedup before the lake).** Full generated report: [gaps-24h.md](gaps-24h.md), from

```bash
python3 bench/gap_report.py --window 24h --end 2026-10-24T01:56:00Z --out bench/gaps-24h.md
```

It was **not fully unattended**, and the two incidents are the most useful thing the run produced:

| When (UTC) | What happened | Recovery |
|---|---|---|
| 10-23 21:27 → 10-24 00:53 (3 h 26 m) | The host disk filled (2.9 GB free on 228 GB). The Docker VM got write I/O errors and froze: Redpanda, Flink, Trino, MinIO and Prometheus all stopped. 13 GB of the squeeze was stale Flink TaskManager working directories, one per crash or forced restart that day. | Docker restarted by hand; Flink jobs restored from retained checkpoints. The ingestor (on the host) kept running: once Kafka was back it detected 12 trade-id gaps and refilled **123,253 trades over REST in under 40 s per product**, none unrecoverable. |
| 10-23 21:33 → 22:23 | For the first 50 minutes of that outage the ingestor still accepted trades but every produce failed (144,622 produce errors). The gap tracker had counted those ids as *seen*, so it reported every gap recovered while **20,235 trades were not in Kafka (97.66%)**. The hourly reconcile job missed them too: its fixed 2 h look-back had moved past the holes by the time Airflow came back. | Found by this report's independent Kafka audit (the reason it exists). `reconcile --window 8h` refetched all **20,237** over REST: 100%. |

**Fixes**, so the same failure repairs itself next time:

- `tidemark_ingest_trades_lost_total{venue,product}` counts trades whose produce failed, with a paging alert (`IngestTradesLost`). Repair stays with reconcile, which checks what Kafka actually holds; re-opening gaps inside the ingestor would loop while the broker is down.
- `reconcile --catch-up 48h` (on by default) starts at the end of the last recorded run when that is older than its window, so scheduler downtime of up to two days is covered.
- The TaskManager clears stale `/tmp/tm_*` working directories on start.
- Known blind spot, stated in the report: Prometheus runs in Docker, so a Docker outage leaves no samples and does not lower "scrape availability" (99.92%).

## Fault-injection check (2026-10-22, before the 24 h run)

Frames were discarded for 20 s on each venue via `POST /debug/drop/<venue>?for=20s`, then the ingestor was restarted after 20 s of downtime.

| Scenario | Gaps detected | Recovered | Trades missing → filled | Time to recover |
|---|---:|---:|---:|---|
| 20 s frame drop, both venues | 6 trade-id + 3 Kraken book checksum | 9 / 9 | 150 → 150 | 3.1–3.9 s (3 s is the backfill grace period) |
| 20 s process downtime, resume from Kafka | 3 (Coinbase) | 3 / 3 | 184 → 184 | 3.3–3.9 s |
| Forced Kraken reconnect | 0 (snapshot covered it) | — | — | — |

An independent audit of `trades.v1` after the drop test showed 1,819 of 1,819 trade ids present with 0 duplicates.

The first audit also exposed a real bug: before resume-from-Kafka existed, every restart silently lost the trades that happened while the ingestor was down, and Kraken's subscribe snapshot was re-produced as duplicates. Those early records (before 07:58 UTC) stay in the topic as evidence and are outside the 24 h window.

## Restart after a 6.7-hour shutdown (2026-10-23 01:56 UTC)

The whole stack was stopped for about 6.7 hours (Flink jobs stopped with savepoints, ingestor stopped). On restart the ingestor resumed from the last trade ids in Kafka and treated the outage as gaps:

| Venue | Product | Missing trade ids | Recovered over REST | Time to recover |
|---|---|---:|---:|---|
| coinbase | BTC-USD | 155,214 | 155,214 | 52.2 s |
| coinbase | ETH-USD | 51,850 | 51,850 | 23.7 s |
| coinbase | SOL-USD | 30,039 | 30,039 | 36.5 s |
| kraken | BTC-USD | 46,472 | 46,472 | 84.5 s |
| kraken | ETH-USD | 17,275 | 17,275 | 40.5 s |
| kraken | SOL-USD | 9,945 | 9,945 | 62.4 s |
| **total** | | **310,795** | **310,795** | |

Book data for the outage cannot be recovered (Kraken has no historical order-book REST endpoint); the lakehouse records that window as having no book updates.

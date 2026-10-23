# Gap report

The Week 1 definition of done is a 24-hour unattended run. The first attempt (from 2026-10-22 07:58 UTC) was stopped after about 7 hours because the laptop was overheating under the full stack plus evaluation runs. The run was restarted at **2026-10-23 01:56 UTC** with CPU caps on the heaviest containers; this file is regenerated from it with:

```bash
python3 bench/gap_report.py --window 24h
```

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

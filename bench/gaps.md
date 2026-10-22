# Gap report

The Week 1 definition of done is a 24-hour unattended run. That run started at **2026-10-22 07:58 UTC**; this file is regenerated from it with:

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

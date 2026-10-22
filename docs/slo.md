# Service level objectives

Each SLO has an SLI with a precise good/total definition, a target over a rolling 30 days, the error budget that implies, and how it is alerted. Rules live in [`deploy/prometheus/rules/slo.yml`](../deploy/prometheus/rules/slo.yml) and are validated with `promtool`.

| SLO | SLI (good / total) | Target | Budget (30 d) | Source |
|---|---|---|---|---|
| **Ingest freshness** | Live WebSocket trades acked by Kafka ≤ 1 s after their exchange timestamp / all live trades | 99.5% | 0.5% ≈ 3.6 h of all-bad | `tidemark_ingest_exchange_to_kafka_seconds` histogram |
| **Alert latency** | Alerts emitted ≤ 30 s after the event time that triggered them / all live alerts | 99% | 1% | `tidemark_alerts_within_slo_total` / `tidemark_alerts_total` (Flink, per detector) |
| **Lakehouse freshness** | Minutes in which the lakehouse job committed a checkpoint (= an Iceberg commit) in the trailing 3 min / all minutes | 99.5% | 0.5% ≈ 3.6 h | `flink_jobmanager_job_numberOfCompletedCheckpoints` |
| **Completeness** | Trade ids present in Iceberg / trade ids the venue says exist, after hourly reconciliation | 99.99% | 0.01% | `iceberg.ops.reconciliation_runs` |

## Why these targets

- **Ingest freshness 1 s / 99.5%.** Measured p99 exchange-to-ack is about 140 ms for Coinbase at normal load ([bench/gaps.md](../bench/gaps.md)). 1 s leaves room for venue-side bursts and clock skew while still catching a broker or network problem within minutes. The SLI includes venue → host network time and clock skew, so it cannot be driven to 100%.
- **Alert latency 30 s / 99%.** Detectors work in event time behind a 5 s out-of-orderness watermark. A volume-spike episode is scored when its second closes, and a spoof pull is only confirmed after the 1 s repricing window. A surveillance analyst acting within a minute is the use case; 30 s keeps the investigator agent's evidence window fresh.
- **Lakehouse freshness 3 min / 99.5%.** Iceberg is committed on every successful checkpoint (60 s). Three missed checkpoints in a row means queries and dbt marts are going stale.
- **Completeness 99.99%.** Trade-id gaps are detected live and repaired over REST within seconds, and the hourly reconciliation re-checks the lakehouse itself. The remaining budget covers ids a venue's REST API can no longer serve.

## Burn-rate alerting

Multi-window, multi-burn-rate (Google SRE Workbook, ch. 5). With budget *B* over 30 days:

| Severity | Long window | Short window | Burn rate | Budget spent when it fires |
|---|---|---|---|---|
| page | 1 h | 5 min | 14.4 × *B* | 2% in 1 h |
| page | 6 h | 30 min | 6 × *B* | 5% in 6 h |
| ticket | 3 d | 6 h | 1 × *B* | 10% in 3 d |

Both windows must exceed the rate. The long window gives significance; the short window makes the alert stop soon after recovery instead of firing for an hour after the problem is fixed.

**Worked example (ingest freshness, B = 0.5%).** A fast-burn page needs more than 7.2% of trades late (14.4 × 0.5%) over the last hour *and* the last 5 minutes. If that continued, the whole monthly budget would be gone in about 50 hours (30 d / 14.4).

Two direct pages complement the burn rates: `IngestGapUnrecoverable` (a gap that REST could not fill) and `IngestDisconnected` (a venue WebSocket down for 2 minutes).

## Measured so far

The rules are loaded and evaluating (Week 3). Attainment over a full 30-day window does not exist yet; the chaos experiments in [chaos-report.md](chaos-report.md) record how much budget each failure consumed.

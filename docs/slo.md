# Service level objectives

*Filled in during Week 3.* Every target needs a measurement source (Prometheus query or reconciliation job) and an error budget.

| SLI | Target | Window | Source | Error budget |
|---|---|---|---|---|
| Ingest freshness (exchange ts → Kafka produce) | TBD | 28d | ingest latency histogram | TBD |
| End-to-end alert latency | TBD | 28d | OTel traces | TBD |
| Hourly completeness | TBD | 28d | reconciliation job | TBD |
| Lakehouse freshness | TBD | 28d | Iceberg snapshot age | TBD |

## Burn-rate alerts

TBD (multiwindow, multi-burn-rate).

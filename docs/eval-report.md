# Evaluation report

## Detectors

**Method** ([ADR-0006](adr/0006-detector-evaluation.md)): labelled synthetic patterns are injected into recorded live data (07:48–10:00 UTC, 2026-10-22: a window with no venue outages), and an evaluation detector instance scores every candidate. Thresholds are chosen on injection **seed 42** (train) as the best F1 within an alert budget of **6 unlabelled alerts per hour** per detector, then **frozen** and reported on injection **seed 7** (test). 20 injections per pattern per seed. Reproduce with `bench/detector_eval.sh`.

"Unlabelled" alerts fired on real market data with no injection nearby. Some are genuine events (a real burst *is* a volume spike), so precision against labels is a lower bound.

### Held-out test (seed 7, thresholds frozen from training)

| Detector | Injected | Threshold | TP alerts | Unlabelled alerts | Unlabelled / h | Precision (vs labels) | Labels detected | Recall | F1 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| volume_spike | 20 | 79.7 | 6 | 11 | 5.0 | 0.353 | 6 / 20 | 0.300 | 0.324 |
| spoof_layering | 20 | 30.8 | 2 | 10 | 4.6 | 0.167 | 2 / 20 | 0.100 | 0.125 |
| dislocation | 20 | 5.8 | 28 | 0 | 0.0 | 1.000 | 19 / 20 | 0.950 | 0.974 |
| wash_trade | 20 | 0.977 | 18 | 0 | 0.0 | 1.000 | 18 / 20 | 0.900 | 0.947 |

### Training (seed 42, thresholds chosen here)

| Detector | Injected | Threshold | TP alerts | Unlabelled alerts | Unlabelled / h | Precision (vs labels) | Labels detected | Recall | F1 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| volume_spike | 20 | 79.7 | 7 | 10 | 4.6 | 0.412 | 6 / 20 | 0.300 | 0.347 |
| spoof_layering | 20 | 30.8 | 0 | 13 | 6.0 | 0.000 | 0 / 20 | 0.000 | 0.000 |
| dislocation | 20 | 5.8 | 39 | 0 | 0.0 | 1.000 | 20 / 20 | 1.000 | 1.000 |
| wash_trade | 20 | 0.977 | 20 | 0 | 0.0 | 1.000 | 20 / 20 | 1.000 | 1.000 |

### What the numbers say

- **Dislocation and wash trading are production-ready on this data:** precision 1.00 with recall 0.95 and 0.90 on the held-out seed, and zero unlabelled alerts. Dislocation needs both venues trading: in an earlier window that included a 30-minute Coinbase outage, half the injections were undetectable, which is correct behaviour (stale prices are deliberately ignored); that is why this evaluation uses an outage-free window.
- **Volume spike is a context signal, not a standalone manipulation detector.** Real crypto markets produce large flat-price bursts several times an hour (passive absorption looks the same as volume inflation without trader identity), so staying within 6 alerts/h leaves recall at 0.30.
- **Spoof-like layering is pervasive in Kraken's public book.** The detector finds every injected spoof down to a score of 0.25× visible depth (recall 1.00, training sweep below), but real books show about 1,400 add-and-pull events per hour at 0.1× and about 127 per hour even at 0.8×. Within 6 alerts/h the threshold rises to about 31× depth, above every injection. Public book data cannot say whether those events are manipulation or aggressive market making; the platform's answer is the two-stage design: detectors raise candidates at a triage threshold, and the investigator agent decides what to escalate.

### Spoof-like layering: training sweep

| Threshold | TP | Unlabelled | Unlabelled / h | Precision | Recall | F1 |
|---:|---:|---:|---:|---:|---:|---:|
| 0.1 | 85 | 3126 | 1432.4 | 0.026 | 1.000 | 0.052 |
| 0.113 | 77 | 2843 | 1302.8 | 0.026 | 1.000 | 0.051 |
| 0.127 | 74 | 2554 | 1170.3 | 0.028 | 1.000 | 0.055 |
| 0.143 | 70 | 2267 | 1038.8 | 0.030 | 1.000 | 0.058 |
| 0.166 | 62 | 1982 | 908.2 | 0.030 | 1.000 | 0.059 |
| 0.196 | 57 | 1695 | 776.7 | 0.033 | 1.000 | 0.063 |
| 0.234 | 51 | 1410 | 646.1 | 0.035 | 1.000 | 0.067 |
| 0.287 | 43 | 1126 | 516.0 | 0.037 | 1.000 | 0.071 |
| 0.385 | 37 | 840 | 384.9 | 0.042 | 0.950 | 0.081 |
| 0.521 | 23 | 563 | 258.0 | 0.039 | 0.850 | 0.075 |
| 0.807 | 17 | 277 | 126.9 | 0.058 | 0.650 | 0.106 |
| 30.8 ◀ | 0 | 13 | 6.0 | 0.000 | 0.000 | 0.000 |
| 361 | 0 | 1 | 0.5 | 0.000 | 0.000 | 0.000 |

Recall by injection strength at the chosen threshold:

| Strength | Injected | Detected |
|---|---:|---:|
| [0×, 0.5×) | 5 | 0 |
| [0.5×, 1×) | 3 | 0 |
| [1×, 2×) | 1 | 0 |
| [2×, 4×) | 6 | 0 |
| [4×, ∞) | 5 | 0 |

### Volume spike: training sweep

| Threshold | TP | Unlabelled | Unlabelled / h | Precision | Recall | F1 |
|---:|---:|---:|---:|---:|---:|---:|
| 2 | 28 | 376 | 172.3 | 0.069 | 0.950 | 0.129 |
| 2.31 | 27 | 341 | 156.3 | 0.073 | 0.950 | 0.136 |
| 2.71 | 26 | 305 | 139.8 | 0.079 | 0.950 | 0.145 |
| 3.46 | 26 | 269 | 123.3 | 0.088 | 0.950 | 0.161 |
| 4.19 | 26 | 232 | 106.3 | 0.101 | 0.950 | 0.182 |
| 5.17 | 25 | 196 | 89.8 | 0.113 | 0.950 | 0.202 |
| 6.16 | 23 | 162 | 74.2 | 0.124 | 0.900 | 0.218 |
| 8.38 | 23 | 125 | 57.3 | 0.155 | 0.900 | 0.265 |
| 12 | 20 | 91 | 41.7 | 0.180 | 0.850 | 0.297 |
| 22 | 17 | 58 | 26.6 | 0.227 | 0.750 | 0.348 |
| 36.2 | 14 | 24 | 11.0 | 0.368 | 0.650 | 0.470 |
| 79.7 ◀ | 7 | 10 | 4.6 | 0.412 | 0.300 | 0.347 |
| 283 | 1 | 0 | 0.0 | 1.000 | 0.050 | 0.095 |

Recall by injection strength at the chosen threshold:

| Strength | Injected | Detected |
|---|---:|---:|
| [0×, 5×) | 1 | 0 |
| [5×, 10×) | 2 | 0 |
| [10×, 20×) | 4 | 1 |
| [20×, ∞) | 13 | 5 |

### Bugs this evaluation found and fixed

See ADR-0006. In short: phantom Kraken book levels (no deletes outside the subscribed depth), replay venues drifting apart at max speed, injected orders erased by absolute-quantity updates, pulls missed when other orders joined the level, and the watermark race that silenced the spoof detector in production.

## Investigator agent

*Pending: needs a Claude API key and your approval to spend (about $0.05–0.15 per investigation).* The harness is ready: `evals/run_eval.py` over `evals/eval_set.jsonl` (118 alerts from the held-out run: 77 injected with ground truth, 41 real alerts awaiting a human label in the review page). Metrics: triage accuracy, groundedness, claim support, LLM-judge pass rate with agreement against human grades, cost and latency per investigation.

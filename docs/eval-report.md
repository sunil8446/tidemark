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

**Re-measured 2026-10-24** with the realistic injector (synthetic trades renumbered into the venue's id sequence, real-like sizes, random timing and sides, the tape's `source`): identical held-out results for all four detectors (volume spike 6/20, spoof 2/20, dislocation 19/20, wash 18/20). The detectors never used ids, sizes or timing, so the artefacts only mattered to the investigator agent. The re-run also caught a regression from a spread-operator change (dislocation briefly fell to 13/20; fixed in `f2696f5`, see the agent section below).

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

**Run:** the 30-alert CI subset (`evals/ci_subset.jsonl`) from the held-out detector run (seed 7): 24 injected alerts with ground truth (18 spoof-like layering, 6 volume spike) and 6 real volume-spike alerts with no label yet. Model `claude-opus-5-5` at effort `medium`, through the Claude Code CLI backend (`--backend cli`, a Claude subscription rather than an API key; [agent/README](../agent/README.md)). The same MCP tools, prompts and `Verdict` schema as the API backend. Judge: the same model at effort `low`, blind to the label. 2026-10-23. Reproduce: `uv run python evals/run_eval.py --set evals/ci_subset.jsonl --backend cli --record --fixtures evals/fixtures`; CI replays the recorded tool results.

### Results

| Slice | Cases | Triage correct | Judge pass | Escalated |
|---|---:|---:|---:|---:|
| Injected volume spike | 6 | 6 (1.00) | 5 | 6 |
| Injected spoof-like layering | 18 | 11 (0.61) | 14 | 11 |
| **All labelled (recall)** | **24** | **17 (0.71)** | 19 | 17 |
| Real volume spike (unlabelled) | 6 | needs a human label | 5 | 0 |

| Metric | Value |
|---|---:|
| Groundedness (narrative numbers traceable to a tool result) | 0.996 mean; 26 / 30 cases fully grounded |
| Claim support (each cited claim found in its cited tool call) | 1.00 |
| Judge pass rate (all 30) | 0.80 |
| Tool calls per case | 5.0 mean |
| Latency per case | 68 s p50, 87 s p95 |
| Tokens per case | about 5,200 output; API-equivalent cost $0.31 (not billed: subscription) |
| **CI gate** (accuracy ≥ 0.80, groundedness ≥ 0.95) | **fails on accuracy** (0.71) |

### What the numbers say

- **Every labelled case is an injection, so "triage correct" is recall of escalation.** It says nothing yet about false escalations: those need the hand-labelled real alerts.
- **Volume-spike accuracy (6 / 6) is not evidence of skill yet.** Five of the six case files cite the injector's artefacts: synthetic trades carry ids from 2^62 upward (outside the venue's sequence), identical sizes and a fixed 0.5 s clock. A real wash-style burst would not label itself this way. Until the injector draws ids, sizes and timing from the real distribution, this slice measures artefact spotting. The six *real* bursts were all called `likely_benign` (5 of 6 judged sound), consistent with the detector evaluation finding that most large real bursts are organic; they need a human label before they count.
- **Spoof-like layering is the honest number: 11 / 18 (0.61).** Spoof injections touch only the book, so there is no id tell. The misses are the same ambiguity the detector evaluation found: a large order added and pulled with nothing filled is also what an aggressive market maker does, and in 4 of 7 misses the injected order sat at (or drifted to) the touch, where re-quoting is the natural reading.
- **Confidence is calibrated, which makes the misses cheap.** Wrong verdicts were `low` (5) or `medium` (2), never `high`; 10 of the 17 correct ones were `high`. Routing every `low`-confidence `likely_benign` to a human, instead of auto-closing it, would have caught 5 of the 7 misses, and it would have added 1 review among the 6 real alerts (the only low-confidence benign call there).
- **Grounding holds.** Every cited claim was found in the tool call it cites, and only 4 numbers across 30 narratives were not traceable.
- **The judge grades reasoning, not correctness.** It agreed with triage correctness on 16 / 24 cases: it passes well-argued wrong calls and fails right calls that state one unsupported detail. Agreement with human grades still needs the hand-labelled set.

### Bugs this evaluation found

| Finding | Fix |
|---|---|
| **Book checkpoints were future-dated.** `BookCheckpoints` applied book messages immediately but cut checkpoints on event-time timers; with a lagging watermark the 08:02:30 checkpoint held the 08:02:40 book, so every book rebuilt from it was crossed (bid 2424.53 over ask 2395.62). The same watermark race as the spoof detector's, in another operator | Checkpoints are cut in book time (before the first message past a tick); `LakehouseJob --only book_snapshots` rebuilt the table from Kafka. The MCP server now flags any crossed rebuild as unreliable instead of presenting it |
| MCP tools failed under concurrent calls (one ClickHouse session per process) | One Trino and ClickHouse connection per worker thread |
| The judge was not shown the detector history the investigator gets in its prompt, and saw tool results cut to 6,000 characters (the agent sees 24,000), so it marked supported claims as fabricated (pass rate 0.37) | The judge sees exactly what the investigator saw; all 30 cases re-graded from the recorded tool results (`--regrade`), pass rate 0.80 |
| Grounding split clock times (`08:47:34.314917`) into loose numbers and could not match ISO timestamps | Times are checked against times in tool inputs and outputs |
| On the CLI backend the model could see `get_alert` but not call it, and reported the refusal as a failure in its case file | Non-investigation tools are hidden with `--disallowedTools` |
| A Docker memory squeeze (7.65 GB VM) killed Trino mid-run | The runner treats tool-backend errors as unscored and retries the case; Airflow is paused during evals |

### Next

1. ~~Make the injector realistic~~ Done in code (2026-10-23): every trade on the injected stream is renumbered into one continuous per-instrument sequence, synthetic sizes are drawn from the instrument's recent real sizes, times are random within the window, taker sides are random, and synthetic trades copy the tape's `source` (they were tagged `replay`, which `query_trades` exposes). The flat price stays: it is the pattern being injected. **Not yet re-measured:** the volume-spike slice needs a fresh injected run and re-investigation.
2. Hand-label the real alerts in the review page; report judge-human agreement.
3. Run the full 118-alert set; try routing low-confidence benign verdicts to review and report the queue size it costs.

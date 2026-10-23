# ADR-0006: Evaluate detectors against labelled injections, choose thresholds by alert budget

- **Status:** Accepted
- **Date:** 2026-10-22
- **Week:** 3

## Context

Public crypto feeds carry no trader or order identity, so there is no ground truth for manipulation in live data. Precision and recall still have to be measured, and thresholds chosen, on evidence rather than intuition. Live markets also produce genuine events that look like the patterns (a real volume burst *is* a volume spike), so "unlabelled" does not mean "false".

## Decision

1. **Ground truth by injection.** The replay harness overlays labelled synthetic patterns on recorded live data (volume inflation, spoof-like layering behind the touch, cross-venue dislocation, wash trades between synthetic accounts) into separate `*.v1.inj` topics. One `InjectionLabel` per pattern records its window and parameters.
2. **Score everything, sweep offline.** An evaluation detector instance runs with deliberately low score thresholds, and every other parameter at its production value. An alert's score answers "would this fire at threshold θ?". Escalating episodes re-emit the same alert id with the higher score, so the stored score is the episode peak. Precision/recall at any θ is then a sweep, not a re-run.
3. **Operating point = alert budget.** The threshold is the best F1 among thresholds that keep **unlabelled** alerts within a budget (default 6 per hour per detector across six instruments). That is how surveillance desks size queues; precision against labels alone would punish detectors for finding real events.
4. **Train/test split.** Thresholds are chosen on one injection seed and reported on a different seed with the thresholds frozen (`evaluate --fixed`).

## What the evaluation changed (each found by a failing run, fixed, and re-measured)

| Finding | Fix |
|---|---|
| Fixed 10 s volume buckets: half of all bursts straddled two buckets and scored half as high | Rolling 10 s sum evaluated every second |
| Real volume bursts dominated the alert budget | Score discounted by price impact (`z / (1 + range_bps / 5)`): sweeps move price, inflation prints flat. Caveat: the injection models exactly that pattern, so recall here is an upper bound |
| Spoof candidates scored against dust levels (0.002 BTC median) | Score = share of the *other visible depth* on that side |
| Market makers amend orders (cancel + re-add one tick away within ms) | Minimum resting time (250 ms) and a 1 s repricing filter |
| Trades at the spoofed price counted as fills of the spoof order | Price-time priority: only trades beyond the queue ahead can fill it |
| **Kraken never deletes levels that drift out of the subscribed depth**; untruncated books showed phantom best prices | Truncate to depth after every message (detector, injector, book checkpoints, MCP rebuild) |
| Dislocation false positives from one venue's stale last trade | Only score samples where both venues traded in the last 5 s |
| Injected spoofs were erased by the next real update at that level (Kraken sends absolute quantities) | Injector overlays the synthetic order on every real update until it is pulled |
| Per-venue replay goroutines drifted hours apart at max speed, so cross-venue logic and the injector's clock saw the wrong world | Replay merges all recordings into one stream in global time order |
| A pull whose level gained other orders never "fell back", so it was missed | A pull is a single update removing ≥ 80% of the added size, tracked even while the level drifts out of depth |
| **Spoof confirmations ran on the operator watermark** (min of book and trade inputs). During replay or catch-up the book side raced ahead, and a later resubscribe snapshot wiped every pending candidate before its timer fired: 0 confirmed alerts in production, while unit tests passed | Candidates settle on **book time** (each book update confirms or expires what is due by its own timestamp); timers remain a backstop. Found with per-stage counters, reproduced by running the real job in a local MiniCluster |

## Consequences

- **Good:** every threshold in production traces back to a measured operating point; the evaluation is reproducible (`bench/detector_eval.sh <seed> <n>`) and caught two production bugs (phantom book levels, a naive-datetime shift in the MCP tools).
- **Bad:** injected patterns are idealised; recall on real manipulation would be lower, especially for patterns that do not match the injector's model. Volume spike remains weak as a standalone manipulation signal; it is most useful as context for the investigator.
- **Limitation, stated plainly:** without identity, wash trading can only be evaluated on synthetic accounts, and spoofing can only be flagged as a book pattern, not attributed to a party.

## Evidence

[docs/eval-report.md](../eval-report.md) (train seed 42, test seed 7).

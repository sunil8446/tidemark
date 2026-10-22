#!/usr/bin/env python3
"""Write bench/gaps.md: gap detection and recovery over a window.

Two independent sources:
  1. Prometheus: what the ingestor saw (gaps detected/resolved, latency).
  2. `audit`: what actually landed in Kafka (missing ids, duplicates).

Usage: bench/gap_report.py --window 24h [--prom http://localhost:9090] [--out bench/gaps.md]
Standard library only.
"""
import argparse
import datetime as dt
import json
import subprocess
import sys
import urllib.parse
import urllib.request


def prom(base, expr):
    url = f"{base}/api/v1/query?" + urllib.parse.urlencode({"query": expr})
    with urllib.request.urlopen(url, timeout=30) as r:
        body = json.load(r)
    if body.get("status") != "success":
        raise RuntimeError(body)
    return body["data"]["result"]


def scalar(base, expr):
    res = prom(base, expr)
    return float(res[0]["value"][1]) if res else 0.0


def by(base, expr, *labels):
    return {tuple(r["metric"].get(l, "") for l in labels): float(r["value"][1]) for r in prom(base, expr)}


def fmt_s(v):
    if v != v:  # NaN: no observations
        return "n/a"
    return f"{v * 1000:.0f} ms" if v < 1 else f"{v:.2f} s"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--window", default="24h")
    ap.add_argument("--prom", default="http://localhost:9090")
    ap.add_argument("--out", default="bench/gaps.md")
    ap.add_argument("--brokers", default="localhost:19092")
    a = ap.parse_args()
    w, P = a.window, a.prom

    detected = by(P, f"round(sum by (venue, kind) (increase(tidemark_ingest_gaps_detected_total[{w}])))", "venue", "kind")
    resolved = by(P, f"round(sum by (venue, kind, status) (increase(tidemark_ingest_gaps_resolved_total[{w}])))", "venue", "kind", "status")
    missing = by(P, f"round(sum by (venue) (increase(tidemark_ingest_gap_trades_missing_total[{w}])))", "venue")
    filled = by(P, f"round(sum by (venue) (increase(tidemark_ingest_gap_trades_filled_total[{w}])))", "venue")
    trades = by(P, f"round(sum by (venue, source) (increase(tidemark_ingest_trades_total[{w}])))", "venue", "source")
    reconnects = by(P, f"round(sum by (venue, reason) (increase(tidemark_ingest_reconnects_total[{w}])))", "venue", "reason")
    lat = {q: by(P, f"histogram_quantile({q}, sum by (le, venue) (increase(tidemark_ingest_exchange_to_kafka_seconds_bucket[{w}])))", "venue")
           for q in ("0.5", "0.99")}
    rec = {q: by(P, f"histogram_quantile({q}, sum by (le, venue) (increase(tidemark_ingest_gap_recovery_seconds_bucket[{w}])))", "venue")
           for q in ("0.5", "0.99")}
    up = scalar(P, f"avg_over_time(up{{job='ingest'}}[{w}])")
    produce_err = scalar(P, f"round(sum(increase(tidemark_ingest_produce_errors_total[{w}])))")

    venues = sorted({k[0] for k in list(detected) + list(trades)})
    now = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    L = [f"# Gap report: last {w}", "", f"Generated {now} by `bench/gap_report.py --window {w}`.", ""]
    L += [f"- Ingestor scrape availability over the window: **{up * 100:.2f}%**",
          f"- Kafka produce errors: **{produce_err:.0f}**", ""]

    L += ["## Gaps (ingestor view, Prometheus)", "",
          "| venue | kind | detected | recovered | absent at source | unrecoverable |", "|---|---|---:|---:|---:|---:|"]
    tot = [0, 0, 0, 0]
    for (v, k), n in sorted(detected.items()):
        r = [resolved.get((v, k, s), 0) for s in ("recovered", "absent_at_source", "unrecoverable")]
        tot = [tot[0] + n] + [t + x for t, x in zip(tot[1:], r)]
        L.append(f"| {v} | {k} | {n:.0f} | {r[0]:.0f} | {r[1]:.0f} | {r[2]:.0f} |")
    L.append(f"| **all** | | **{tot[0]:.0f}** | **{tot[1]:.0f}** | **{tot[2]:.0f}** | **{tot[3]:.0f}** |")
    L.append("")
    L.append("Gaps still open at report time are counted as detected but not yet resolved.")
    L.append("")

    L += ["## Trades", "", "| venue | via WebSocket | via REST backfill | ids missing in gaps | ids filled | p50 / p99 recovery |",
          "|---|---:|---:|---:|---:|---|"]
    for v in venues:
        L.append(f"| {v} | {trades.get((v, 'ws'), 0):.0f} | {trades.get((v, 'rest'), 0):.0f} | {missing.get((v,), 0):.0f} | "
                 f"{filled.get((v,), 0):.0f} | {fmt_s(rec['0.5'].get((v,), float('nan')))} / {fmt_s(rec['0.99'].get((v,), float('nan')))} |")
    L.append("")

    L += ["## Exchange → Kafka latency (live WebSocket trades)", "", "| venue | p50 | p99 |", "|---|---|---|"]
    for v in venues:
        L.append(f"| {v} | {fmt_s(lat['0.5'].get((v,), float('nan')))} | {fmt_s(lat['0.99'].get((v,), float('nan')))} |")
    L += ["", "Measured from the exchange's event timestamp to the broker ack, so it includes network time from the venue "
          "and any clock skew between the venue and this host. Histogram buckets bound the precision.", ""]

    L += ["## Reconnects", "", "| venue | reason | count |", "|---|---|---:|"]
    nonzero = [(k, n) for k, n in sorted(reconnects.items()) if n > 0]
    L += [f"| {v} | {r} | {n:.0f} |" for (v, r), n in nonzero] or ["| — | none | 0 |"]
    L.append("")

    L += ["## Kafka audit (independent check of `trades.v1`)", ""]
    hours = w[:-1] + "h" if w.endswith("h") else w
    try:
        audit = subprocess.run(["go", "run", "./ingest/cmd/audit", "--since", hours, "--format", "markdown", "--brokers", a.brokers],
                               capture_output=True, text=True, timeout=900, check=True).stdout
        L.append(audit.strip())
    except Exception as e:  # report still useful without it
        L.append(f"_audit failed: {e}_")
    L.append("")

    with open(a.out, "w") as f:
        f.write("\n".join(L))
    print(f"wrote {a.out}")


if __name__ == "__main__":
    sys.exit(main())

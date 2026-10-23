"""Run the investigator over the eval set and score it.

    uv run python evals/run_eval.py --set evals/eval_set.jsonl [--limit N] [--concurrency 4]
    uv run python evals/run_eval.py --set evals/ci_subset.jsonl --fixtures evals/fixtures --gate

Metrics (written to ~/.tidemark-data/eval/agent-<ts>.json and printed as markdown):
  triage accuracy     verdict.classification vs the label (injected -> escalate; real -> hand label)
  groundedness        share of narrative numbers traceable to a tool result (automated)
  claim support       share of cited claims found in the cited tool call's output
  judge pass rate     LLM-as-judge rubric; agreement with human grades reported separately
  cost / latency      per investigation, from API usage

--record saves every tool result to --fixtures so CI can replay without the data stack.
--gate exits non-zero if accuracy or groundedness falls below the thresholds.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import statistics
import sys
import time
from pathlib import Path

import anthropic

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "agent"))
from investigator.graph import build, investigate_alert  # noqa: E402
from investigator.tools import FixtureTools, McpTools, RecordingTools  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parent))
import judge  # noqa: E402

GATE = {"accuracy": 0.80, "groundedness": 0.95}


async def one(client, app_factory, item, sem, fixtures_dir, record):
    async with sem:
        async with app_factory(item) as (app, tools):
            t0 = time.monotonic()
            out = await investigate_alert(app, item["alert_id"], item["stream"])
            out["latency_s"] = round(time.monotonic() - t0, 2)
            if record and isinstance(tools, RecordingTools):
                (fixtures_dir / (item["alert_id"].replace(":", "_") + ".json")).write_text(
                    json.dumps(tools.recorded, sort_keys=True))
            if out.get("verdict"):
                log = [{"id": c["id"], "name": c["name"], "input": c["input"],
                        "output": await tools.call(c["name"], c["input"])} for c in out["tool_calls"]]
                alert = json.loads(await tools.call("get_alert", {"alert_id": item["alert_id"], "stream": item["stream"]}))
                g, resp = await judge.grade(client, alert, log, out["verdict"])
                out["judge"] = g.model_dump()
                out["judge_usage"] = {"input": resp.usage.input_tokens, "output": resp.usage.output_tokens}
            out["label"] = item.get("label")
            out["source"] = item.get("source")
            return out


def summarize(results: list[dict]) -> dict:
    done = [r for r in results if r.get("verdict")]
    labelled = [r for r in done if r.get("label")]
    correct = [r for r in labelled if r["verdict"]["classification"] == r["label"]]
    g = [r["grounding"]["groundedness"] for r in done]
    cs = [r["grounding"]["claim_support"] for r in done]
    j = [r["judge"]["overall_pass"] for r in done if r.get("judge")]
    cost = [r["usage"]["cost_usd"] for r in results]
    lat = [r["latency_s"] for r in results]
    by_src = {}
    for r in labelled:
        s = by_src.setdefault(r["source"], [0, 0])
        s[0] += r["verdict"]["classification"] == r["label"]
        s[1] += 1
    return {
        "cases": len(results), "completed": len(done), "errors": len(results) - len(done),
        "accuracy": round(len(correct) / len(labelled), 4) if labelled else None, "labelled": len(labelled),
        "accuracy_by_source": {k: round(a / n, 4) for k, (a, n) in by_src.items()},
        "groundedness_mean": round(statistics.mean(g), 4) if g else None,
        "fully_grounded_share": round(sum(x == 1.0 for x in g) / len(g), 4) if g else None,
        "claim_support_mean": round(statistics.mean(cs), 4) if cs else None,
        "judge_pass_rate": round(sum(j) / len(j), 4) if j else None,
        "cost_per_case_usd": round(statistics.mean(cost), 4) if cost else None,
        "cost_total_usd": round(sum(cost), 2),
        "latency_p50_s": round(statistics.median(lat), 1) if lat else None,
        "latency_p95_s": round(sorted(lat)[int(0.95 * (len(lat) - 1))], 1) if lat else None,
    }


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--set", required=True)
    ap.add_argument("--limit", type=int)
    ap.add_argument("--concurrency", type=int, default=4)
    ap.add_argument("--fixtures", type=Path)
    ap.add_argument("--record", action="store_true")
    ap.add_argument("--gate", action="store_true")
    a = ap.parse_args()
    items = [json.loads(l) for l in open(a.set) if l.strip()][: a.limit]
    client = anthropic.AsyncAnthropic()

    class Live:
        def __init__(self, item):
            self.item = item

        async def __aenter__(self):
            self.mcp = await McpTools().__aenter__()
            self.tools = RecordingTools(self.mcp) if a.record else self.mcp
            return build(client, self.tools), self.tools

        async def __aexit__(self, *exc):
            await self.mcp.__aexit__(*exc)

    class Replay:
        def __init__(self, item):
            path = a.fixtures / (item["alert_id"].replace(":", "_") + ".json")
            self.tools = FixtureTools(json.loads(path.read_text()))

        async def __aenter__(self):
            return build(client, self.tools), self.tools

        async def __aexit__(self, *exc):
            pass

    factory = Replay if (a.fixtures and not a.record) else Live
    if a.record:
        a.fixtures.mkdir(parents=True, exist_ok=True)
    sem = asyncio.Semaphore(a.concurrency)
    results = await asyncio.gather(*(one(client, factory, it, sem, a.fixtures, a.record) for it in items))
    summary = summarize(results)
    out = Path(os.path.expanduser("~/.tidemark-data/eval")) / f"agent-{int(time.time())}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps({"summary": summary, "results": results}, indent=1, default=str))
    print(json.dumps(summary, indent=2))
    print("details:", out)
    if a.gate:
        failed = [k for k, th in GATE.items()
                  if (summary["accuracy"] if k == "accuracy" else summary["groundedness_mean"]) is None
                  or (summary["accuracy"] if k == "accuracy" else summary["groundedness_mean"]) < th]
        if failed:
            print("GATE FAILED:", failed, GATE)
            return 1
        print("gate passed", GATE)
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))

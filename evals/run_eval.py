"""Run the investigator over the eval set and score it.

    uv run python evals/run_eval.py --set evals/eval_set.jsonl [--limit N] [--concurrency 4]
    uv run python evals/run_eval.py --set evals/ci_subset.jsonl --fixtures evals/fixtures --gate
    uv run python evals/run_eval.py --set evals/ci_subset.jsonl --backend cli   # on a Claude plan, via `claude -p`

Metrics (written to ~/.tidemark-data/eval/agent-<ts>.json and printed as markdown):
  triage accuracy     verdict.classification vs the label (injected -> escalate; real -> hand label)
  groundedness        share of narrative numbers traceable to a tool result (automated)
  claim support       share of cited claims found in the cited tool call's output
  judge pass rate     LLM-as-judge rubric; agreement with human grades reported separately
  cost / latency      per investigation, from API usage (API-equivalent estimate with --backend cli)

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
import traceback
from pathlib import Path

import anthropic

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "agent"))
from investigator.claude_cli import ClaudeCli  # noqa: E402
from investigator.graph import EFFORT, MODEL, build, investigate_alert  # noqa: E402
from investigator.tools import FixtureTools, McpTools, RecordingTools  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parent))
import judge  # noqa: E402

GATE = {"accuracy": 0.80, "groundedness": 0.95}


CASE_TIMEOUT_S = 1800
BACKEND_ERROR = "Error executing tool"  # MCP's text for an unexpected exception (a query error is JSON)


LIMIT_HIT = asyncio.Event()  # a Claude plan's usage limit: stop starting cases, resume later
LIMIT_TEXT = ("hit your session limit", "usage limit", "rate limit")


async def one(client, app_factory, item, sem, fixtures_dir, record, done_log):
    async with sem:
        if LIMIT_HIT.is_set():
            return {"alert_id": item["alert_id"], "stream": item["stream"], "verdict": None,
                    "error": "skipped: usage limit reached earlier in this run", "usage": {"cost_usd": 0.0},
                    "label": item.get("label"), "source": item.get("source"), "latency_s": 0.0}
        t0 = time.monotonic()
        try:
            out = await asyncio.wait_for(investigate_and_grade(client, app_factory, item, fixtures_dir, record),
                                         CASE_TIMEOUT_S)
        except Exception as e:  # one stuck or broken case must not sink the run
            out = {"alert_id": item["alert_id"], "stream": item["stream"], "verdict": None,
                   "error": f"{type(e).__name__}: {str(e)[:300]}", "usage": {"cost_usd": 0.0},
                   "traceback": traceback.format_exc(limit=-6)}
        if any(t in str(out.get("error")).lower() for t in LIMIT_TEXT):
            LIMIT_HIT.set()
            print(f"usage limit reached: {out['error']}", flush=True)
        out.setdefault("latency_s", round(time.monotonic() - t0, 2))
        out["label"] = item.get("label")
        out["source"] = item.get("source")
        with done_log.open("a") as f:  # results survive an interrupted run; reruns skip these cases
            f.write(json.dumps(out, default=str) + "\n")
        return out


async def investigate_and_grade(client, app_factory, item, fixtures_dir, record):
    async with app_factory(item) as (app, tools):
        t0 = time.monotonic()
        out = await investigate_alert(app, item["alert_id"], item["stream"])
        out["latency_s"] = round(time.monotonic() - t0, 2)
        if out.get("verdict"):
            log = await tool_log(tools, out)
            broken = [t["name"] for t in log if t["output"].startswith(BACKEND_ERROR)]
            if broken:
                # The data stack failed mid-case (e.g. Trino restarting): not the agent's fault, and
                # neither the investigation nor a grade is trustworthy. Retried on the next run.
                out["unscored_verdict"], out["verdict"] = out["verdict"], None
                out["error"] = f"tool backend error during case: {broken}"
                return out
            await grade(client, tools, item, out, log)
        # Written last, so the fixture holds every tool result the agent and the judge used.
        if record and isinstance(tools, RecordingTools):
            (fixtures_dir / (item["alert_id"].replace(":", "_") + ".json")).write_text(
                json.dumps(tools.recorded, sort_keys=True))
        return out


async def tool_log(tools, out: dict) -> list[dict]:
    return [{"id": c["id"], "name": c["name"], "input": c["input"],
             "output": await tools.call(c["name"], c["input"])} for c in out["tool_calls"]]


async def grade(client, tools, item: dict, out: dict, log: list[dict]) -> None:
    alert = json.loads(await tools.call("get_alert", {"alert_id": item["alert_id"], "stream": item["stream"]}))
    history = out.get("history")
    if history is None:  # results from before history was kept: the same call the graph makes
        history = json.loads(await tools.call("similar_past_alerts", {
            "detector": alert["detector"], "product_id": alert["product_id"],
            "before": alert["event_ts"], "stream": item["stream"], "k": 5}))
    try:
        g, resp = await judge.grade(client, alert, log, out["verdict"], history)
        out["judge"] = g.model_dump()
        out["judge_usage"] = {"input": resp.usage.input_tokens, "output": resp.usage.output_tokens}
        out.pop("judge_error", None)
    except Exception as e:  # a failed grade is reported, not fatal to the run
        out["judge_error"] = str(e)[:300]


async def regrade(client, items: list[dict], done_log: Path, fixtures: Path, concurrency: int) -> None:
    """Re-run only the judge on finished cases, from their recorded tool results (no investigation)."""
    rows = {}
    for line in done_log.read_text().splitlines():
        r = json.loads(line)
        rows[r["alert_id"]] = r
    sem = asyncio.Semaphore(concurrency)

    async def one_grade(r):
        async with sem:
            tools = FixtureTools(json.loads((fixtures / (r["alert_id"].replace(":", "_") + ".json")).read_text()))
            item = {"alert_id": r["alert_id"], "stream": r["stream"]}
            await grade(client, tools, item, r, await tool_log(tools, r))

    await asyncio.gather(*(one_grade(r) for r in rows.values() if r.get("verdict")))
    done_log.write_text("".join(json.dumps(r, default=str) + "\n" for r in rows.values()))
    print(f"regraded {sum(1 for r in rows.values() if r.get('verdict'))} cases", flush=True)


def summarize(results: list[dict]) -> dict:
    done = [r for r in results if r.get("verdict")]
    labelled = [r for r in done if r.get("label")]
    correct = [r for r in labelled if r["verdict"]["classification"] == r["label"]]
    g = [r["grounding"]["groundedness"] for r in done]
    cs = [r["grounding"]["claim_support"] for r in done]
    j = [r["judge"]["overall_pass"] for r in done if r.get("judge")]
    cost = [r["usage"].get("cost_usd", 0.0) for r in results]
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
    ap.add_argument("--backend", choices=["api", "cli"], default="api",
                    help="api: Claude API key; cli: the Claude Code CLI on a Claude subscription")
    ap.add_argument("--fresh", action="store_true", help="ignore results of an earlier, interrupted run")
    ap.add_argument("--regrade", action="store_true",
                    help="re-run only the judge on finished cases, from --fixtures (after a judge change)")
    a = ap.parse_args()
    items = [json.loads(l) for l in open(a.set) if l.strip()][: a.limit]
    eval_dir = Path(os.path.expanduser("~/.tidemark-data/eval"))
    eval_dir.mkdir(parents=True, exist_ok=True)
    done_log = eval_dir / f"progress-{Path(a.set).stem}-{a.backend}.jsonl"
    if a.fresh:
        done_log.unlink(missing_ok=True)
    client = ClaudeCli(MODEL, EFFORT) if a.backend == "cli" else anthropic.AsyncAnthropic()
    if a.regrade:
        await regrade(client, items, done_log, a.fixtures, a.concurrency)
    previous = {}
    if done_log.exists():
        for line in done_log.read_text().splitlines():
            r = json.loads(line)
            if r.get("verdict"):
                previous[r["alert_id"]] = r  # cases without a verdict are retried
    todo = [] if a.regrade else [it for it in items if it["alert_id"] not in previous]
    print(f"{len(items) - len(todo)} cases done in an earlier run, {len(todo)} to go", flush=True)

    # One MCP connection for the whole run: opening and closing one per case hung in the stdio
    # client's shutdown, and each connection costs a server process.
    shared: list[McpTools] = []

    class Live:
        def __init__(self, item):
            self.item = item

        async def __aenter__(self):
            self.tools = RecordingTools(shared[0]) if a.record else shared[0]
            return build(client, self.tools), self.tools

        async def __aexit__(self, *exc):
            pass

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
    if factory is Live and todo:
        shared.append(await McpTools().__aenter__())
    fresh = await asyncio.gather(*(one(client, factory, it, sem, a.fixtures, a.record, done_log) for it in todo))
    by_id = {**previous, **{r["alert_id"]: r for r in fresh}}
    results = [by_id[it["alert_id"]] for it in items]
    summary = summarize(results)
    summary["backend"] = a.backend
    summary["model"] = MODEL
    out = eval_dir / f"agent-{int(time.time())}.json"
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


async def main_and_exit() -> None:
    rc = await main()
    sys.stdout.flush()
    # Exit from inside the loop: asyncio.run's teardown of the shared MCP stdio client hangs
    # (anyio cancel scopes exited from another task).
    os._exit(rc)


if __name__ == "__main__":
    asyncio.run(main_and_exit())

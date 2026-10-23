"""The investigator as a LangGraph state machine.

    fetch_alert  ->  investigate  ->  conclude  ->  check_grounding  ->  route
    (tools, no LLM)  (Claude + tools)  (structured)   (deterministic)     (escalate queue / log)

Only `investigate` and `conclude` call Claude. Everything the model is allowed
to cite is recorded in `tool_log`, which the grounding check verifies against.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, TypedDict

import anthropic
from langgraph.graph import END, START, StateGraph

from . import grounding, prompts
from .cost import Usage
from .models import Verdict
from .tools import ToolBackend

MODEL = os.environ.get("TIDEMARK_AGENT_MODEL", "claude-opus-5-5")
EFFORT = os.environ.get("TIDEMARK_AGENT_EFFORT", "medium")
MAX_TOOL_TURNS = 10
# Refusal fallbacks: a safety decline is re-run server-side on Anthropic's recommended model.
FALLBACK = {"betas": ["server-side-fallback-2026-07-01"], "fallbacks": "default"}
CASES = Path(os.environ.get("TIDEMARK_CASES", os.path.expanduser("~/.tidemark-data/cases/queue.jsonl")))
INVESTIGATION_TOOLS = {"query_trades", "book_snapshot", "cross_venue_context", "similar_past_alerts"}


class State(TypedDict, total=False):
    alert_id: str
    stream: str
    alert: dict
    history: dict
    messages: list
    tool_log: list[dict]
    verdict: dict
    grounding: dict
    usage: Usage
    started: float
    seconds: float
    error: str
    routed_to: str


def build(client: anthropic.AsyncAnthropic, tools: ToolBackend):
    tool_defs: list[dict] = []

    async def ensure_tools() -> list[dict]:
        if not tool_defs:
            tool_defs.extend(t for t in await tools.list_tools() if t["name"] in INVESTIGATION_TOOLS)
        return tool_defs

    def system_blocks() -> list[dict]:
        # One breakpoint on the frozen system prompt caches tools + system for every investigation.
        return [{"type": "text", "text": prompts.SYSTEM, "cache_control": {"type": "ephemeral"}}]

    async def fetch_alert(state: State) -> State:
        alert = json.loads(await tools.call("get_alert", {"alert_id": state["alert_id"], "stream": state["stream"]}))
        if "error" in alert:
            return {"error": alert["error"], "alert": alert}
        history = json.loads(await tools.call("similar_past_alerts", {
            "detector": alert["detector"], "product_id": alert["product_id"],
            "before": alert["event_ts"], "stream": state["stream"], "k": 5}))
        return {"alert": alert, "history": history}

    async def investigate(state: State) -> State:
        usage = state["usage"]
        defs = await ensure_tools()
        messages: list[Any] = [{"role": "user", "content": prompts.INVESTIGATE.format(
            stream=state["stream"],
            alert_json=json.dumps(state["alert"], sort_keys=True, indent=1),
            history_json=json.dumps(state["history"], sort_keys=True, indent=1))}]
        log: list[dict] = []
        for _ in range(MAX_TOOL_TURNS):
            resp = await client.beta.messages.create(
                model=MODEL, max_tokens=16000, system=system_blocks(), tools=defs, messages=messages,
                output_config={"effort": EFFORT}, cache_control={"type": "ephemeral"}, **FALLBACK)
            usage.add(resp.model, resp.usage)
            if resp.stop_reason == "refusal":
                return {"error": f"refusal: {getattr(resp.stop_details, 'category', None)}", "messages": messages,
                        "tool_log": log}
            messages.append({"role": "assistant", "content": resp.content})
            calls = [b for b in resp.content if b.type == "tool_use"]
            if resp.stop_reason == "pause_turn":
                continue
            if not calls:
                break
            outputs = await asyncio.gather(*(tools.call(c.name, dict(c.input)) for c in calls))
            results = []
            for c, out in zip(calls, outputs):
                log.append({"id": c.id, "name": c.name, "input": dict(c.input), "output": out})
                is_err = '"error"' in out[:200]
                results.append({"type": "tool_result", "tool_use_id": c.id, "content": out, "is_error": is_err})
            messages.append({"role": "user", "content": results})  # all results in one message
        return {"messages": messages, "tool_log": log}

    async def conclude(state: State) -> State:
        usage = state["usage"]
        ids = ", ".join(t["id"] for t in state["tool_log"]) or "none"
        messages = state["messages"] + [{"role": "user", "content": prompts.CONCLUDE.format(tool_call_ids=ids)}]
        resp = await client.beta.messages.parse(
            model=MODEL, max_tokens=16000, system=system_blocks(), tools=await ensure_tools(), messages=messages,
            output_format=Verdict, output_config={"effort": EFFORT}, cache_control={"type": "ephemeral"}, **FALLBACK)
        usage.add(resp.model, resp.usage)
        if resp.stop_reason == "refusal" or resp.parsed_output is None:
            return {"error": f"no verdict (stop_reason={resp.stop_reason})"}
        return {"verdict": resp.parsed_output.model_dump()}

    async def check_grounding(state: State) -> State:
        v = state["verdict"]
        return {"grounding": grounding.check(v["narrative"], v["claims"], state["tool_log"], state.get("alert"))}

    async def route(state: State) -> State:
        record = {
            "alert_id": state["alert_id"], "stream": state["stream"],
            "decided_at": datetime.now(timezone.utc).isoformat(),
            "verdict": state.get("verdict"), "grounding": state.get("grounding"),
            "tool_calls": len(state.get("tool_log", [])), "usage": state["usage"].as_dict(),
            "seconds": round(time.monotonic() - state["started"], 2), "error": state.get("error"),
            "status": "pending_review",
        }
        escalate = (state.get("verdict") or {}).get("classification") == "escalate" or state.get("error")
        if escalate:
            CASES.parent.mkdir(parents=True, exist_ok=True)
            with CASES.open("a") as f:
                f.write(json.dumps(record, default=str) + "\n")
        return {"routed_to": "review_queue" if escalate else "log", "seconds": record["seconds"]}

    def after_fetch(state: State) -> str:
        return "route" if state.get("error") else "investigate"

    def after_investigate(state: State) -> str:
        return "route" if state.get("error") else "conclude"

    def after_conclude(state: State) -> str:
        return "route" if state.get("error") else "check_grounding"

    g = StateGraph(State)
    g.add_node("fetch_alert", fetch_alert)
    g.add_node("investigate", investigate)
    g.add_node("conclude", conclude)
    g.add_node("check_grounding", check_grounding)
    g.add_node("route", route)
    g.add_edge(START, "fetch_alert")
    g.add_conditional_edges("fetch_alert", after_fetch, ["investigate", "route"])
    g.add_conditional_edges("investigate", after_investigate, ["conclude", "route"])
    g.add_conditional_edges("conclude", after_conclude, ["check_grounding", "route"])
    g.add_edge("check_grounding", "route")
    g.add_edge("route", END)
    return g.compile()


async def investigate_alert(app, alert_id: str, stream: str = "live") -> dict:
    final = await app.ainvoke({"alert_id": alert_id, "stream": stream, "usage": Usage(), "started": time.monotonic()})
    return {
        "alert_id": alert_id, "stream": stream,
        "verdict": final.get("verdict"), "grounding": final.get("grounding"),
        "tool_calls": [{k: t[k] for k in ("id", "name", "input")} for t in final.get("tool_log", [])],
        "usage": final["usage"].as_dict(), "seconds": final.get("seconds"),
        "routed_to": final.get("routed_to"), "error": final.get("error"),
    }

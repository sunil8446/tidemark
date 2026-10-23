"""The investigator graph against a scripted model and fake tools (no API calls)."""

import json
from types import SimpleNamespace

import pytest

from investigator import graph as graph_mod
from investigator.cost import Usage
from investigator.graph import build, investigate_alert
from investigator.grounding import check, numbers_in_text
from investigator.models import Verdict

ALERT = {
    "alert_id": "volume_spike:kraken:SOL-USD:1792663960000", "detector": "volume_spike", "venue": "kraken",
    "product_id": "SOL-USD", "event_ts": "2026-10-22T10:12:50Z", "window_start": "2026-10-22T10:12:40Z",
    "window_end": "2026-10-22T10:12:50Z", "score": 13.0, "threshold": 6.0, "severity": "high",
    "summary": "kraken SOL-USD traded 56834 notional in 10 s", "evidence": {"window_notional": "56833.671938"},
}
TRADES = {"summary": {"trades": 37, "notional": "56833.671938", "price_range_bps": 3.830683777054204,
                      "buy_volume_share": 0.990241338216088}, "trades": [], "truncated": False}


class FakeTools:
    def __init__(self):
        self.calls = []

    async def list_tools(self):
        return [{"name": n, "description": n, "input_schema": {"type": "object", "properties": {}}}
                for n in ("book_snapshot", "cross_venue_context", "get_alert", "query_trades", "similar_past_alerts")]

    async def call(self, name, args):
        self.calls.append((name, args))
        if name == "get_alert":
            return json.dumps(ALERT)
        if name == "similar_past_alerts":
            return json.dumps({"base_rate": {"last_24h": 13}, "recent": []})
        if name == "query_trades":
            return json.dumps(TRADES)
        return json.dumps({"error": "not scripted"})


def usage(i=100, o=50, cr=0, cw=0):
    return SimpleNamespace(input_tokens=i, output_tokens=o, cache_read_input_tokens=cr, cache_creation_input_tokens=cw)


class ScriptedClient:
    """Plays back: one tool call, then a final text, then a structured verdict."""

    def __init__(self, verdict: Verdict, refuse=False):
        self.verdict, self.refuse, self.requests = verdict, refuse, []
        self.beta = SimpleNamespace(messages=SimpleNamespace(create=self._create, parse=self._parse))

    async def _create(self, **kw):
        self.requests.append({**kw, "messages": list(kw["messages"])})  # snapshot: the agent keeps appending
        if self.refuse:
            return SimpleNamespace(model=kw["model"], usage=usage(), stop_reason="refusal",
                                   stop_details=SimpleNamespace(category="cyber"), content=[])
        if len(self.requests) == 1:
            call = SimpleNamespace(type="tool_use", id="toolu_1", name="query_trades",
                                   input={"venue": "kraken", "product_id": "SOL-USD",
                                          "start": "2026-10-22T10:12:40Z", "end": "2026-10-22T10:12:50Z"})
            return SimpleNamespace(model=kw["model"], usage=usage(cw=4000), stop_reason="tool_use", content=[call])
        text = SimpleNamespace(type="text", text="Found 37 trades.")
        return SimpleNamespace(model=kw["model"], usage=usage(cr=4000), stop_reason="end_turn", content=[text])

    async def _parse(self, **kw):
        self.requests.append({**kw, "messages": list(kw["messages"])})
        return SimpleNamespace(model=kw["model"], usage=usage(cr=4000, o=400), stop_reason="end_turn",
                               parsed_output=self.verdict)


def verdict(narrative, claims, cls="escalate"):
    return Verdict(classification=cls, confidence="medium", hypothesis="Flat-price volume burst.",
                   narrative=narrative, claims=claims, benign_explanations_considered=["passive absorption"])


@pytest.fixture(autouse=True)
def cases_path(tmp_path, monkeypatch):
    monkeypatch.setattr(graph_mod, "CASES", tmp_path / "queue.jsonl")
    return tmp_path / "queue.jsonl"


async def test_full_run_is_grounded_cached_and_queued(cases_path):
    v = verdict("Kraken SOL-USD printed 37 trades worth 56,834 within 3.8 bps, 99% buyer-initiated.",
                [{"statement": "37 trades", "value": "37", "tool_call_id": "toolu_1"},
                 {"statement": "notional", "value": "56833.67", "tool_call_id": "toolu_1"}])
    client, tools = ScriptedClient(v), FakeTools()
    out = await investigate_alert(build(client, tools), ALERT["alert_id"], "live")

    assert out["error"] is None and out["verdict"]["classification"] == "escalate"
    assert out["grounding"]["groundedness"] == 1.0 and out["grounding"]["claim_support"] == 1.0
    assert out["routed_to"] == "review_queue" and cases_path.exists()
    # Every request: frozen cached system prompt, fallbacks on, explicit effort, no forced tool choice.
    for r in client.requests:
        assert r["system"][0]["cache_control"] == {"type": "ephemeral"}
        assert r["fallbacks"] == "default" and "server-side-fallback-2026-07-01" in r["betas"]
        assert r["output_config"]["effort"] in {"low", "medium", "high", "xhigh", "max"}
        assert "tool_choice" not in r
    # Tool results go back in one user message right after the tool call.
    second = client.requests[1]["messages"]
    assert second[-1]["role"] == "user" and second[-1]["content"][0]["tool_use_id"] == "toolu_1"
    assert [c[0] for c in tools.calls] == ["get_alert", "similar_past_alerts", "query_trades"]
    assert out["usage"]["requests"] == 3 and out["usage"]["cost_usd"] > 0


async def test_invented_numbers_are_flagged():
    v = verdict("37 trades moved the price 12.5 bps.",
                [{"statement": "move", "value": "12.5", "tool_call_id": "toolu_1"}], cls="likely_benign")
    out = await investigate_alert(build(ScriptedClient(v), FakeTools()), ALERT["alert_id"], "live")
    g = out["grounding"]
    assert g["ungrounded_numbers"] == ["12.5"] and g["groundedness"] == 0.5
    assert len(g["claims_not_supported_by_cited_call"]) == 1
    assert out["routed_to"] == "log"


async def test_refusal_is_routed_for_review_not_crashed(cases_path):
    out = await investigate_alert(build(ScriptedClient(verdict("x", []), refuse=True), FakeTools()),
                                  ALERT["alert_id"], "live")
    assert out["error"].startswith("refusal") and out["routed_to"] == "review_queue"


def test_number_extraction_and_tolerant_matching():
    assert numbers_in_text("traded 56,834 in 10 s, 3.8 bps, on 2026") == ["56834", "10", "3.8"]
    g = check("56,834 and 3.8 bps and 99%", [], [{"id": "t", "output": json.dumps(TRADES)}], ALERT)
    assert g["groundedness"] == 1.0, g


def test_cost_uses_cache_rates():
    u = Usage()
    u.add("claude-opus-5-5", usage(i=1_000_000, o=0))
    u.add("claude-opus-5-5", usage(i=0, o=0, cr=1_000_000))
    assert u.cost_usd() == 4.0 + 0.2


FAKE_CLAUDE = r'''#!/usr/bin/env python3
"""Stands in for `claude -p --output-format stream-json`: logs its argv, emits scripted events."""
import json, os, sys
argv = sys.argv[1:]
sys.stdin.read()
with open(os.environ["FAKE_CLAUDE_LOG"], "a") as f:
    f.write(json.dumps(argv) + "\n")
usage = {"input_tokens": 100, "output_tokens": 50, "cache_read_input_tokens": 0, "cache_creation_input_tokens": 0}
emit = lambda e: print(json.dumps(e))
emit({"type": "system", "subtype": "init"})
if "--json-schema" in argv:
    emit({"type": "result", "subtype": "success", "is_error": False, "usage": usage,
          "structured_output": json.loads(os.environ["FAKE_CLAUDE_VERDICT"])})
else:
    emit({"type": "assistant", "message": {"content": [{"type": "tool_use", "id": "toolu_cli1",
          "name": "mcp__tidemark__query_trades", "input": {"venue": "kraken"}}]}})
    emit({"type": "user", "message": {"content": [{"type": "tool_result", "tool_use_id": "toolu_cli1",
          "content": [{"type": "text", "text": os.environ["FAKE_CLAUDE_TRADES"]}]}]}})
    emit({"type": "assistant", "message": {"content": [{"type": "text", "text": "Found 37 trades."}]}})
    emit({"type": "result", "subtype": "success", "is_error": False, "usage": usage, "result": "Found 37 trades."})
'''


async def test_cli_backend_reads_tool_calls_from_stream_json(tmp_path, monkeypatch):
    from investigator.claude_cli import ClaudeCli

    fake = tmp_path / "claude"
    fake.write_text(FAKE_CLAUDE)
    fake.chmod(0o755)
    log = tmp_path / "argv.jsonl"
    v = verdict("37 trades carried 56833.67 notional.",
                [{"statement": "notional", "value": "56833.67", "tool_call_id": "toolu_cli1"}])
    monkeypatch.setenv("FAKE_CLAUDE_LOG", str(log))
    monkeypatch.setenv("FAKE_CLAUDE_VERDICT", v.model_dump_json())
    monkeypatch.setenv("FAKE_CLAUDE_TRADES", json.dumps(TRADES))

    cli = ClaudeCli("claude-opus-5-5", "medium", binary=str(fake), workdir=tmp_path / "work")
    out = await investigate_alert(build(cli, FakeTools()), ALERT["alert_id"], "live")

    assert out["error"] is None
    assert out["verdict"]["classification"] == "escalate"
    assert out["tool_calls"] == [{"id": "toolu_cli1", "name": "query_trades", "input": {"venue": "kraken"}}]
    assert out["grounding"]["groundedness"] == 1.0
    assert out["usage"]["requests"] == 2
    investigate_argv, conclude_argv = [json.loads(l) for l in log.read_text().splitlines()]
    # Investigation: MCP tools only, a fresh session; conclusion resumes that session with the schema.
    assert investigate_argv[investigate_argv.index("--tools") + 1] == ""
    assert "mcp__tidemark__query_trades" in investigate_argv[investigate_argv.index("--allowedTools") + 1]
    session = investigate_argv[investigate_argv.index("--session-id") + 1]
    assert conclude_argv[conclude_argv.index("--resume") + 1] == session
    assert "--json-schema" in conclude_argv


async def test_cli_backend_reports_a_failed_run_as_an_error(tmp_path):
    from investigator.claude_cli import ClaudeCli

    fake = tmp_path / "claude"
    fake.write_text('#!/bin/sh\necho \'{"type":"result","subtype":"success","is_error":true,'
                    '"result":"Not logged in"}\'\n')
    fake.chmod(0o755)
    cli = ClaudeCli("claude-opus-5-5", "medium", binary=str(fake), workdir=tmp_path / "work")
    out = await investigate_alert(build(cli, FakeTools()), ALERT["alert_id"], "live")
    assert "Not logged in" in out["error"]
    assert out["verdict"] is None


def test_clock_times_are_checked_against_timestamps():
    out = json.dumps({"trades": [{"ts": "2026-10-22T08:47:34.314917Z", "price": "103.66"}]})
    log = [{"id": "t", "input": {"start": "2026-10-22T08:44:00Z"}, "output": out}]
    g = check("A print at 08:47:34.314917 for 103.66, in a window from 08:44.",
              [{"statement": "print", "value": "2026-10-22T08:47:34.314917Z", "tool_call_id": "t"}], log)
    assert g["ungrounded_numbers"] == [] and g["claim_support"] == 1.0, g
    assert check("A print at 09:12:01.", [], log)["ungrounded_numbers"] == ["09:12:01"]

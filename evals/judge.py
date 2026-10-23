"""LLM-as-judge for investigator case files.

The judge never sees the ground-truth label: it grades the case on its own
merits against a fixed rubric, so its scores can be compared with human
grades of the same cases (calibration) before they are trusted.
"""

from __future__ import annotations

import json
from types import SimpleNamespace

import anthropic
from investigator import claude_cli
from investigator.claude_cli import ClaudeCli
from pydantic import BaseModel, Field

JUDGE_MODEL = "claude-opus-5-5"

RUBRIC = """\
You grade case files written by a market-surveillance investigator. You see the \
alert, every tool result the investigator received, and its case file. Grade \
strictly on these criteria, each pass or fail:

1. evidence_use: the narrative rests on the tool results; it checked the alert's \
own window and compared against context (before/after, the other venue, or history).
2. alternatives: it considered at least one ordinary market explanation and said \
why it does or does not fit, using data.
3. no_fabrication: no number, fact or attribution appears that the tool results do \
not support; it does not claim to know who traded or why.
4. calibration: the classification and confidence follow from the evidence it \
cites (strong evidence -> high confidence; thin or missing data -> low confidence).
5. clarity: an analyst could act on it in under a minute.

Overall pass = all five pass."""


class Grade(BaseModel):
    evidence_use: bool
    alternatives: bool
    no_fabrication: bool
    calibration: bool
    clarity: bool
    overall_pass: bool
    reason: str = Field(description="Two sentences: the main strength and the main weakness.")


async def grade(client, alert: dict, tool_log: list[dict], verdict: dict) -> tuple[Grade, object]:
    """`client` is an AsyncAnthropic, or a ClaudeCli to grade on a Claude subscription."""
    case = {
        "alert": alert,
        "tool_results": [{"id": t["id"], "tool": t["name"], "input": t["input"], "output": t["output"][:6000]}
                         for t in tool_log],
        "case_file": verdict,
    }
    if isinstance(client, ClaudeCli):
        events = await client.run(json.dumps(case, default=str), system=RUBRIC, tools=[],
                                  json_schema=Grade.model_json_schema(), effort="low")
        return Grade.model_validate(claude_cli.structured_output(events)), SimpleNamespace(
            usage=claude_cli.usage_of(events))
    resp = await client.beta.messages.parse(
        model=JUDGE_MODEL, max_tokens=4000,
        system=[{"type": "text", "text": RUBRIC, "cache_control": {"type": "ephemeral"}}],
        messages=[{"role": "user", "content": json.dumps(case, default=str)}],
        output_format=Grade, output_config={"effort": "low"},
        betas=["server-side-fallback-2026-07-01"], fallbacks="default",
    )
    return resp.parsed_output, resp

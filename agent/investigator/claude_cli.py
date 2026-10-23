"""Run Claude through the Claude Code CLI (`claude -p`) instead of the API.

This bills a Claude subscription's usage limits rather than an API key, which
makes it the cheap way to run evaluations on a personal plan. The investigation
is the same: same system prompt, same MCP tools (Claude Code connects to the
Tidemark MCP server itself), same Verdict schema. Tool calls and results are
read back from the CLI's stream-json events, so grounding checks see exactly
what the model saw.

Token counts come from the CLI's usage report; dollar figures computed from
them are API-equivalent estimates, not what the subscription charges.
"""

from __future__ import annotations

import asyncio
import json
import os
import uuid
from pathlib import Path
from types import SimpleNamespace
from typing import Any

MCP_PREFIX = "mcp__tidemark__"
TIMEOUT_S = 900


class CliError(RuntimeError):
    pass


class ClaudeCli:
    """Spawns one `claude -p` process per call. Sessions are resumed by id for multi-step work."""

    def __init__(self, model: str, effort: str, binary: str | None = None, workdir: Path | None = None):
        self.model = model
        self.effort = effort
        self.binary = binary or os.environ.get("TIDEMARK_CLAUDE_BIN", "claude")
        # A dedicated, empty working directory: no CLAUDE.md or project settings leak into the prompt.
        self.workdir = workdir or Path(os.path.expanduser("~/.tidemark-data/agent-cli"))
        self.workdir.mkdir(parents=True, exist_ok=True)
        mcp_command = os.environ.get(
            "TIDEMARK_MCP_COMMAND", os.path.expanduser("~/.tidemark-data/venvs/mcp/bin/tidemark-mcp"))
        self.mcp_config = self.workdir / "mcp.json"
        self.mcp_config.write_text(json.dumps({"mcpServers": {"tidemark": {"command": mcp_command, "args": []}}}))

    async def run(self, prompt: str, *, system: str, tools: list[str], session_id: str | None = None,
                  resume: str | None = None, json_schema: dict | None = None, effort: str | None = None,
                  ) -> list[dict]:
        args = [self.binary, "-p", "--output-format", "stream-json", "--verbose",
                "--model", self.model, "--effort", effort or self.effort,
                "--system-prompt", system, "--setting-sources", "", "--disable-slash-commands",
                "--strict-mcp-config", "--mcp-config", str(self.mcp_config),
                # No built-in tools (Bash, Read, ...): the investigator only gets the MCP tools.
                "--tools", ""]
        if tools:
            args += ["--allowedTools", ",".join(MCP_PREFIX + t for t in tools)]
        if session_id:
            args += ["--session-id", session_id]
        if resume:
            args += ["--resume", resume]
        if json_schema:
            args += ["--json-schema", json.dumps(json_schema)]
        proc = await asyncio.create_subprocess_exec(
            *args, cwd=self.workdir, stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        try:
            out, err = await asyncio.wait_for(proc.communicate(prompt.encode()), TIMEOUT_S)
        except asyncio.TimeoutError:
            proc.kill()
            raise CliError(f"claude -p timed out after {TIMEOUT_S}s")
        events = [json.loads(line) for line in out.decode().splitlines() if line.startswith("{")]
        result = next((e for e in reversed(events) if e.get("type") == "result"), None)
        if result is None:
            raise CliError(f"claude -p exited {proc.returncode} without a result: {err.decode()[-500:]}")
        if result.get("is_error"):
            raise CliError(f"claude -p: {result.get('subtype')}: {str(result.get('result'))[:300]}")
        return events


def result_of(events: list[dict]) -> dict:
    return next(e for e in reversed(events) if e.get("type") == "result")


def usage_of(events: list[dict]) -> SimpleNamespace:
    """Total usage of one CLI run, shaped like an API `usage` object."""
    u = result_of(events).get("usage") or {}
    return SimpleNamespace(**{k: u.get(k, 0) or 0 for k in (
        "input_tokens", "output_tokens", "cache_read_input_tokens", "cache_creation_input_tokens")})


def structured_output(events: list[dict]) -> Any:
    r = result_of(events)
    if r.get("structured_output") is not None:
        return r["structured_output"]
    return json.loads(r.get("result") or "null")


def _text(content: Any) -> str:
    if isinstance(content, str):
        return content
    return "\n".join(c.get("text", "") for c in content or [] if isinstance(c, dict))


def tool_log(events: list[dict]) -> list[dict]:
    """Tool calls in call order, each with the exact output the model received."""
    calls: dict[str, dict] = {}
    order: list[str] = []
    for e in events:
        content = (e.get("message") or {}).get("content")
        if not isinstance(content, list):
            continue
        for b in content:
            if e["type"] == "assistant" and b.get("type") == "tool_use":
                calls[b["id"]] = {"id": b["id"], "name": b["name"].removeprefix(MCP_PREFIX),
                                  "input": b.get("input") or {}, "output": ""}
                order.append(b["id"])
            elif e["type"] == "user" and b.get("type") == "tool_result" and b.get("tool_use_id") in calls:
                calls[b["tool_use_id"]]["output"] = _text(b.get("content"))
    return [calls[i] for i in order]


def new_session_id() -> str:
    return str(uuid.uuid4())

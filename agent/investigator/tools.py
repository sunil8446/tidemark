"""Tool backends: the investigator only sees tool names, schemas and JSON results.

McpTools talks to the Tidemark MCP server over stdio (production and evals).
Tests use a fake with the same two methods.
"""

from __future__ import annotations

import json
import os
from contextlib import AsyncExitStack
from typing import Any, Protocol

from mcp import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client

MAX_RESULT_CHARS = 24_000  # bound what one tool result can add to the context


class ToolBackend(Protocol):
    async def list_tools(self) -> list[dict]: ...
    async def call(self, name: str, args: dict[str, Any]) -> str: ...


class McpTools:
    """Spawns `tidemark-mcp` and exposes its tools in Claude API format."""

    def __init__(self, command: str | None = None, env: dict[str, str] | None = None):
        self.command = command or os.environ.get(
            "TIDEMARK_MCP_COMMAND", os.path.expanduser("~/.tidemark-data/venvs/mcp/bin/tidemark-mcp"))
        self.env = {**os.environ, **(env or {})}
        self._stack = AsyncExitStack()
        self.session: ClientSession | None = None

    async def __aenter__(self) -> "McpTools":
        read, write = await self._stack.enter_async_context(
            stdio_client(StdioServerParameters(command=self.command, env=self.env)))
        self.session = await self._stack.enter_async_context(ClientSession(read, write))
        await self.session.initialize()
        return self

    async def __aexit__(self, *exc) -> None:
        await self._stack.aclose()

    async def list_tools(self) -> list[dict]:
        res = await self.session.list_tools()
        tools = []
        for t in sorted(res.tools, key=lambda t: t.name):  # deterministic order keeps the cache prefix stable
            schema = getattr(t, "input_schema", None) or getattr(t, "inputSchema", None)
            tools.append({"name": t.name, "description": t.description or "", "input_schema": schema})
        return tools

    async def call(self, name: str, args: dict[str, Any]) -> str:
        res = await self.session.call_tool(name, args)
        structured = getattr(res, "structured_content", None) or getattr(res, "structuredContent", None)
        if structured is not None:
            text = json.dumps(structured.get("result", structured), default=str, sort_keys=True)
        else:
            text = "\n".join(getattr(c, "text", "") for c in res.content)
        return text[:MAX_RESULT_CHARS]


class RecordingTools:
    """Wraps a backend and records every call, so CI can replay tool results without the data stack."""

    def __init__(self, inner: ToolBackend):
        self.inner = inner
        self.recorded: dict[str, str] = {}

    @staticmethod
    def key(name: str, args: dict[str, Any]) -> str:
        return name + " " + json.dumps(args, sort_keys=True, default=str)

    async def list_tools(self) -> list[dict]:
        tools = await self.inner.list_tools()
        self.recorded["__tools__"] = json.dumps(tools, sort_keys=True)
        return tools

    async def call(self, name: str, args: dict[str, Any]) -> str:
        out = await self.inner.call(name, args)
        self.recorded[self.key(name, args)] = out
        return out


class FixtureTools:
    """Serves recorded tool results. A call the recording never saw gets an explicit
    'not recorded' error, which the agent must treat like any missing data."""

    def __init__(self, recorded: dict[str, str]):
        self.recorded = recorded

    async def list_tools(self) -> list[dict]:
        return json.loads(self.recorded["__tools__"])

    async def call(self, name: str, args: dict[str, Any]) -> str:
        return self.recorded.get(RecordingTools.key(name, args),
                                 json.dumps({"error": "no recorded result for this call (fixture replay)"}))

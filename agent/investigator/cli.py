"""tidemark-investigate --alert-id <id> [--stream live|inj] [--backend api|cli]"""

from __future__ import annotations

import argparse
import asyncio
import json

import anthropic

from .claude_cli import ClaudeCli
from .graph import EFFORT, MODEL, build, investigate_alert
from .tools import McpTools


async def run(alert_id: str, stream: str, backend: str = "api") -> dict:
    client = ClaudeCli(MODEL, EFFORT) if backend == "cli" else anthropic.AsyncAnthropic()
    async with McpTools() as tools:
        app = build(client, tools)
        return await investigate_alert(app, alert_id, stream)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--alert-id", required=True)
    ap.add_argument("--stream", default="live", choices=["live", "inj"])
    ap.add_argument("--backend", default="api", choices=["api", "cli"],
                    help="api: Claude API key; cli: the Claude Code CLI on a Claude subscription")
    a = ap.parse_args()
    print(json.dumps(asyncio.run(run(a.alert_id, a.stream, a.backend)), indent=2, default=str))


if __name__ == "__main__":
    main()

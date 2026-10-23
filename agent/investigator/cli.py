"""tidemark-investigate --alert-id <id> [--stream live|inj]"""

from __future__ import annotations

import argparse
import asyncio
import json

import anthropic

from .graph import build, investigate_alert
from .tools import McpTools


async def run(alert_id: str, stream: str) -> dict:
    async with McpTools() as tools:
        app = build(anthropic.AsyncAnthropic(), tools)
        return await investigate_alert(app, alert_id, stream)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--alert-id", required=True)
    ap.add_argument("--stream", default="live", choices=["live", "inj"])
    a = ap.parse_args()
    print(json.dumps(asyncio.run(run(a.alert_id, a.stream)), indent=2, default=str))


if __name__ == "__main__":
    main()

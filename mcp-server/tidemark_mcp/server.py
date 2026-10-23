"""Tidemark MCP server: read-only tools for investigating surveillance alerts.

Run over stdio (what the investigator agent and Claude Code use):

    tidemark-mcp

Configuration comes from the environment: TRINO_HOST/TRINO_PORT/TRINO_USER,
CLICKHOUSE_HOST/CLICKHOUSE_PORT/CLICKHOUSE_USER/CLICKHOUSE_PASSWORD.
"""

from __future__ import annotations

import os
import threading
from typing import Any

import clickhouse_connect
import trino
from mcp.server.mcpserver import MCPServer
from mcp.types import ToolAnnotations

from .queries import Detector, Product, Queries, QueryError, Stream, Venue

READ_ONLY = ToolAnnotations(read_only_hint=True, destructive_hint=False, idempotent_hint=True, open_world_hint=False)

INSTRUCTIONS = """\
Tools for investigating Tidemark market-surveillance alerts. All tools are
read-only and bounded: time windows are capped at 30 minutes and results at a
few hundred rows. Every number you report must come from a tool result.
Use stream="live" for production alerts and stream="inj" for the evaluation
stream that contains injected, labelled patterns.
"""


# Tools run concurrently in worker threads (an agent may call several at once), and neither
# client is safe to share across threads: clickhouse-connect rejects concurrent queries in
# one session. Each thread gets its own connection.


class TrinoClient:
    def __init__(self) -> None:
        self.local = threading.local()

    @property
    def conn(self):
        if not hasattr(self.local, "conn"):
            self.local.conn = trino.dbapi.connect(
                host=os.environ.get("TRINO_HOST", "localhost"),
                port=int(os.environ.get("TRINO_PORT", "8088")),
                user=os.environ.get("TRINO_USER", "tidemark-mcp"),
                catalog="iceberg",
                request_timeout=30,
            )
        return self.local.conn

    def query(self, sql: str, params: tuple) -> list[tuple]:
        cur = self.conn.cursor()
        cur.execute(sql, params)
        return cur.fetchall()


class ClickHouseClient:
    def __init__(self) -> None:
        self.local = threading.local()

    @property
    def client(self):
        if not hasattr(self.local, "client"):
            self.local.client = clickhouse_connect.get_client(
                host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
                port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
                username=os.environ.get("CLICKHOUSE_USER", "tidemark"),
                password=os.environ.get("CLICKHOUSE_PASSWORD", "tidemark-local-dev"),
                settings={"readonly": 1, "max_execution_time": 20},
            )
        return self.local.client

    def query(self, sql: str, params: dict) -> list[dict]:
        res = self.client.query(sql, parameters=params)
        return [dict(zip(res.column_names, row)) for row in res.result_rows]


def build_server(q: Queries) -> MCPServer:
    server = MCPServer(name="tidemark", version="0.4.0", instructions=INSTRUCTIONS)

    def safe(fn, *args: Any) -> Any:
        try:
            return fn(*args)
        except QueryError as e:
            return {"error": str(e)}

    @server.tool(annotations=READ_ONLY)
    def get_alert(alert_id: str, stream: Stream = "live") -> dict:
        """Fetch one alert: detector, venue, product, evidence window, score vs threshold, severity,
        one-line summary and the detector's evidence map."""
        return safe(q.get_alert, alert_id, stream)

    @server.tool(annotations=READ_ONLY)
    def query_trades(venue: Venue, product_id: Product, start: str, end: str, stream: Stream = "live",
                     limit: int = 200) -> dict:
        """Trades for one venue and product in [start, end) (ISO-8601, at most 30 minutes):
        summary statistics (count, volume, notional, VWAP, OHLC, price range in bps, buy share,
        REST-backfilled count) and up to `limit` (max 500) trades in time order."""
        return safe(q.query_trades, venue, product_id, start, end, stream, limit)

    @server.tool(annotations=READ_ONLY)
    def book_snapshot(product_id: Product, at: str, stream: Stream = "live", depth: int = 10) -> dict:
        """Kraken order book for a product as of a timestamp, rebuilt from the lakehouse
        (top `depth` levels per side, best bid/ask, spread, visible depth). Coinbase book data
        is not collected."""
        return safe(q.book_snapshot, product_id, at, stream, depth)

    @server.tool(annotations=READ_ONLY)
    def cross_venue_context(product_id: Product, start: str, end: str, stream: Stream = "live",
                            bucket_seconds: int = 10) -> dict:
        """Coinbase vs Kraken for one product in [start, end): per-bucket VWAP, volume, trade
        count and spread in bps, plus a summary of the spread."""
        return safe(q.cross_venue_context, product_id, start, end, stream, bucket_seconds)

    @server.tool(annotations=READ_ONLY)
    def similar_past_alerts(detector: Detector, product_id: Product, before: str, stream: Stream = "live",
                            k: int = 10) -> dict:
        """The detector's most recent alerts for this product before a timestamp (up to k, max 20)
        and its 24-hour base rate (count, median and p95 score) for context on how unusual this is."""
        return safe(q.similar_past_alerts, detector, product_id, before, stream, k)

    return server


def main() -> None:
    build_server(Queries(trino=TrinoClient(), clickhouse=ClickHouseClient())).run("stdio")


if __name__ == "__main__":
    main()

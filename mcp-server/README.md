# tidemark-mcp

Read-only MCP server (Python SDK 2.x, stdio) that gives the investigator agent, or Claude Code, bounded access to Tidemark's data.

| Tool | Backed by | Bounds |
|---|---|---|
| `get_alert` | ClickHouse `tidemark.alerts` | alert id validated against a strict pattern |
| `query_trades` | Iceberg `raw_trades` via Trino | window ≤ 30 min, ≤ 500 rows, summary computed in SQL |
| `book_snapshot` | Iceberg `book_snapshots` (10 s checkpoints) + `book_deltas` | nearest checkpoint ≤ 2 min before, then deltas; truncated to depth |
| `cross_venue_context` | Iceberg `raw_trades` via Trino | window ≤ 30 min, ≤ 400 buckets |
| `similar_past_alerts` | ClickHouse | ≤ 20 rows plus the 24 h base rate |

Every query is a fixed template with bound parameters. The only interpolated identifier is the Iceberg namespace, chosen from a whitelist (`live` → `market`, `inj` → `market_inj`). ClickHouse connections run with `readonly=1`. All tools carry `read_only_hint`.

```bash
UV_PROJECT_ENVIRONMENT=~/.tidemark-data/venvs/mcp uv sync && uv run pytest -q
UV_PROJECT_ENVIRONMENT=~/.tidemark-data/venvs/mcp uv run tidemark-mcp   # stdio
```

Configuration: `TRINO_HOST`, `TRINO_PORT`, `CLICKHOUSE_HOST`, `CLICKHOUSE_PORT`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`.

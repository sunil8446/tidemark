# Exchange feeds: verified notes

Checked against official docs on 2026-10-22. Exchange feeds change, so re-verify before relying on anything marked *inferred*.

## Coinbase Exchange

**WebSocket:** `wss://ws-feed.exchange.coinbase.com`

| Item | Detail |
|---|---|
| Channels used | `matches` (public), `heartbeat` (public) |
| Subscribe | `{"type":"subscribe","product_ids":["BTC-USD"],"channels":["matches","heartbeat"]}` |
| `match` fields | `type, trade_id (int), sequence (int), maker_order_id, taker_order_id, time (ISO8601 µs), product_id, size (string), price (string), side` |
| `side` meaning | **Maker** side |
| On subscribe | One `last_match` message, same shape as `match` |
| Heartbeat | 1/s per product: `{type, sequence, last_trade_id, product_id, time}` |
| Drop behaviour | Docs say `matches` messages **can be dropped**. Use heartbeat `last_trade_id` and REST to detect and fill gaps. |
| Rate limits (per IP) | 8 connections/s (burst 20). Inbound messages 10/s (burst 1000). |

**REST backfill:** `GET https://api.exchange.coinbase.com/products/{id}/trades`

- Cursor pagination, newest first. `CB-AFTER` header → pass as `after=` for **older** trades. `CB-BEFORE` → `before=` for **newer**.
- `limit` max 1000. Fields: `trade_id, side, size, price, time`.
- Public rate limit: 10 req/s per IP, burst 15.

*Inferred, not documented:* `trade_id` is a per-product increasing integer. The heartbeat `last_trade_id` and the gap-backfill guidance only make sense if it is. The ingestor treats a jump as a gap and a step backwards as an anomaly to log.

**Fallback:** the Advanced Trade WS (`wss://advanced-trade-ws.coinbase.com`, `market_trades`) is also public. It batches trades every 250 ms and closes idle channels unless subscribed to `heartbeats`.

## Kraken WebSocket v2

**WebSocket:** `wss://ws.kraken.com/v2`

| Item | Detail |
|---|---|
| Subscribe | `{"method":"subscribe","params":{"channel":"trade","symbol":["BTC/USD"],"snapshot":true}}` |
| Trade fields | `symbol, side, price, qty, ord_type, trade_id, timestamp (RFC3339)` |
| `side` meaning | **Taker** side (opposite of Coinbase, so normalise) |
| Numbers | `price` and `qty` are **JSON numbers**, not strings |
| `trade_id` | Documented as a sequence number unique per book, so sequential per symbol |
| Book depth | 10, 25, 100, 500 or 1000. A `snapshot` comes first, then `update` messages with changed levels. Apply updates in order. |
| Heartbeat | `{"channel":"heartbeat"}` about 1/s, **only when nothing else is flowing**. Starts automatically after any subscription. |
| Connection limit | About 150 connection attempts per 10 min per IP (Cloudflare). Exceeding it bans the IP for 10 min, so reconnect backoff must respect this. |

### Book CRC32 checksum

- Always computed over the **top 10** levels, whatever the subscribed depth.
- Order: asks low → high, then bids high → low.
- For each level, take the price text and the qty text, remove the `.`, strip leading zeros, and append price then qty.
- Run CRC32 (IEEE) over the concatenated string and compare as uint32 with `data[].checksum`.
- **Precision matters.** Because values arrive as JSON numbers, the ingestor must decode with a decimal-preserving parser and format to the pair's price and qty precision, keeping trailing zeros. Get the precision from the v2 `instrument` channel (*inferred*: the checksum page does not spell this out).

### REST backfill

`GET https://api.kraken.com/0/public/Trades?pair=&since=&count=`

- `count` 1–1000. Each row is `[price, volume, time, side, ordtype, misc, trade_id]`, so `trade_id` is element 7.
- Page by passing the response's `last` as the next `since`. **`since` is a nanosecond timestamp cursor, not a trade_id.** Whether it is inclusive is undocumented, so dedupe by `trade_id`.
- Stay at or below 1 req/s per pair.

## Normalised trade (what the ingestor emits)

| Field | Coinbase source | Kraken source |
|---|---|---|
| `venue` | `"coinbase"` | `"kraken"` |
| `product_id` | `product_id` (`BTC-USD`) | `symbol` (`BTC/USD` → `BTC-USD`) |
| `trade_id` | `trade_id` | `trade_id` |
| `price`, `size` | decimal strings | decimal text preserved from JSON |
| `taker_side` | inverse of `side` | `side` |
| `event_ts` | `time` | `timestamp` |
| `source` | `ws` or `rest` | `ws` or `rest` |

## Sources

- https://docs.cdp.coinbase.com/exchange/websocket-feed/channels
- https://docs.cdp.coinbase.com/exchange/websocket-feed/rate-limits
- https://docs.cdp.coinbase.com/exchange/rest-api/rate-limits
- https://docs.cdp.coinbase.com/exchange/rest-api/pagination
- https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-channels
- https://docs.kraken.com/api/docs/websocket-v2/trade
- https://docs.kraken.com/api/docs/websocket-v2/book
- https://docs.kraken.com/api/docs/guides/spot-ws-book-v2
- https://docs.kraken.com/api/docs/websocket-v2/heartbeat
- https://docs.kraken.com/api/docs/rest-api/get-recent-trades
- https://docs.kraken.com/api/docs/guides/spot-ratelimits/

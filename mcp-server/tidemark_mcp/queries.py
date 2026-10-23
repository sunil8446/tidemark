"""Read-only data access for the investigator tools.

Every query is a fixed template. User-supplied values are bound as
parameters; the only interpolated pieces are identifiers chosen from
whitelists (the Iceberg namespace for a stream). Every query has a row limit
and a time-window cap, so a tool call can never scan the lake or return an
unbounded result.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Literal, Protocol

Stream = Literal["live", "inj"]
Venue = Literal["coinbase", "kraken"]
Product = Literal["BTC-USD", "ETH-USD", "SOL-USD"]
Detector = Literal["volume_spike", "spoof_layering", "dislocation", "wash_trade"]

NAMESPACE: dict[str, str] = {"live": "market", "inj": "market_inj"}
MAX_WINDOW = timedelta(minutes=30)
MAX_TRADE_ROWS = 500
MAX_BOOK_ROWS = 50_000
ALERT_ID = re.compile(r"^[a-z_]+:[a-z]+:[A-Z]+-USD:[0-9]+(:[0-9]+)*$")


class QueryError(ValueError):
    """A request outside the allowed shape (bad id, window too long, ...)."""


class Trino(Protocol):
    def query(self, sql: str, params: tuple) -> list[tuple]: ...


class ClickHouse(Protocol):
    def query(self, sql: str, params: dict) -> list[dict]: ...


def parse_ts(value: str) -> datetime:
    try:
        ts = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as e:
        raise QueryError(f"not an ISO-8601 timestamp: {value!r}") from e
    return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)


def window(start: str, end: str) -> tuple[datetime, datetime]:
    lo, hi = parse_ts(start), parse_ts(end)
    if hi <= lo:
        raise QueryError("end must be after start")
    if hi - lo > MAX_WINDOW:
        raise QueryError(f"window too long ({hi - lo}); the limit is {MAX_WINDOW}")
    return lo, hi


def iso(ts: datetime) -> str:
    # ClickHouse and Trino hand back naive datetimes for UTC columns. astimezone()
    # would read a naive value as *local* time and shift every evidence window.
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def num(v: Any) -> Any:
    """JSON-friendly numbers: Decimals become strings so no precision is lost."""
    if isinstance(v, Decimal):
        return format(v.normalize(), "f")
    if isinstance(v, datetime):
        return iso(v)
    return v


@dataclass
class Queries:
    trino: Trino
    clickhouse: ClickHouse

    # ------------------------------------------------------------------ alerts

    def get_alert(self, alert_id: str, stream: Stream) -> dict:
        if not ALERT_ID.match(alert_id):
            raise QueryError(f"malformed alert id: {alert_id!r}")
        rows = self.clickhouse.query(
            """SELECT alert_id, detector, venue, product_id, event_ts, window_start, window_end,
                      score, threshold, severity, summary, evidence, emitted_at
               FROM tidemark.alerts FINAL
               WHERE stream = {stream:String} AND alert_id = {id:String}
               LIMIT 1""",
            {"stream": stream, "id": alert_id},
        )
        if not rows:
            raise QueryError(f"no alert {alert_id!r} in stream {stream!r}")
        return {k: num(v) for k, v in rows[0].items()}

    def similar_past_alerts(self, detector: Detector, product_id: Product, before: str, stream: Stream,
                            k: int = 10) -> dict:
        k = max(1, min(k, 20))
        t = parse_ts(before)
        rows = self.clickhouse.query(
            """SELECT alert_id, venue, event_ts, round(score, 3) AS score, severity, summary
               FROM tidemark.alerts FINAL
               WHERE stream = {stream:String} AND detector = {d:String} AND product_id = {p:String}
                 AND event_ts < {t:DateTime64(3)}
               ORDER BY event_ts DESC
               LIMIT {k:UInt32}""",
            {"stream": stream, "d": detector, "p": product_id, "t": t, "k": k},
        )
        base = self.clickhouse.query(
            """SELECT count() AS last_24h, round(quantile(0.5)(score), 3) AS median_score,
                      round(quantile(0.95)(score), 3) AS p95_score
               FROM tidemark.alerts FINAL
               WHERE stream = {stream:String} AND detector = {d:String} AND product_id = {p:String}
                 AND event_ts BETWEEN {t:DateTime64(3)} - INTERVAL 24 HOUR AND {t:DateTime64(3)}""",
            {"stream": stream, "d": detector, "p": product_id, "t": t},
        )
        return {
            "base_rate": {k2: num(v) for k2, v in (base[0] if base else {}).items()},
            "recent": [{k2: num(v) for k2, v in r.items()} for r in rows],
        }

    # ------------------------------------------------------------------ trades

    def query_trades(self, venue: Venue, product_id: Product, start: str, end: str, stream: Stream,
                     limit: int = 200) -> dict:
        lo, hi = window(start, end)
        limit = max(1, min(limit, MAX_TRADE_ROWS))
        ns = NAMESPACE[stream]
        summary = self.trino.query(
            f"""SELECT count(*), sum(size), sum(price * size), min(price), max(price),
                       min_by(price, trade_id), max_by(price, trade_id),
                       sum(CASE WHEN taker_side = 'buy' THEN size ELSE 0 END),
                       count_if(source = 'rest'), min(trade_id), max(trade_id)
                FROM iceberg.{ns}.raw_trades
                WHERE venue = ? AND product_id = ?
                  AND event_ts >= from_iso8601_timestamp(?) AND event_ts < from_iso8601_timestamp(?)""",
            (venue, product_id, iso(lo), iso(hi)),
        )[0]
        n, vol, notional, low, high, first, last, buy, rest, min_id, max_id = summary
        rows = self.trino.query(
            f"""SELECT trade_id, event_ts, price, size, taker_side, source
                FROM iceberg.{ns}.raw_trades
                WHERE venue = ? AND product_id = ?
                  AND event_ts >= from_iso8601_timestamp(?) AND event_ts < from_iso8601_timestamp(?)
                ORDER BY event_ts, trade_id
                LIMIT {limit}""",
            (venue, product_id, iso(lo), iso(hi)),
        )
        mid = (low + high) / 2 if n else None
        return {
            "venue": venue, "product_id": product_id, "start": iso(lo), "end": iso(hi),
            "summary": {
                "trades": n,
                "volume": num(vol),
                "notional": num(notional),
                "vwap": num(notional / vol) if n and vol else None,
                "open": num(first), "close": num(last), "low": num(low), "high": num(high),
                "price_range_bps": float((high - low) / mid * 10_000) if n and mid else None,
                "buy_volume_share": float(buy / vol) if n and vol else None,
                "rest_backfilled_trades": rest,
                "trade_id_span": (max_id - min_id + 1) if n else 0,
            },
            "trades": [
                {"trade_id": r[0], "event_ts": num(r[1]), "price": num(r[2]), "size": num(r[3]),
                 "taker_side": r[4], "source": r[5]}
                for r in rows
            ],
            "truncated": n > len(rows),
        }

    def cross_venue_context(self, product_id: Product, start: str, end: str, stream: Stream,
                            bucket_seconds: int = 10) -> dict:
        lo, hi = window(start, end)
        bucket_seconds = max(1, min(bucket_seconds, 300))
        ns = NAMESPACE[stream]
        rows = self.trino.query(
            f"""WITH b AS (
                  SELECT venue,
                         from_unixtime(floor(to_unixtime(event_ts) / {bucket_seconds}) * {bucket_seconds}) AS bucket,
                         sum(price * size) / sum(size) AS vwap, sum(size) AS volume, count(*) AS trades
                  FROM iceberg.{ns}.raw_trades
                  WHERE product_id = ?
                    AND event_ts >= from_iso8601_timestamp(?) AND event_ts < from_iso8601_timestamp(?)
                  GROUP BY 1, 2)
                SELECT cb.bucket, cb.vwap, kr.vwap, cb.volume, kr.volume, cb.trades, kr.trades
                FROM b AS cb FULL OUTER JOIN b AS kr
                  ON cb.bucket = kr.bucket AND cb.venue = 'coinbase' AND kr.venue = 'kraken'
                WHERE cb.venue = 'coinbase' OR kr.venue = 'kraken'
                ORDER BY coalesce(cb.bucket, kr.bucket)
                LIMIT 400""",
            (product_id, iso(lo), iso(hi)),
        )
        out = []
        for bucket, cb, kr, cbv, krv, cbn, krn in rows:
            spread = None
            if cb is not None and kr is not None:
                spread = float((cb - kr) / ((cb + kr) / 2) * 10_000)
            out.append({"bucket": num(bucket), "coinbase_vwap": num(cb), "kraken_vwap": num(kr),
                        "spread_bps": spread, "coinbase_volume": num(cbv), "kraken_volume": num(krv),
                        "coinbase_trades": cbn, "kraken_trades": krn})
        spreads = [b["spread_bps"] for b in out if b["spread_bps"] is not None]
        return {
            "product_id": product_id, "bucket_seconds": bucket_seconds, "buckets": out,
            "spread_bps_summary": {
                "buckets_with_both_venues": len(spreads),
                "max_abs": max((abs(s) for s in spreads), default=None),
                "mean": sum(spreads) / len(spreads) if spreads else None,
            },
        }

    # ------------------------------------------------------------------ book

    def book_snapshot(self, product_id: Product, at: str, stream: Stream, depth: int = 10) -> dict:
        """Kraken book as of `at`: the nearest 10-second book checkpoint at or
        before `at` (written by the lakehouse job), plus the deltas between the
        checkpoint and `at`, truncated to the subscribed depth after each message."""
        t = parse_ts(at)
        depth = max(1, min(depth, 10))
        ns = NAMESPACE[stream]
        base = self.trino.query(
            f"""SELECT snapshot_ts, side, level, price, qty FROM iceberg.{ns}.book_snapshots
                WHERE venue = 'kraken' AND product_id = ?
                  AND snapshot_ts = (SELECT max(snapshot_ts) FROM iceberg.{ns}.book_snapshots
                                     WHERE venue = 'kraken' AND product_id = ?
                                       AND snapshot_ts BETWEEN from_iso8601_timestamp(?) - INTERVAL '2' MINUTE
                                                           AND from_iso8601_timestamp(?))
                ORDER BY side, level""",
            (product_id, product_id, iso(t), iso(t)),
        )
        if not base:
            return {"product_id": product_id, "as_of": iso(t), "complete": False,
                    "error": "no book checkpoint in the 2 minutes before this time"}
        checkpoint_ts = base[0][0]
        deltas = self.trino.query(
            f"""SELECT kafka_offset, side, level_index, price, qty, is_snapshot, event_ts
                FROM iceberg.{ns}.book_deltas
                WHERE venue = 'kraken' AND product_id = ?
                  AND event_ts > ? AND event_ts <= from_iso8601_timestamp(?)
                ORDER BY kafka_offset, side, level_index
                LIMIT {MAX_BOOK_ROWS}""",
            (product_id, checkpoint_ts, iso(t)),
        )
        seed = [(-1, side, lvl, price, qty, True, checkpoint_ts) for _ts, side, lvl, price, qty in base]
        out = rebuild_book(seed + deltas, depth, True, len(deltas) >= MAX_BOOK_ROWS, product_id, t)
        out["checkpoint_ts"] = num(checkpoint_ts)
        return out


def rebuild_book(rows: list[tuple], depth: int, has_snapshot: bool, truncated_input: bool,
                 product_id: str, t: datetime) -> dict:
    bids: dict[Decimal, Decimal] = {}
    asks: dict[Decimal, Decimal] = {}
    last_offset, last_ts, messages = None, None, 0

    def trim():
        for side, best_first in ((bids, True), (asks, False)):
            for p in sorted(side, reverse=best_first)[depth:]:
                del side[p]

    for offset, side, _idx, price, qty, is_snap, ts in rows:
        if offset != last_offset:
            if last_offset is not None:
                trim()
            if is_snap:
                bids.clear()
                asks.clear()
            last_offset, messages = offset, messages + 1
        book = bids if side == "bid" else asks
        price, qty = Decimal(str(price)), Decimal(str(qty))
        if qty == 0:
            book.pop(price, None)
        else:
            book[price] = qty
        last_ts = ts
    trim()
    best_bid = max(bids) if bids else None
    best_ask = min(asks) if asks else None
    crossed = best_bid is not None and best_ask is not None and best_bid >= best_ask
    out = {
        "product_id": product_id,
        "as_of": iso(t),
        "last_update": num(last_ts),
        "messages_applied": messages,
        "complete": has_snapshot and not truncated_input,
        "bids": [{"price": num(p), "qty": num(bids[p])} for p in sorted(bids, reverse=True)],
        "asks": [{"price": num(p), "qty": num(asks[p])} for p in sorted(asks)],
        "best_bid": num(best_bid),
        "best_ask": num(best_ask),
        "spread_bps": float((best_ask - best_bid) / ((best_ask + best_bid) / 2) * 10_000)
        if best_bid and best_ask else None,
        "bid_depth": num(sum(bids.values(), Decimal(0))),
        "ask_depth": num(sum(asks.values(), Decimal(0))),
    }
    if crossed:
        # A crossed rebuild means the inputs are inconsistent (a bad checkpoint or missed
        # deltas), not that the market crossed: say so instead of presenting it as fact.
        out["complete"] = False
        out["warning"] = "rebuilt book is crossed (best bid >= best ask): reconstruction unreliable at this time"
    return out

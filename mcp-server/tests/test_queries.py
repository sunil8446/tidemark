from datetime import datetime, timezone
from decimal import Decimal

import pytest

from tidemark_mcp.queries import Queries, QueryError, iso, rebuild_book, window


class FakeTrino:
    def __init__(self, results):
        self.results = list(results)
        self.calls = []

    def query(self, sql, params):
        self.calls.append((sql, params))
        return self.results.pop(0)


class FakeCH:
    def __init__(self, results):
        self.results = list(results)
        self.calls = []

    def query(self, sql, params):
        self.calls.append((sql, params))
        return self.results.pop(0)


def test_window_is_capped_and_ordered():
    window("2026-10-22T10:00:00Z", "2026-10-22T10:30:00Z")
    with pytest.raises(QueryError):
        window("2026-10-22T10:00:00Z", "2026-10-22T10:31:00Z")
    with pytest.raises(QueryError):
        window("2026-10-22T10:00:00Z", "2026-10-22T09:00:00Z")
    with pytest.raises(QueryError):
        window("yesterday", "today")


def test_alert_ids_are_validated_before_any_query():
    ch = FakeCH([])
    q = Queries(trino=FakeTrino([]), clickhouse=ch)
    with pytest.raises(QueryError):
        q.get_alert("x'; DROP TABLE alerts; --", "live")
    assert ch.calls == []


def test_values_are_bound_and_namespace_comes_from_whitelist():
    tr = FakeTrino([
        [(2, Decimal("0.3"), Decimal("30000"), Decimal("100000"), Decimal("100100"),
          Decimal("100000"), Decimal("100100"), Decimal("0.1"), 0, 10, 11)],
        [(10, datetime(2026, 10, 22, tzinfo=timezone.utc), Decimal("100000"), Decimal("0.1"), "buy", "ws"),
         (11, datetime(2026, 10, 22, tzinfo=timezone.utc), Decimal("100100"), Decimal("0.2"), "sell", "ws")],
    ])
    q = Queries(trino=tr, clickhouse=FakeCH([]))
    out = q.query_trades("kraken", "BTC-USD", "2026-10-22T10:00:00Z", "2026-10-22T10:05:00Z", "inj", limit=10_000)
    sql, params = tr.calls[1]
    assert "iceberg.market_inj.raw_trades" in sql
    assert "LIMIT 500" in sql  # clamped
    assert params == ("kraken", "BTC-USD", "2026-10-22T10:00:00Z", "2026-10-22T10:05:00Z")
    s = out["summary"]
    assert s["trades"] == 2 and s["trade_id_span"] == 2
    assert s["vwap"] == "100000"
    assert round(s["price_range_bps"], 2) == 10.0
    assert out["trades"][0]["price"] == "100000"


def test_rebuild_book_applies_snapshot_updates_and_truncation():
    t = datetime(2026, 10, 22, 10, tzinfo=timezone.utc)
    rows = [
        # snapshot (offset 1): bids 100, 99; asks 101
        (1, "bid", 0, Decimal("100"), Decimal("1"), True, t),
        (1, "bid", 1, Decimal("99"), Decimal("2"), True, t),
        (1, "ask", 0, Decimal("101"), Decimal("3"), True, t),
        # update (offset 2): new best bid 100.5 pushes 99 out of a depth-2 book
        (2, "bid", 0, Decimal("100.5"), Decimal("4"), False, t),
        # update (offset 3): 100.5 removed; the phantom 99 must not come back
        (3, "bid", 0, Decimal("100.5"), Decimal("0"), False, t),
    ]
    book = rebuild_book(rows, depth=2, has_snapshot=True, truncated_input=False, product_id="BTC-USD", t=t)
    assert [b["price"] for b in book["bids"]] == ["100"]
    assert book["best_bid"] == "100" and book["best_ask"] == "101"
    assert book["messages_applied"] == 3 and book["complete"]
    assert round(book["spread_bps"], 2) == round(1 / 100.5 * 10_000, 2)


def test_naive_datetimes_are_utc_not_local_time():
    # Regression: naive values from ClickHouse were converted as local time,
    # shifting evidence windows by the host's UTC offset.
    assert iso(datetime(2026, 10, 22, 9, 32, 40)) == "2026-10-22T09:32:40Z"


def test_book_snapshot_starts_from_checkpoint_and_applies_later_deltas():
    t0 = datetime(2026, 10, 22, 10, 0, 0, tzinfo=timezone.utc)
    tr = FakeTrino([
        [(t0, "ask", 1, Decimal("101"), Decimal("3")), (t0, "bid", 1, Decimal("100"), Decimal("1")),
         (t0, "bid", 2, Decimal("99"), Decimal("2"))],
        [(7, "bid", 0, Decimal("100"), Decimal("0"), False, t0)],  # best bid pulled after the checkpoint
    ])
    q = Queries(trino=tr, clickhouse=FakeCH([]))
    b = q.book_snapshot("BTC-USD", "2026-10-22T10:00:05Z", "live")
    assert b["best_bid"] == "99" and b["best_ask"] == "101" and b["complete"]
    assert b["checkpoint_ts"] == "2026-10-22T10:00:00Z"
    assert tr.calls[1][1][1] == t0  # deltas strictly after the checkpoint

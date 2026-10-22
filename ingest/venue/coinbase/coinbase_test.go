package coinbase

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	pb "tidemark/gen/go/tidemark/v1"
)

func TestParseMatchInvertsMakerSide(t *testing.T) {
	f := NewFeed("", []string{"BTC-USD"})
	// Live frame shape from 2026-10-22; side is the maker side.
	frame := `{"type":"match","trade_id":1109224114,"maker_order_id":"m","taker_order_id":"t","side":"sell","size":"0.00000024","price":"82198.23","product_id":"BTC-USD","sequence":138137972425,"time":"2026-10-22T07:33:36.823723Z"}`
	evs, err := f.Parse([]byte(frame), time.Now())
	if err != nil || len(evs) != 1 || evs[0].Trade == nil {
		t.Fatalf("evs=%v err=%v", evs, err)
	}
	tr := evs[0].Trade
	if tr.TakerSide != pb.Side_SIDE_BUY || tr.TradeId != 1109224114 || tr.Price != "82198.23" {
		t.Fatalf("trade %+v", tr)
	}
	if tr.EventTsNs != time.Date(2026, 10, 22, 7, 33, 36, 823723000, time.UTC).UnixNano() {
		t.Fatalf("event ts %d", tr.EventTsNs)
	}
}

func TestParseHeartbeatAndControl(t *testing.T) {
	f := NewFeed("", nil)
	evs, _ := f.Parse([]byte(`{"type":"heartbeat","last_trade_id":42,"product_id":"ETH-USD","sequence":1,"time":"2026-10-22T07:33:37Z"}`), time.Now())
	if len(evs) != 1 || evs[0].Heartbeat == nil || evs[0].Heartbeat.LastTradeID != 42 {
		t.Fatalf("heartbeat evs=%v", evs)
	}
	if evs, _ := f.Parse([]byte(`{"type":"subscriptions","channels":[]}`), time.Now()); len(evs) != 0 {
		t.Fatal("subscriptions should produce no events")
	}
	if _, err := f.Parse([]byte(`{"type":"error","message":"bad","reason":"x"}`), time.Now()); err == nil {
		t.Fatal("error message should return an error")
	}
}

func TestSubscribeMessage(t *testing.T) {
	var m map[string]any
	json.Unmarshal(NewFeed("", []string{"BTC-USD"}).SubscribeMessages()[0], &m)
	if m["type"] != "subscribe" || fmt.Sprint(m["channels"]) != "[matches heartbeat]" {
		t.Fatalf("subscribe %v", m)
	}
}

// fakeCoinbase serves trade ids 1..n newest first, 4 per page, honouring
// `after` (return ids < after) and setting CB-AFTER to the oldest id returned.
func fakeCoinbase(n uint64, absent map[uint64]bool) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		limit = min(limit, 4)
		after := n + 1
		if a := r.URL.Query().Get("after"); a != "" {
			after, _ = strconv.ParseUint(a, 10, 64)
		}
		var rows []map[string]any
		for id := after - 1; id >= 1 && len(rows) < limit; id-- {
			if absent[id] {
				continue
			}
			rows = append(rows, map[string]any{"trade_id": id, "side": "buy", "size": "1", "price": "2", "time": "2026-10-22T00:00:00Z"})
		}
		if len(rows) > 0 {
			w.Header().Set("CB-AFTER", fmt.Sprint(rows[len(rows)-1]["trade_id"]))
		}
		json.NewEncoder(w).Encode(rows)
	}))
}

func TestBackfillFetchesExactRange(t *testing.T) {
	srv := fakeCoinbase(30, nil)
	defer srv.Close()
	b := NewBackfiller(srv.URL)
	b.limiter = time.NewTicker(time.Millisecond)
	got, max, err := b.Fetch(context.Background(), "BTC-USD", 10, 20, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 11 || max != 30 {
		t.Fatalf("got %d trades, sourceMax %d", len(got), max)
	}
	seen := map[uint64]bool{}
	for _, tr := range got {
		if tr.TradeId < 10 || tr.TradeId > 20 || seen[tr.TradeId] {
			t.Fatalf("bad id %d", tr.TradeId)
		}
		seen[tr.TradeId] = true
		if tr.TakerSide != pb.Side_SIDE_SELL || tr.Source != pb.Source_SOURCE_REST {
			t.Fatalf("trade %+v", tr)
		}
	}
}

func TestBackfillReportsAbsentIDs(t *testing.T) {
	srv := fakeCoinbase(30, map[uint64]bool{15: true})
	defer srv.Close()
	b := NewBackfiller(srv.URL)
	b.limiter = time.NewTicker(time.Millisecond)
	got, max, _ := b.Fetch(context.Background(), "BTC-USD", 10, 20, time.Time{})
	if len(got) != 10 || max != 30 {
		t.Fatalf("got %d, max %d", len(got), max)
	}
}

package kraken

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	pb "tidemark/gen/go/tidemark/v1"
)

// Shape copied from a live response on 2026-10-22.
const liveTrades = `{"error":[],"result":{"BTC/USD":[["82184.10000","0.00058374",1792654390.913895,"b","l","",112495520],["82184.00000","0.06175168",1792654395.870135,"s","l","",112495521]],"last":"1792654396383435100"}}`

func TestParseTrades(t *testing.T) {
	trades, next, err := parseTrades([]byte(liveTrades))
	if err != nil {
		t.Fatal(err)
	}
	if next != "1792654396383435100" || len(trades) != 2 {
		t.Fatalf("next=%q n=%d", next, len(trades))
	}
	tr := trades[0]
	if tr.TradeId != 112495520 || tr.Price != "82184.10000" || tr.Size != "0.00058374" ||
		tr.TakerSide != pb.Side_SIDE_BUY || tr.EventTsNs != 1792654390913895000 || tr.Source != pb.Source_SOURCE_REST {
		t.Fatalf("unexpected trade %+v", tr)
	}
	if trades[1].TakerSide != pb.Side_SIDE_SELL {
		t.Fatal("side b/s mapping")
	}
}

// fakeKraken serves trades 1..n, 3 per page, ignoring ids in `absent`.
func fakeKraken(t *testing.T, n uint64, absent map[uint64]bool) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		since, _ := strconv.ParseInt(r.URL.Query().Get("since"), 10, 64)
		// Trade i happens at t = i seconds; since is in ns.
		var rows string
		var last int64 = since
		count := 0
		for i := uint64(1); i <= n && count < 3; i++ {
			ts := int64(i) * int64(time.Second)
			if ts <= since || absent[i] {
				continue
			}
			if rows != "" {
				rows += ","
			}
			rows += fmt.Sprintf(`["1.0","2.0",%d.000000,"b","l","",%d]`, i, i)
			last = ts
			count++
		}
		fmt.Fprintf(w, `{"error":[],"result":{"BTC/USD":[%s],"last":"%d"}}`, rows, last)
	}))
}

func TestBackfillPagesUntilRangeCovered(t *testing.T) {
	srv := fakeKraken(t, 20, nil)
	defer srv.Close()
	b := NewBackfiller(srv.URL)
	b.limiter = time.NewTicker(time.Millisecond)
	got, max, err := b.Fetch(context.Background(), "BTC-USD", 5, 11, time.Unix(4, 0))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 7 || got[0].TradeId != 5 || got[6].TradeId != 11 || max < 11 {
		t.Fatalf("got %d trades, max %d", len(got), max)
	}
	if got[0].ProductId != "BTC-USD" {
		t.Fatalf("product %q", got[0].ProductId)
	}
}

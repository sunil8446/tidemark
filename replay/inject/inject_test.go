package inject

import (
	"context"
	"testing"
	"time"

	pb "tidemark/gen/go/tidemark/v1"
	"tidemark/ingest/sink"
)

type labels struct{ got []*pb.InjectionLabel }

func (l *labels) Label(x *pb.InjectionLabel) { l.got = append(l.got, x) }

func TestPlanIsDeterministicSpacedAndBalanced(t *testing.T) {
	start := time.Unix(1_800_000_000, 0)
	end := start.Add(2 * time.Hour)
	a := Plan(7, start, end, 10*time.Minute, 90*time.Second, 5, []string{"BTC-USD"})
	b := Plan(7, start, end, 10*time.Minute, 90*time.Second, 5, []string{"BTC-USD"})
	if len(a) != 20 || len(b) != 20 {
		t.Fatalf("len %d %d", len(a), len(b))
	}
	count := map[string]int{}
	for i := range a {
		if a[i].ID != b[i].ID || !a[i].At.Equal(b[i].At) {
			t.Fatal("same seed must give the same plan")
		}
		if a[i].At.Before(start.Add(10 * time.Minute)) {
			t.Fatal("injection inside warm-up")
		}
		if i > 0 && a[i].At.Sub(a[i-1].At) < 90*time.Second/2 {
			t.Fatalf("injections %d and %d too close", i-1, i)
		}
		count[a[i].Pattern]++
	}
	for _, p := range []string{"volume_spike", "spoof_layering", "dislocation", "wash_trade"} {
		if count[p] != 5 {
			t.Fatalf("%s: %d", p, count[p])
		}
	}
}

func trade(id uint64, ts time.Time, price string) *pb.Trade {
	return &pb.Trade{Venue: pb.Venue_VENUE_COINBASE, ProductId: "BTC-USD", TradeId: id, Price: price, Size: "0.01",
		EventTsNs: ts.UnixNano()}
}

func TestInjectorAssignsAccountsAndReleasesInEventTimeOrder(t *testing.T) {
	base := time.Unix(1_800_000_000, 0)
	plan := []Injection{
		{ID: "w1", Pattern: "wash_trade", Venue: pb.Venue_VENUE_COINBASE, Product: "BTC-USD", At: base.Add(30 * time.Second),
			Params: map[string]float64{"round_trip": 1, "gap_ms": 5000, "size_jitter": 0.01}},
		{ID: "d1", Pattern: "dislocation", Product: "BTC-USD", At: base.Add(40 * time.Second),
			Params: map[string]float64{"bps": 100, "seconds": 5}},
	}
	mem := &sink.Memory{}
	lb := &labels{}
	in := New(mem, lb, plan, 100)
	for i := 0; i < 60; i++ {
		in.Trade(trade(uint64(i+1), base.Add(time.Duration(i)*time.Second), "100.00"))
	}
	in.Flush(context.Background())

	trades, _, _ := mem.Snapshot()
	var last int64
	var washLegs, shifted int
	for _, tr := range trades {
		if tr.BuyerAccount == "" || tr.SellerAccount == "" {
			t.Fatalf("trade %d has no accounts", tr.TradeId)
		}
		if tr.IngestId != "inject" && tr.BuyerAccount == tr.SellerAccount {
			t.Fatalf("background trade %d is a self trade", tr.TradeId)
		}
		if tr.EventTsNs < last {
			t.Fatalf("out of event-time order at trade %d", tr.TradeId)
		}
		last = tr.EventTsNs
		if tr.IngestId == "inject" {
			washLegs++
		}
		if tr.Price == "101.00" {
			shifted++
		}
	}
	if washLegs != 2 {
		t.Fatalf("wash legs %d", washLegs)
	}
	if shifted != 6 { // trades at 40..45 s inclusive, shifted +100 bps
		t.Fatalf("shifted trades %d", shifted)
	}
	if len(lb.got) != 2 || lb.got[0].Pattern != "wash_trade" || lb.got[0].EndTsNs-lb.got[0].StartTsNs != int64(5*time.Second) {
		t.Fatalf("labels %+v", lb.got)
	}
}

func TestSpoofInjectionNeedsABookAndRestoresTheLevel(t *testing.T) {
	base := time.Unix(1_800_000_000, 0)
	plan := []Injection{{ID: "s1", Pattern: "spoof_layering", Venue: pb.Venue_VENUE_KRAKEN, Product: "BTC-USD",
		At: base.Add(10 * time.Second), Params: map[string]float64{"depth_share": 2, "life_ms": 2000, "rank": 2, "ask": 0}}}
	mem := &sink.Memory{}
	in := New(mem, &labels{}, plan, 100)
	in.Book(&pb.BookUpdate{Venue: pb.Venue_VENUE_KRAKEN, ProductId: "BTC-USD", IsSnapshot: true, EventTsNs: base.UnixNano(),
		Bids: []*pb.BookLevel{{Price: "100.0", Qty: "1.0"}, {Price: "99.9", Qty: "2.0"}, {Price: "99.8", Qty: "3.0"}},
		Asks: []*pb.BookLevel{{Price: "100.1", Qty: "1.0"}}})
	for i := 1; i <= 20; i++ {
		ts := base.Add(time.Duration(i) * time.Second).UnixNano()
		u := &pb.BookUpdate{Venue: pb.Venue_VENUE_KRAKEN, ProductId: "BTC-USD", EventTsNs: ts}
		if i == 11 { // a real update to the spoofed level while the synthetic order rests
			u.Bids = []*pb.BookLevel{{Price: "99.9", Qty: "2.5"}}
		}
		in.Book(u)
	}
	_, books, _ := mem.Snapshot()
	var up, down, during *pb.BookUpdate
	for _, b := range books {
		if b.Source == pb.Source_SOURCE_REPLAY && len(b.Bids) == 1 {
			if up == nil {
				up = b
			} else {
				down = b
			}
		} else if len(b.Bids) == 1 {
			during = b
		}
	}
	if during == nil || during.Bids[0].Qty != "10.50000000" {
		t.Fatalf("real update during the spoof must carry the synthetic size: %v", during)
	}
	if up == nil || down == nil {
		t.Fatal("expected an add and a pull")
	}
	// Rank 2 is 99.9 (2.0); the other levels hold 1.0 + 3.0, so a 2x depth share adds 8.
	if up.Bids[0].Price != "99.9" || up.Bids[0].Qty != "10.00000000" || down.Bids[0].Qty != "2.5" {
		t.Fatalf("up %v down %v", up.Bids, down.Bids)
	}
	if down.EventTsNs-up.EventTsNs != int64(2*time.Second) {
		t.Fatal("pull should follow after life_ms")
	}
}

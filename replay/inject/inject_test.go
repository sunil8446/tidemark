package inject

import (
	"context"
	"strconv"
	"strings"
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

func TestVolumeSpikeTradesBlendIntoTheTape(t *testing.T) {
	// Injected trades must not be identifiable by artefacts: ids continue the venue's sequence,
	// sizes come from the real tape, timing is not a fixed clock. (An investigator agent spotted
	// the old injections by ids from 2^62, identical sizes and a 0.5 s cadence.)
	base := time.Unix(1_800_000_000, 0)
	plan := []Injection{{ID: "v1", Pattern: "volume_spike", Venue: pb.Venue_VENUE_COINBASE, Product: "BTC-USD",
		At: base.Add(400 * time.Second), Params: map[string]float64{"multiple": 20}}}
	mem := &sink.Memory{}
	in := New(mem, &labels{}, plan, 100)
	sizes := []string{"0.01", "0.25", "0.003", "1.2", "0.07"}
	for i := 0; i < 600; i++ {
		tr := trade(uint64(5_000_000+i), base.Add(time.Duration(i)*time.Second), "100.00")
		tr.Size = sizes[i%len(sizes)]
		tr.Source = pb.Source_SOURCE_WS
		in.Trade(tr)
	}
	// Kraken-style resend of an earlier real trade must keep its id, so dedup still drops it.
	in.Trade(trade(5_000_010, base.Add(599*time.Second), "100.00"))
	in.Flush(context.Background())

	trades, _, _ := mem.Snapshot()
	ids := map[uint64]int{}
	var synth []*pb.Trade
	for _, tr := range trades {
		ids[tr.TradeId]++
		if tr.IngestId == "inject" {
			synth = append(synth, tr)
		}
	}
	if len(synth) < 12 || len(synth) > 30 {
		t.Fatalf("synthetic trades %d", len(synth))
	}
	distinctSizes, gaps := map[string]bool{}, map[int64]bool{}
	for i, tr := range synth {
		if tr.TradeId < 5_000_000 || tr.TradeId > 5_000_000+700 {
			t.Fatalf("synthetic id %d outside the real sequence", tr.TradeId)
		}
		if ids[tr.TradeId] != 1 {
			t.Fatalf("synthetic id %d collides", tr.TradeId)
		}
		distinctSizes[tr.Size] = true
		if tr.Source != pb.Source_SOURCE_WS {
			t.Fatalf("synthetic trade source %v differs from the tape", tr.Source)
		}
		if i > 0 {
			gaps[(tr.EventTsNs-synth[i-1].EventTsNs)/int64(time.Millisecond)] = true
		}
	}
	if len(distinctSizes) < 3 || len(gaps) < 5 {
		t.Fatalf("sizes %d distinct, gaps %d distinct: still looks synthetic", len(distinctSizes), len(gaps))
	}
	// Ids are one gapless sequence; the resent real trade reuses its id (the only duplicate).
	dups := 0
	for _, n := range ids {
		if n > 1 {
			dups++
		}
	}
	if dups != 1 || len(ids) != 600+len(synth) {
		t.Fatalf("dups %d, distinct ids %d, want 1 and %d", dups, len(ids), 600+len(synth))
	}
}

func TestInjectedSizesAreDistinctAndFormattedLikeTheTape(t *testing.T) {
	// Sizes used to be drawn with replacement and scaled by one factor, so exact sizes repeated
	// within a burst and echoed earlier real trades; every size was also printed with 8
	// significant decimals. Each synthetic size must be unique, never copy a real size, and keep
	// the venue's formatting (Kraken pads to 8 decimals; Coinbase prints what is significant).
	base := time.Unix(1_800_000_000, 0)
	for _, tc := range []struct {
		venue pb.Venue
		sizes []string
	}{
		{pb.Venue_VENUE_KRAKEN, []string{"0.01000000", "0.25000000", "0.00300000", "1.20000000", "0.07000000"}},
		{pb.Venue_VENUE_COINBASE, []string{"0.01", "0.25", "0.003", "1.2", "0.07"}},
	} {
		plan := []Injection{{ID: "v1", Pattern: "volume_spike", Venue: tc.venue, Product: "BTC-USD",
			At: base.Add(400 * time.Second), Params: map[string]float64{"multiple": 20}}}
		mem := &sink.Memory{}
		in := New(mem, &labels{}, plan, 100)
		real := map[string]bool{}
		for i := 0; i < 600; i++ {
			tr := trade(uint64(5_000_000+i), base.Add(time.Duration(i)*time.Second), "100.00")
			tr.Venue = tc.venue
			tr.Size = tc.sizes[i%len(tc.sizes)]
			real[tr.Size] = true
			in.Trade(tr)
		}
		in.Flush(context.Background())
		trades, _, _ := mem.Snapshot()
		seen := map[string]bool{}
		n := 0
		for _, tr := range trades {
			if tr.IngestId != "inject" {
				continue
			}
			n++
			if seen[tr.Size] || real[tr.Size] {
				t.Fatalf("%v: size %s repeats a synthetic or real size", tc.venue, tr.Size)
			}
			seen[tr.Size] = true
			dec := len(tr.Size) - strings.IndexByte(tr.Size, '.') - 1
			if tc.venue == pb.Venue_VENUE_KRAKEN && dec != 8 {
				t.Fatalf("kraken size %s is not padded to 8 decimals", tr.Size)
			}
			if tc.venue == pb.Venue_VENUE_COINBASE && dec > 5 {
				t.Fatalf("coinbase size %s has %d decimals; the tape uses at most 3", tr.Size, dec)
			}
			if v, _ := strconv.ParseFloat(tr.Size, 64); v <= 0 {
				t.Fatalf("size %s is not positive", tr.Size)
			}
		}
		if n < 12 {
			t.Fatalf("%v: %d synthetic trades", tc.venue, n)
		}
	}
}

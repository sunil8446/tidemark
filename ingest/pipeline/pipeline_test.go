package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	pb "tidemark/gen/go/tidemark/v1"
	"tidemark/ingest/metrics"
	"tidemark/ingest/sink"
	"tidemark/ingest/venue"
)

// fakeFeed parses frames like {"id":5} into a BTC-USD trade, and {"hb":9} into a heartbeat.
type fakeFeed struct{}

func (fakeFeed) Venue() pb.Venue             { return pb.Venue_VENUE_COINBASE }
func (fakeFeed) URL() string                 { return "" }
func (fakeFeed) SubscribeMessages() [][]byte { return nil }
func (fakeFeed) Reset()                      {}
func (fakeFeed) Parse(b []byte, _ time.Time) ([]venue.Event, error) {
	var m struct{ ID, HB uint64 }
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	if m.HB != 0 {
		return []venue.Event{{Heartbeat: &venue.Heartbeat{Product: "BTC-USD", LastTradeID: m.HB}}}, nil
	}
	return []venue.Event{{Trade: &pb.Trade{Venue: pb.Venue_VENUE_COINBASE, ProductId: "BTC-USD", TradeId: m.ID, EventTsNs: int64(m.ID)}}}, nil
}

// fakeREST returns ids in [first,last] except `absent`, after `lagTries` calls
// that return nothing (simulating REST indexing lag).
type fakeREST struct {
	mu       sync.Mutex
	absent   map[uint64]bool
	lagTries int
	calls    int
	max      uint64
	err      error
}

func (f *fakeREST) Fetch(_ context.Context, product string, first, last uint64, _ time.Time) ([]*pb.Trade, uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return nil, 0, f.err
	}
	if f.calls <= f.lagTries {
		return nil, first - 1, nil
	}
	var out []*pb.Trade
	for id := first; id <= last; id++ {
		if !f.absent[id] {
			out = append(out, &pb.Trade{Venue: pb.Venue_VENUE_COINBASE, ProductId: product, TradeId: id})
		}
	}
	return out, max(f.max, last), nil
}

func frame(id uint64) []byte { b, _ := json.Marshal(map[string]uint64{"id": id}); return b }

func newTestPipeline(rest venue.Backfiller) (*Pipeline, *sink.Memory) {
	mem := &sink.Memory{}
	p := New(fakeFeed{}, mem, metrics.New(), Options{
		TrackGaps: true, Backfiller: rest, BackfillGrace: time.Millisecond, BackfillTries: 3,
		BackfillRetryDelay: 10 * time.Millisecond,
	})
	return p, mem
}

func waitGap(t *testing.T, mem *sink.Memory, status pb.GapStatus) *pb.GapEvent {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		_, _, gaps := mem.Snapshot()
		for _, g := range gaps {
			if g.Status == status {
				return g
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no gap event with status %v", status)
	return nil
}

func tradeIDs(mem *sink.Memory) map[uint64]int {
	trades, _, _ := mem.Snapshot()
	ids := map[uint64]int{}
	for _, tr := range trades {
		ids[tr.TradeId]++
	}
	return ids
}

func TestGapIsBackfilledExactlyOnce(t *testing.T) {
	p, mem := newTestPipeline(&fakeREST{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.RunBackfill(ctx, 1)

	for _, id := range []uint64{1, 2, 3, 8, 8, 9} { // 4..7 missing, 8 duplicated
		p.HandleFrame(frame(id), time.Now())
	}
	p.HandleFrame(frame(5), time.Now()) // late WS arrival inside the gap

	g := waitGap(t, mem, pb.GapStatus_GAP_STATUS_RECOVERED)
	if g.FirstMissingTradeId != 4 || g.LastMissingTradeId != 7 || g.RecoveredCount != 4 {
		t.Fatalf("gap %+v", g)
	}
	ids := tradeIDs(mem)
	for id := uint64(1); id <= 9; id++ {
		if ids[id] != 1 {
			t.Fatalf("trade %d produced %d times; all: %v", id, ids[id], ids)
		}
	}
	trades, _, _ := mem.Snapshot()
	rest := 0
	for _, tr := range trades {
		if tr.Source == pb.Source_SOURCE_REST {
			rest++
		}
	}
	if rest != 3 { // 4, 6, 7 — 5 arrived late over WS
		t.Fatalf("rest-sourced trades = %d, want 3", rest)
	}
}

func TestRetriesThroughRESTLag(t *testing.T) {
	rest := &fakeREST{lagTries: 2}
	p, mem := newTestPipeline(rest)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.RunBackfill(ctx, 1)
	for _, id := range []uint64{1, 4} {
		p.HandleFrame(frame(id), time.Now())
	}
	waitGap(t, mem, pb.GapStatus_GAP_STATUS_RECOVERED)
	if rest.calls != 3 {
		t.Fatalf("calls = %d, want 3", rest.calls)
	}
}

func TestAbsentAtSource(t *testing.T) {
	p, mem := newTestPipeline(&fakeREST{absent: map[uint64]bool{3: true}})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.RunBackfill(ctx, 1)
	for _, id := range []uint64{1, 5} {
		p.HandleFrame(frame(id), time.Now())
	}
	g := waitGap(t, mem, pb.GapStatus_GAP_STATUS_ABSENT_AT_SOURCE)
	if g.RecoveredCount != 2 {
		t.Fatalf("recovered %d want 2", g.RecoveredCount)
	}
}

func TestUnrecoverableAfterRetries(t *testing.T) {
	p, mem := newTestPipeline(&fakeREST{err: errors.New("503")})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.RunBackfill(ctx, 1)
	for _, id := range []uint64{1, 3} {
		p.HandleFrame(frame(id), time.Now())
	}
	g := waitGap(t, mem, pb.GapStatus_GAP_STATUS_UNRECOVERABLE)
	if g.Detail != "503" {
		t.Fatalf("detail %q", g.Detail)
	}
}

func TestHeartbeatGapRecoversWithoutProducingTwice(t *testing.T) {
	p, mem := newTestPipeline(&fakeREST{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.RunBackfill(ctx, 1)
	p.HandleFrame(frame(10), time.Now())
	p.HandleFrame([]byte(`{"hb":12}`), time.Now())
	waitGap(t, mem, pb.GapStatus_GAP_STATUS_RECOVERED)
	p.HandleFrame(frame(11), time.Now()) // arrives after backfill: duplicate
	p.HandleFrame(frame(13), time.Now())
	ids := tradeIDs(mem)
	for id := uint64(10); id <= 13; id++ {
		if ids[id] != 1 {
			t.Fatalf("trade %d produced %d times", id, ids[id])
		}
	}
}

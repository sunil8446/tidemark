package gaps

import (
	"testing"
	"time"

	pb "tidemark/gen/go/tidemark/v1"
)

type step struct {
	id       uint64
	accept   bool
	gapFirst uint64 // 0 = no gap expected
	gapLast  uint64
}

func run(t *testing.T, tr *Tracker, steps []step) []*Gap {
	t.Helper()
	var opened []*Gap
	for i, s := range steps {
		accept, g := tr.Observe("BTC-USD", s.id, time.Unix(int64(s.id), 0))
		if accept != s.accept {
			t.Fatalf("step %d id=%d: accept=%v want %v", i, s.id, accept, s.accept)
		}
		switch {
		case s.gapFirst == 0 && g != nil:
			t.Fatalf("step %d id=%d: unexpected gap %+v", i, s.id, g)
		case s.gapFirst != 0 && (g == nil || g.First != s.gapFirst || g.Last != s.gapLast):
			t.Fatalf("step %d id=%d: gap %+v want [%d,%d]", i, s.id, g, s.gapFirst, s.gapLast)
		}
		if g != nil {
			opened = append(opened, g)
		}
	}
	return opened
}

func TestContiguousSequenceHasNoGaps(t *testing.T) {
	tr := NewTracker(pb.Venue_VENUE_COINBASE)
	var steps []step
	for id := uint64(100); id < 200; id++ {
		steps = append(steps, step{id: id, accept: true})
	}
	run(t, tr, steps)
	if tr.OpenGaps() != 0 {
		t.Fatal("no gaps expected")
	}
}

func TestJumpDuplicateAndLateFill(t *testing.T) {
	tr := NewTracker(pb.Venue_VENUE_KRAKEN)
	gs := run(t, tr, []step{
		{id: 10, accept: true},                            // baseline
		{id: 11, accept: true},                            //
		{id: 15, accept: true, gapFirst: 12, gapLast: 14}, // jump
		{id: 15, accept: false},                           // duplicate of last
		{id: 11, accept: false},                           // old duplicate
		{id: 13, accept: true},                            // late arrival inside the gap
		{id: 13, accept: false},                           // ...only once
		{id: 16, accept: true},
	})
	g := gs[0]
	if g.Kind != pb.GapKind_GAP_KIND_TRADE_ID_JUMP || g.PrevTradeTs != time.Unix(11, 0) {
		t.Fatalf("gap %+v", g)
	}
	if r := tr.Remaining("BTC-USD", g.ID); r != 2 {
		t.Fatalf("remaining %d want 2", r)
	}
	fresh := tr.Claim("BTC-USD", g.ID, []uint64{12, 13, 14, 99})
	if !fresh[0] || fresh[1] || !fresh[2] || fresh[3] {
		t.Fatalf("claim %v want [true false true false]", fresh)
	}
	if tr.Remaining("BTC-USD", g.ID) != 0 {
		t.Fatal("gap should be fully filled")
	}
	if n := tr.Close("BTC-USD", g.ID); n != 3 {
		t.Fatalf("filled %d want 3", n)
	}
	if tr.OpenGaps() != 0 {
		t.Fatal("gap should be closed")
	}
}

func TestReconnectInsideGap(t *testing.T) {
	// A reconnect lands mid-stream: the venue resends trades we have (dups),
	// then continues past a range we never saw. Only the unseen range is a gap.
	tr := NewTracker(pb.Venue_VENUE_KRAKEN)
	run(t, tr, []step{
		{id: 1, accept: true}, {id: 2, accept: true}, {id: 3, accept: true},
		// connection drops; snapshot on resubscribe replays 2..3 then jumps
		{id: 2, accept: false}, {id: 3, accept: false},
		{id: 7, accept: true, gapFirst: 4, gapLast: 6},
		{id: 8, accept: true},
	})
}

func TestHeartbeatAheadOpensTailGap(t *testing.T) {
	tr := NewTracker(pb.Venue_VENUE_COINBASE)
	if g := tr.ObserveHeartbeat("BTC-USD", 50); g != nil {
		t.Fatal("heartbeat before any trade must not open a gap")
	}
	run(t, tr, []step{{id: 50, accept: true}})
	if g := tr.ObserveHeartbeat("BTC-USD", 50); g != nil {
		t.Fatal("heartbeat at last id is not a gap")
	}
	g := tr.ObserveHeartbeat("BTC-USD", 53)
	if g == nil || g.First != 51 || g.Last != 53 || g.Kind != pb.GapKind_GAP_KIND_HEARTBEAT_AHEAD {
		t.Fatalf("gap %+v", g)
	}
	// The trades then arrive late: they fill the gap instead of being dropped.
	run(t, tr, []step{{id: 51, accept: true}, {id: 52, accept: true}, {id: 53, accept: true}, {id: 54, accept: true}})
	if tr.Remaining("BTC-USD", g.ID) != 0 {
		t.Fatal("late trades should fill the heartbeat gap")
	}
}

func TestProductsAreIndependent(t *testing.T) {
	tr := NewTracker(pb.Venue_VENUE_COINBASE)
	tr.Observe("BTC-USD", 100, time.Time{})
	if _, g := tr.Observe("ETH-USD", 5, time.Time{}); g != nil {
		t.Fatal("first id of another product is a baseline, not a gap")
	}
	if _, g := tr.Observe("BTC-USD", 101, time.Time{}); g != nil {
		t.Fatal("unexpected gap")
	}
}

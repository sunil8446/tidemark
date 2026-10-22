// Package gaps tracks the last trade_id seen per product and turns forward
// jumps into gap records that a backfill worker can repair.
//
// Rules for an observed trade_id `id` against the last accepted `last`:
//
//	first id for a product   baseline, accepted, no gap
//	id == last+1             accepted
//	id >  last+1             gap [last+1, id-1] opened, id accepted
//	id <= last, inside an open gap and not yet seen   accepted (late arrival)
//	id <= last otherwise     duplicate, dropped
//
// Open gaps remember which ids have been filled, by late WebSocket arrivals or
// by backfill, so nothing inside a gap is produced twice by this ingestor.
// (Downstream Flink still dedups on (venue, product, trade_id).)
package gaps

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	pb "tidemark/gen/go/tidemark/v1"
)

// MaxSpan caps how many ids a single gap may cover before it is treated as
// unrecoverable instead of being backfilled id by id.
const MaxSpan = 200_000

// Gap is an open range of missing trade ids for one product.
type Gap struct {
	ID          string
	Venue       pb.Venue
	Product     string
	Kind        pb.GapKind
	First, Last uint64    // inclusive
	DetectedAt  time.Time //
	PrevTradeTs time.Time // event time of the last trade before the gap (REST cursor hint)
}

// Span is the number of ids in the gap.
func (g Gap) Span() uint64 { return g.Last - g.First + 1 }

type openGap struct {
	Gap
	seen map[uint64]struct{}
}

type product struct {
	last   uint64
	lastTs time.Time
	open   map[string]*openGap
}

type Tracker struct {
	mu       sync.Mutex
	venue    pb.Venue
	products map[string]*product
	seq      atomic.Uint64
	now      func() time.Time
}

func NewTracker(v pb.Venue) *Tracker {
	return &Tracker{venue: v, products: map[string]*product{}, now: time.Now}
}

func (t *Tracker) get(p string) *product {
	st, ok := t.products[p]
	if !ok {
		st = &product{open: map[string]*openGap{}}
		t.products[p] = st
	}
	return st
}

// Seed sets the resume position for a product, e.g. from the tail of the
// trades topic after a restart. It never moves a product backwards.
func (t *Tracker) Seed(p string, lastID uint64, lastTs time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	st := t.get(p)
	if lastID > st.last {
		st.last, st.lastTs = lastID, lastTs
	}
}

// Observe records a trade id. It reports whether the trade should be produced
// and, if the id jumped forward, the newly opened gap.
func (t *Tracker) Observe(p string, id uint64, ts time.Time) (accept bool, gap *Gap) {
	t.mu.Lock()
	defer t.mu.Unlock()
	st := t.get(p)
	switch {
	case st.last == 0:
		st.last, st.lastTs = id, ts
		return true, nil
	case id == st.last+1:
		st.last, st.lastTs = id, ts
		return true, nil
	case id > st.last+1:
		g := t.open(st, p, pb.GapKind_GAP_KIND_TRADE_ID_JUMP, st.last+1, id-1)
		st.last, st.lastTs = id, ts
		return true, g
	default:
		return t.fill(st, id), nil
	}
}

// ObserveHeartbeat opens a gap when the venue says trades exist beyond the
// last one we received. If those trades then arrive late they fill the gap.
func (t *Tracker) ObserveHeartbeat(p string, lastTradeID uint64) *Gap {
	t.mu.Lock()
	defer t.mu.Unlock()
	st := t.get(p)
	if st.last == 0 || lastTradeID <= st.last {
		return nil
	}
	g := t.open(st, p, pb.GapKind_GAP_KIND_HEARTBEAT_AHEAD, st.last+1, lastTradeID)
	st.last = lastTradeID
	return g
}

func (t *Tracker) open(st *product, p string, kind pb.GapKind, first, last uint64) *Gap {
	g := &openGap{
		Gap: Gap{
			ID:          fmt.Sprintf("%s-%s-%d-%d", shortVenue(t.venue), p, first, t.seq.Add(1)),
			Venue:       t.venue,
			Product:     p,
			Kind:        kind,
			First:       first,
			Last:        last,
			DetectedAt:  t.now(),
			PrevTradeTs: st.lastTs,
		},
		seen: map[uint64]struct{}{},
	}
	st.open[g.ID] = g
	out := g.Gap
	return &out
}

func (t *Tracker) fill(st *product, id uint64) bool {
	for _, g := range st.open {
		if id >= g.First && id <= g.Last {
			if _, dup := g.seen[id]; dup {
				return false
			}
			g.seen[id] = struct{}{}
			return true
		}
	}
	return false
}

// Claim marks ids inside a gap as filled and returns, for each id, whether
// this call filled it (so the caller should produce that trade).
func (t *Tracker) Claim(p, gapID string, ids []uint64) []bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]bool, len(ids))
	g, ok := t.get(p).open[gapID]
	if !ok {
		return out
	}
	for i, id := range ids {
		if id < g.First || id > g.Last {
			continue
		}
		if _, dup := g.seen[id]; !dup {
			g.seen[id] = struct{}{}
			out[i] = true
		}
	}
	return out
}

// Remaining is how many ids in the gap are still unfilled.
func (t *Tracker) Remaining(p, gapID string) uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	g, ok := t.get(p).open[gapID]
	if !ok {
		return 0
	}
	return g.Span() - uint64(len(g.seen))
}

// Close forgets the gap and returns how many of its ids were filled.
func (t *Tracker) Close(p, gapID string) uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	st := t.get(p)
	g, ok := st.open[gapID]
	if !ok {
		return 0
	}
	delete(st.open, gapID)
	return uint64(len(g.seen))
}

// OpenGaps is the number of gaps not yet closed, for metrics.
func (t *Tracker) OpenGaps() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	n := 0
	for _, st := range t.products {
		n += len(st.open)
	}
	return n
}

func shortVenue(v pb.Venue) string {
	switch v {
	case pb.Venue_VENUE_COINBASE:
		return "cb"
	case pb.Venue_VENUE_KRAKEN:
		return "kr"
	}
	return "xx"
}

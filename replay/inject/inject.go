// Package inject adds labelled synthetic manipulation patterns to a replayed
// stream so detectors can be scored against ground truth.
//
// It wraps the replay sink. Every real trade gets synthetic buyer and seller
// accounts (public feeds have none). Injections are scheduled at event times;
// synthetic events are released in event-time order as the replayed stream
// passes them, so they interleave with real data exactly as live events would.
// Each injection emits one InjectionLabel with the pattern and its window.
//
// Patterns (vocabulary shared with Alert.detector):
//
//	volume_spike    extra trades worth M× the recent 10 s notional, spread over 10 s
//	spoof_layering  an order of S× the rest of the visible depth added behind the touch (level 2 or 3),
//	                pulled after L ms, never traded
//	dislocation     Coinbase prices for a product shifted by B bps for D seconds
//	wash_trade      a self trade, or a round trip A→B then B→A of the same size
package inject

import (
	"container/heap"
	"context"
	"fmt"
	"hash/fnv"
	"math"
	"math/rand/v2"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	pb "tidemark/gen/go/tidemark/v1"
	"tidemark/ingest/sink"
)

// Labeller receives ground truth. *sink.Kafka implements it.
type Labeller interface {
	Label(*pb.InjectionLabel)
}

// Injection is one planned pattern.
type Injection struct {
	ID      string
	Pattern string
	Venue   pb.Venue
	Product string
	At      time.Time // event time at which it starts
	Params  map[string]float64
}

// Plan spreads `perPattern` injections of each pattern over [start, end),
// at least `gap` apart, after a warm-up so detector baselines exist.
func Plan(seed uint64, start, end time.Time, warmup, gap time.Duration, perPattern int, products []string) []Injection {
	r := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	patterns := []string{"volume_spike", "spoof_layering", "dislocation", "wash_trade"}
	total := perPattern * len(patterns)
	usable := end.Sub(start) - warmup
	if total == 0 || usable <= 0 {
		return nil
	}
	slot := usable / time.Duration(total)
	if slot < gap {
		total = int(usable / gap)
		slot = gap
	}
	order := make([]string, 0, total)
	for i := 0; len(order) < total; i++ {
		order = append(order, patterns[i%len(patterns)])
	}
	r.Shuffle(len(order), func(i, j int) { order[i], order[j] = order[j], order[i] })
	var out []Injection
	for i, pat := range order {
		at := start.Add(warmup + time.Duration(i)*slot + time.Duration(r.Int64N(int64(slot/3)+1)))
		inj := Injection{
			ID:      fmt.Sprintf("inj-%03d-%s", i, pat),
			Pattern: pat,
			Product: products[r.IntN(len(products))],
			At:      at,
			Params:  map[string]float64{},
		}
		switch pat {
		case "volume_spike":
			inj.Venue = pick(r, pb.Venue_VENUE_COINBASE, pb.Venue_VENUE_KRAKEN)
			inj.Params["multiple"] = between(r, 2, 40) // includes subtle ones near any sane threshold
		case "spoof_layering":
			inj.Venue = pb.Venue_VENUE_KRAKEN                 // the only venue with a book feed
			inj.Params["depth_share"] = logBetween(r, 0.3, 8) // spans the real noise floor (see docs/eval-report.md)
			inj.Params["life_ms"] = between(r, 500, 8000)
			inj.Params["rank"] = float64(2 + r.IntN(2))
			inj.Params["ask"] = float64(r.IntN(2))
		case "dislocation":
			inj.Params["bps"] = between(r, 3, 60) * float64(pick(r, 1, -1))
			inj.Params["seconds"] = between(r, 5, 60)
		case "wash_trade":
			inj.Venue = pick(r, pb.Venue_VENUE_COINBASE, pb.Venue_VENUE_KRAKEN)
			inj.Params["round_trip"] = float64(r.IntN(2))
			inj.Params["gap_ms"] = between(r, 2000, 40000)
			inj.Params["size_jitter"] = between(r, 0, 0.03)
		}
		out = append(out, inj)
	}
	return out
}

func between(r *rand.Rand, lo, hi float64) float64 { return lo + r.Float64()*(hi-lo) }

// logBetween draws uniformly in log space, so small and large magnitudes are equally represented.
func logBetween(r *rand.Rand, lo, hi float64) float64 {
	return math.Exp(math.Log(lo) + r.Float64()*(math.Log(hi)-math.Log(lo)))
}
func pick[T any](r *rand.Rand, a, b T) T {
	if r.IntN(2) == 0 {
		return a
	}
	return b
}

// ------------------------------------------------------------------ injector

type pending struct {
	ts       int64
	seq      int
	trade    *pb.Trade
	book     *pb.BookUpdate
	spoofEnd *overlay // pull the synthetic order
}

// overlay is a synthetic resting order: while active, every real update to its
// level is shown with the synthetic quantity added (Kraken sends absolute
// level quantities, so without this the next real update would erase it).
type overlay struct {
	venue    pb.Venue
	product  string
	bid      bool
	price    string
	added    float64
	lastReal string
}

type queue []pending

func (q queue) Len() int { return len(q) }
func (q queue) Less(i, j int) bool {
	if q[i].ts != q[j].ts {
		return q[i].ts < q[j].ts
	}
	return q[i].seq < q[j].seq
}
func (q queue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }
func (q *queue) Push(x any)   { *q = append(*q, x.(pending)) }
func (q *queue) Pop() any {
	old := *q
	x := old[len(old)-1]
	*q = old[:len(old)-1]
	return x
}

type instrument struct {
	lastPrice   string
	bucket      int64   // current 10 s bucket start (ns)
	bucketNotnl float64 // notional in the current bucket
	ewma        float64 // EWMA of closed 10 s bucket notional
	bids, asks  map[string]string
}

type dislocation struct {
	product  string
	from, to int64
	factor   float64
}

// Injector is a sink.Sink that adds injections to the stream it forwards.
type Injector struct {
	mu       sync.Mutex
	out      sink.Sink
	labels   Labeller
	plan     []Injection
	next     int
	q        queue
	seq      int
	nextID   uint64
	accounts int
	inst     map[string]*instrument
	disloc   []dislocation
	overlays []*overlay
	depth    int
	Fired    map[string]int
	Skipped  []string
}

// New wraps out. accounts is the size of the synthetic account pool.
func New(out sink.Sink, labels Labeller, plan []Injection, accounts int) *Injector {
	sort.Slice(plan, func(i, j int) bool { return plan[i].At.Before(plan[j].At) })
	return &Injector{
		out: out, labels: labels, plan: plan, accounts: accounts,
		nextID: 1 << 62, inst: map[string]*instrument{}, Fired: map[string]int{}, depth: 10,
	}
}

func key(v pb.Venue, product string) string { return v.String() + ":" + product }

func (in *Injector) instr(v pb.Venue, product string) *instrument {
	k := key(v, product)
	st, ok := in.inst[k]
	if !ok {
		st = &instrument{bids: map[string]string{}, asks: map[string]string{}}
		in.inst[k] = st
	}
	return st
}

func account(n int) string { return "acct-" + strconv.Itoa(n) }

// assign gives a real trade two different, deterministic background accounts.
func (in *Injector) assign(t *pb.Trade) {
	h := fnv.New64a()
	fmt.Fprintf(h, "%d:%s:%d", t.Venue, t.ProductId, t.TradeId)
	x := h.Sum64()
	b := int(x % uint64(in.accounts))
	s := (b + 1 + int((x>>32)%uint64(in.accounts-1))) % in.accounts
	t.BuyerAccount, t.SellerAccount = account(b), account(s)
}

func (in *Injector) Trade(t *pb.Trade) {
	in.mu.Lock()
	defer in.mu.Unlock()
	in.advance(t.EventTsNs)
	in.assign(t)
	st := in.instr(t.Venue, t.ProductId)
	in.observeTrade(st, t)
	if t.Venue == pb.Venue_VENUE_COINBASE {
		for _, d := range in.disloc {
			if d.product == t.ProductId && t.EventTsNs >= d.from && t.EventTsNs <= d.to {
				t.Price = scale(t.Price, d.factor)
			}
		}
	}
	in.out.Trade(t)
}

func (in *Injector) Book(b *pb.BookUpdate) {
	in.mu.Lock()
	defer in.mu.Unlock()
	in.advance(b.EventTsNs)
	in.observeBook(b)
	in.applyOverlays(b)
	in.out.Book(b)
}

func (in *Injector) applyOverlays(b *pb.BookUpdate) {
	for _, o := range in.overlays {
		if o.venue != b.Venue || o.product != b.ProductId {
			continue
		}
		levels := b.Asks
		if o.bid {
			levels = b.Bids
		}
		for _, l := range levels {
			if l.Price == o.price {
				o.lastReal = l.Qty
				real, _ := strconv.ParseFloat(l.Qty, 64)
				l.Qty = strconv.FormatFloat(real+o.added, 'f', 8, 64)
			}
		}
	}
}

func (in *Injector) endOverlay(o *overlay, ts int64) {
	for i, x := range in.overlays {
		if x == o {
			in.overlays = append(in.overlays[:i], in.overlays[i+1:]...)
			break
		}
	}
	pull := &pb.BookUpdate{Venue: o.venue, ProductId: o.product, EventTsNs: ts, RecvTsNs: ts,
		ChecksumOk: true, Source: pb.Source_SOURCE_REPLAY}
	lvl := []*pb.BookLevel{{Price: o.price, Qty: o.lastReal}}
	if o.bid {
		pull.Bids = lvl
	} else {
		pull.Asks = lvl
	}
	in.out.Book(pull)
}

func (in *Injector) Gap(g *pb.GapEvent)              { in.out.Gap(g) }
func (in *Injector) Flush(ctx context.Context) error { in.drain(); return in.out.Flush(ctx) }
func (in *Injector) Close()                          { in.out.Close() }

// drain releases any synthetic events still queued at end of input.
func (in *Injector) drain() {
	in.mu.Lock()
	defer in.mu.Unlock()
	in.advance(math.MaxInt64)
}

func (in *Injector) observeTrade(st *instrument, t *pb.Trade) {
	st.lastPrice = t.Price
	b := t.EventTsNs - t.EventTsNs%int64(10*time.Second)
	if b != st.bucket {
		if st.bucket != 0 {
			closed := float64((b-st.bucket)/int64(10*time.Second)) - 1 // empty buckets in between
			for i := 0.0; i < math.Min(closed, 50); i++ {
				st.ewma = 0.98 * st.ewma
			}
			st.ewma = 0.98*st.ewma + 0.02*st.bucketNotnl
		}
		st.bucket, st.bucketNotnl = b, 0
	}
	p, _ := strconv.ParseFloat(t.Price, 64)
	s, _ := strconv.ParseFloat(t.Size, 64)
	st.bucketNotnl += p * s
}

func (in *Injector) observeBook(b *pb.BookUpdate) {
	st := in.instr(b.Venue, b.ProductId)
	if b.IsSnapshot {
		st.bids, st.asks = map[string]string{}, map[string]string{}
	}
	apply := func(m map[string]string, levels []*pb.BookLevel) {
		for _, l := range levels {
			if q, _ := strconv.ParseFloat(l.Qty, 64); q == 0 {
				delete(m, l.Price)
			} else {
				m[l.Price] = l.Qty
			}
		}
	}
	apply(st.bids, b.Bids)
	apply(st.asks, b.Asks)
	// Kraken never deletes levels that drift out of the subscribed depth;
	// without truncation they resurface later as phantom best prices.
	truncate(st.bids, in.depth, true)
	truncate(st.asks, in.depth, false)
}

func truncate(m map[string]string, depth int, bid bool) {
	if depth <= 0 || len(m) <= depth {
		return
	}
	type lv struct {
		p  string
		pf float64
	}
	ls := make([]lv, 0, len(m))
	for p := range m {
		pf, _ := strconv.ParseFloat(p, 64)
		ls = append(ls, lv{p, pf})
	}
	sort.Slice(ls, func(i, j int) bool {
		if bid {
			return ls[i].pf > ls[j].pf
		}
		return ls[i].pf < ls[j].pf
	})
	for _, l := range ls[depth:] {
		delete(m, l.p)
	}
}

// advance fires injections due by ts and releases queued synthetic events.
func (in *Injector) advance(ts int64) {
	for in.next < len(in.plan) && in.plan[in.next].At.UnixNano() <= ts {
		in.fire(in.plan[in.next])
		in.next++
	}
	for in.q.Len() > 0 && in.q[0].ts <= ts {
		p := heap.Pop(&in.q).(pending)
		switch {
		case p.trade != nil:
			in.out.Trade(p.trade)
		case p.spoofEnd != nil:
			in.endOverlay(p.spoofEnd, p.ts)
		default:
			in.out.Book(p.book)
		}
	}
}

func (in *Injector) push(p pending) {
	in.seq++
	p.seq = in.seq
	heap.Push(&in.q, p)
}

func (in *Injector) synthTrade(v pb.Venue, product, price, size string, side pb.Side, ts int64, buyer, seller string) *pb.Trade {
	in.nextID++
	return &pb.Trade{
		Venue: v, ProductId: product, TradeId: in.nextID, Price: price, Size: size, TakerSide: side,
		EventTsNs: ts, RecvTsNs: ts, Source: pb.Source_SOURCE_REPLAY, IngestId: "inject",
		BuyerAccount: buyer, SellerAccount: seller,
	}
}

func (in *Injector) fire(inj Injection) {
	at := inj.At.UnixNano()
	label := &pb.InjectionLabel{
		InjectionId: inj.ID, Pattern: inj.Pattern, Venue: inj.Venue, ProductId: inj.Product,
		StartTsNs: at, Params: map[string]string{},
	}
	for k, v := range inj.Params {
		label.Params[k] = strconv.FormatFloat(v, 'f', 4, 64)
	}
	skip := func(why string) { in.Skipped = append(in.Skipped, inj.ID+": "+why) }

	switch inj.Pattern {
	case "volume_spike":
		st := in.instr(inj.Venue, inj.Product)
		price, _ := strconv.ParseFloat(st.lastPrice, 64)
		if price == 0 || st.ewma == 0 {
			skip("no price or baseline yet")
			return
		}
		target := inj.Params["multiple"] * st.ewma
		n := 20
		size := target / price / float64(n)
		for i := 0; i < n; i++ {
			ts := at + int64(i)*int64(500*time.Millisecond)
			side := pb.Side_SIDE_BUY
			if i%3 == 0 {
				side = pb.Side_SIDE_SELL
			}
			in.push(pending{ts: ts, trade: in.synthTrade(inj.Venue, inj.Product, st.lastPrice,
				strconv.FormatFloat(size, 'f', 8, 64), side, ts, "", "")})
		}
		label.EndTsNs = at + int64(10*time.Second)
		label.Params["target_notional"] = strconv.FormatFloat(target, 'f', 2, 64)

	case "spoof_layering":
		st := in.instr(inj.Venue, inj.Product)
		bid := inj.Params["ask"] < 0.5
		side, levels := "bid", st.bids
		if !bid {
			side, levels = "ask", st.asks
		}
		price, qty, others := levelAt(levels, bid, int(inj.Params["rank"]))
		if price == "" || others == 0 {
			skip("no book yet")
			return
		}
		added := inj.Params["depth_share"] * others
		bq, _ := strconv.ParseFloat(qty, 64)
		life := int64(inj.Params["life_ms"]) * int64(time.Millisecond)
		o := &overlay{venue: inj.Venue, product: inj.Product, bid: bid, price: price, added: added, lastReal: qty}
		in.overlays = append(in.overlays, o)
		up := &pb.BookUpdate{Venue: inj.Venue, ProductId: inj.Product, EventTsNs: at, RecvTsNs: at,
			ChecksumOk: true, Source: pb.Source_SOURCE_REPLAY}
		upLvl := []*pb.BookLevel{{Price: price, Qty: strconv.FormatFloat(bq+added, 'f', 8, 64)}}
		if bid {
			up.Bids = upLvl
		} else {
			up.Asks = upLvl
		}
		in.push(pending{ts: at, book: up})
		in.push(pending{ts: at + life, spoofEnd: o})
		label.Params["side"] = side
		label.Params["price"] = price
		label.Params["added_qty"] = strconv.FormatFloat(added, 'f', 8, 64)
		label.EndTsNs = at + life

	case "dislocation":
		secs := int64(inj.Params["seconds"] * float64(time.Second))
		in.disloc = append(in.disloc, dislocation{product: inj.Product, from: at, to: at + secs,
			factor: 1 + inj.Params["bps"]/10_000})
		label.EndTsNs = at + secs

	case "wash_trade":
		st := in.instr(inj.Venue, inj.Product)
		if st.lastPrice == "" {
			skip("no price yet")
			return
		}
		price, _ := strconv.ParseFloat(st.lastPrice, 64)
		size := math.Max(0.001, 2000/price) // about $2k notional
		a, b := "wash-"+inj.ID+"-a", "wash-"+inj.ID+"-b"
		if inj.Params["round_trip"] < 0.5 {
			in.push(pending{ts: at, trade: in.synthTrade(inj.Venue, inj.Product, st.lastPrice,
				strconv.FormatFloat(size, 'f', 8, 64), pb.Side_SIDE_BUY, at, a, a)})
			label.EndTsNs = at
			label.Params["kind"] = "self_trade"
		} else {
			gap := int64(inj.Params["gap_ms"]) * int64(time.Millisecond)
			back := size * (1 + inj.Params["size_jitter"])
			in.push(pending{ts: at, trade: in.synthTrade(inj.Venue, inj.Product, st.lastPrice,
				strconv.FormatFloat(size, 'f', 8, 64), pb.Side_SIDE_BUY, at, a, b)})
			in.push(pending{ts: at + gap, trade: in.synthTrade(inj.Venue, inj.Product, st.lastPrice,
				strconv.FormatFloat(back, 'f', 8, 64), pb.Side_SIDE_SELL, at+gap, b, a)})
			label.EndTsNs = at + gap
			label.Params["kind"] = "round_trip"
		}
	}
	in.Fired[inj.Pattern]++
	if in.labels != nil {
		in.labels.Label(label)
	}
}

// levelAt returns the price and quantity at a 1-based rank and the total
// quantity on the other levels of that side.
func levelAt(levels map[string]string, bid bool, rank int) (price, qty string, others float64) {
	type lv struct {
		p, q string
		pf   float64
		qf   float64
	}
	var ls []lv
	for p, q := range levels {
		pf, _ := strconv.ParseFloat(p, 64)
		qf, _ := strconv.ParseFloat(q, 64)
		ls = append(ls, lv{p, q, pf, qf})
	}
	if len(ls) < rank+1 {
		return "", "", 0
	}
	sort.Slice(ls, func(i, j int) bool {
		if bid {
			return ls[i].pf > ls[j].pf
		}
		return ls[i].pf < ls[j].pf
	})
	for i, l := range ls {
		if i != rank-1 {
			others += l.qf
		}
	}
	return ls[rank-1].p, ls[rank-1].q, others
}

// scale multiplies decimal text by factor, keeping the original number of decimals.
func scale(price string, factor float64) string {
	p, err := strconv.ParseFloat(price, 64)
	if err != nil {
		return price
	}
	dec := 0
	if i := strings.IndexByte(price, '.'); i >= 0 {
		dec = len(price) - i - 1
	}
	return strconv.FormatFloat(p*factor, 'f', dec, 64)
}

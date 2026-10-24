// Package pipeline connects a venue feed to the gap tracker, the backfill
// workers and the sink. HandleFrame is called from one goroutine per venue;
// backfill workers run concurrently and share the tracker.
package pipeline

import (
	"context"
	"log/slog"
	"strconv"
	"sync/atomic"
	"time"

	pb "tidemark/gen/go/tidemark/v1"
	"tidemark/ingest/gaps"
	"tidemark/ingest/metrics"
	"tidemark/ingest/sink"
	"tidemark/ingest/venue"
)

type Options struct {
	Source     pb.Source
	IngestID   string
	TrackGaps  bool
	Backfiller venue.Backfiller // nil disables backfill (gaps stay DETECTED)
	// BackfillGrace waits before the first REST call so late WS frames and
	// REST indexing can catch up.
	BackfillGrace time.Duration
	// BackfillTries before a gap is declared unrecoverable.
	BackfillTries int
	// BackfillRetryDelay is the first delay between tries; it doubles each time.
	BackfillRetryDelay time.Duration
}

type Pipeline struct {
	feed    venue.Feed
	tracker *gaps.Tracker
	sink    sink.Sink
	m       *metrics.Metrics
	opts    Options
	vname   string
	send    func([]byte)
	gapCh   chan gaps.Gap
	bookSeq map[string]uint64
	resyncs map[string]resync
	log     *slog.Logger
	now     func() time.Time
	dropTil atomic.Int64 // fault injection: discard frames until this unix-nano time
}

type resync struct {
	id string
	at time.Time
}

func New(feed venue.Feed, s sink.Sink, m *metrics.Metrics, opts Options) *Pipeline {
	if opts.Source == pb.Source_SOURCE_UNSPECIFIED {
		opts.Source = pb.Source_SOURCE_WS
	}
	if opts.BackfillTries == 0 {
		opts.BackfillTries = 6
	}
	if opts.BackfillRetryDelay == 0 {
		opts.BackfillRetryDelay = 2 * time.Second
	}
	vname := venue.Name(feed.Venue())
	return &Pipeline{
		feed:    feed,
		tracker: gaps.NewTracker(feed.Venue()),
		sink:    s,
		m:       m,
		opts:    opts,
		vname:   vname,
		send:    func([]byte) {},
		gapCh:   make(chan gaps.Gap, 1024),
		bookSeq: map[string]uint64{},
		resyncs: map[string]resync{},
		log:     slog.With("venue", vname),
		now:     time.Now,
	}
}

// InitMetrics creates every gap series at zero for the given products.
// A counter that first appears with value 1 has no earlier sample, so
// Prometheus increase() would miss that first gap.
func (p *Pipeline) InitMetrics(products []string) {
	kinds := []pb.GapKind{pb.GapKind_GAP_KIND_TRADE_ID_JUMP, pb.GapKind_GAP_KIND_HEARTBEAT_AHEAD, pb.GapKind_GAP_KIND_CHECKSUM_MISMATCH}
	statuses := []pb.GapStatus{pb.GapStatus_GAP_STATUS_RECOVERED, pb.GapStatus_GAP_STATUS_ABSENT_AT_SOURCE, pb.GapStatus_GAP_STATUS_UNRECOVERABLE}
	for _, prod := range products {
		p.m.GapTradesMissing.WithLabelValues(p.vname, prod)
		p.m.TradesLost.WithLabelValues(p.vname, prod)
		p.m.GapTradesFilled.WithLabelValues(p.vname, prod)
		p.m.ChecksumFailures.WithLabelValues(p.vname, prod)
		p.m.Duplicates.WithLabelValues(p.vname, prod)
		for _, k := range kinds {
			p.m.GapsDetected.WithLabelValues(p.vname, prod, kindLabel(k))
			for _, st := range statuses {
				p.m.GapsResolved.WithLabelValues(p.vname, prod, kindLabel(k), statusLabel(st))
			}
		}
	}
	p.m.GapsOpen.WithLabelValues(p.vname)
}

// SetSender wires the function used to send frames on the live connection.
func (p *Pipeline) SetSender(f func([]byte)) { p.send = f }

func (p *Pipeline) Tracker() *gaps.Tracker { return p.tracker }

// Reset is called on every new connection. Open book resyncs are moot because
// the new session subscribes to fresh snapshots.
func (p *Pipeline) Reset() {
	p.feed.Reset()
	for product, rs := range p.resyncs {
		p.resolveResync(product, rs, pb.GapStatus_GAP_STATUS_RECOVERED, "reconnected")
	}
}

// DropFor discards every incoming frame for d, simulating messages lost in
// transit. Used by the /debug/drop endpoint and chaos experiments.
func (p *Pipeline) DropFor(d time.Duration) {
	p.dropTil.Store(p.now().Add(d).UnixNano())
	p.log.Warn("fault injection: dropping frames", "for", d)
}

func (p *Pipeline) HandleFrame(frame []byte, recv time.Time) {
	p.m.Frames.WithLabelValues(p.vname).Inc()
	if recv.UnixNano() < p.dropTil.Load() {
		return
	}
	events, err := p.feed.Parse(frame, recv)
	if err != nil {
		p.m.ParseErrors.WithLabelValues(p.vname).Inc()
		p.log.Warn("parse", "err", err)
	}
	for _, ev := range events {
		switch {
		case ev.Trade != nil:
			p.handleTrade(ev.Trade, recv)
		case ev.Heartbeat != nil:
			if p.opts.TrackGaps {
				if g := p.tracker.ObserveHeartbeat(ev.Heartbeat.Product, ev.Heartbeat.LastTradeID); g != nil {
					p.gapOpened(*g)
				}
			}
		case ev.Book != nil:
			b := ev.Book
			p.bookSeq[b.ProductId]++
			b.Seq = p.bookSeq[b.ProductId]
			b.Source = p.opts.Source
			p.m.BookUpdates.WithLabelValues(p.vname, b.ProductId, strconv.FormatBool(b.ChecksumOk || b.Checksum == 0)).Inc()
			p.sink.Book(b)
		case ev.BookMismatch != nil:
			p.handleMismatch(ev.BookMismatch)
		case ev.BookResynced != "":
			if rs, ok := p.resyncs[ev.BookResynced]; ok {
				p.resolveResync(ev.BookResynced, rs, pb.GapStatus_GAP_STATUS_RECOVERED, "fresh snapshot")
			}
		}
	}
}

func (p *Pipeline) handleTrade(t *pb.Trade, recv time.Time) {
	t.Source = p.opts.Source
	t.IngestId = p.opts.IngestID
	if t.RecvTsNs == 0 {
		t.RecvTsNs = recv.UnixNano()
	}
	if p.opts.TrackGaps {
		accept, g := p.tracker.Observe(t.ProductId, t.TradeId, time.Unix(0, t.EventTsNs))
		if g != nil {
			p.gapOpened(*g)
		}
		if !accept {
			p.m.Duplicates.WithLabelValues(p.vname, t.ProductId).Inc()
			return
		}
	}
	p.m.Trades.WithLabelValues(p.vname, t.ProductId, sourceLabel(t.Source)).Inc()
	p.sink.Trade(t)
}

func (p *Pipeline) gapOpened(g gaps.Gap) {
	kind := kindLabel(g.Kind)
	p.m.GapsDetected.WithLabelValues(p.vname, g.Product, kind).Inc()
	p.m.GapTradesMissing.WithLabelValues(p.vname, g.Product).Add(float64(g.Span()))
	p.m.GapsOpen.WithLabelValues(p.vname).Set(float64(p.tracker.OpenGaps()))
	p.log.Warn("gap detected", "product", g.Product, "kind", kind, "first", g.First, "last", g.Last, "span", g.Span())
	p.sink.Gap(&pb.GapEvent{
		GapId:               g.ID,
		Venue:               g.Venue,
		ProductId:           g.Product,
		Kind:                g.Kind,
		Status:              pb.GapStatus_GAP_STATUS_DETECTED,
		FirstMissingTradeId: g.First,
		LastMissingTradeId:  g.Last,
		DetectedTsNs:        g.DetectedAt.UnixNano(),
	})
	switch {
	case p.opts.Backfiller == nil:
		// Replay: record the gap only.
	case g.Span() > gaps.MaxSpan:
		p.closeGap(g, pb.GapStatus_GAP_STATUS_UNRECOVERABLE, "span exceeds backfill cap")
	default:
		select {
		case p.gapCh <- g:
		default:
			p.closeGap(g, pb.GapStatus_GAP_STATUS_UNRECOVERABLE, "backfill queue full")
		}
	}
}

func (p *Pipeline) closeGap(g gaps.Gap, status pb.GapStatus, detail string) {
	filled := p.tracker.Close(g.Product, g.ID)
	now := p.now()
	kind, st := kindLabel(g.Kind), statusLabel(status)
	p.m.GapsResolved.WithLabelValues(p.vname, g.Product, kind, st).Inc()
	p.m.GapTradesFilled.WithLabelValues(p.vname, g.Product).Add(float64(filled))
	p.m.GapRecovery.WithLabelValues(p.vname, kind, st).Observe(now.Sub(g.DetectedAt).Seconds())
	p.m.GapsOpen.WithLabelValues(p.vname).Set(float64(p.tracker.OpenGaps()))
	p.log.Info("gap resolved", "product", g.Product, "status", st, "filled", filled, "span", g.Span(),
		"took", now.Sub(g.DetectedAt).Round(time.Millisecond), "detail", detail)
	p.sink.Gap(&pb.GapEvent{
		GapId:               g.ID,
		Venue:               g.Venue,
		ProductId:           g.Product,
		Kind:                g.Kind,
		Status:              status,
		FirstMissingTradeId: g.First,
		LastMissingTradeId:  g.Last,
		RecoveredCount:      filled,
		DetectedTsNs:        g.DetectedAt.UnixNano(),
		ResolvedTsNs:        now.UnixNano(),
		Detail:              detail,
	})
}

// RunBackfill processes gaps until ctx is done.
func (p *Pipeline) RunBackfill(ctx context.Context, workers int) {
	if p.opts.Backfiller == nil {
		return
	}
	for range workers {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case g := <-p.gapCh:
					p.backfill(ctx, g)
				}
			}
		}()
	}
}

func (p *Pipeline) backfill(ctx context.Context, g gaps.Gap) {
	wait := func(d time.Duration) bool {
		select {
		case <-ctx.Done():
			return false
		case <-time.After(d):
			return true
		}
	}
	if !wait(time.Until(g.DetectedAt.Add(p.opts.BackfillGrace))) {
		return
	}
	delay := p.opts.BackfillRetryDelay
	for try := 1; ; try++ {
		if p.tracker.Remaining(g.Product, g.ID) == 0 {
			p.closeGap(g, pb.GapStatus_GAP_STATUS_RECOVERED, "filled by late websocket frames")
			return
		}
		trades, sourceMax, err := p.opts.Backfiller.Fetch(ctx, g.Product, g.First, g.Last, g.PrevTradeTs)
		ids := make([]uint64, len(trades))
		for i, t := range trades {
			ids[i] = t.TradeId
		}
		now := p.now().UnixNano()
		for i, fresh := range p.tracker.Claim(g.Product, g.ID, ids) {
			if !fresh {
				continue
			}
			t := trades[i]
			t.Source = pb.Source_SOURCE_REST
			t.IngestId = p.opts.IngestID
			t.RecvTsNs = now
			p.m.Trades.WithLabelValues(p.vname, t.ProductId, "rest").Inc()
			p.sink.Trade(t)
		}
		remaining := p.tracker.Remaining(g.Product, g.ID)
		switch {
		case remaining == 0:
			p.closeGap(g, pb.GapStatus_GAP_STATUS_RECOVERED, "backfilled over REST")
			return
		case err == nil && sourceMax >= g.Last:
			p.closeGap(g, pb.GapStatus_GAP_STATUS_ABSENT_AT_SOURCE,
				strconv.FormatUint(remaining, 10)+" ids not present at source")
			return
		case try >= p.opts.BackfillTries:
			detail := "source still behind after retries"
			if err != nil {
				detail = err.Error()
			}
			p.closeGap(g, pb.GapStatus_GAP_STATUS_UNRECOVERABLE, detail)
			return
		}
		if err != nil {
			p.log.Warn("backfill attempt failed", "product", g.Product, "try", try, "err", err)
		}
		if !wait(delay) {
			return
		}
		delay *= 2
	}
}

func (p *Pipeline) handleMismatch(bm *venue.BookMismatch) {
	p.m.ChecksumFailures.WithLabelValues(p.vname, bm.Product).Inc()
	if _, open := p.resyncs[bm.Product]; !open {
		now := p.now()
		id := p.vname + "-book-" + bm.Product + "-" + strconv.FormatInt(now.UnixNano(), 36)
		p.resyncs[bm.Product] = resync{id: id, at: now}
		p.m.GapsDetected.WithLabelValues(p.vname, bm.Product, "checksum_mismatch").Inc()
		p.sink.Gap(&pb.GapEvent{
			GapId:        id,
			Venue:        p.feed.Venue(),
			ProductId:    bm.Product,
			Kind:         pb.GapKind_GAP_KIND_CHECKSUM_MISMATCH,
			Status:       pb.GapStatus_GAP_STATUS_DETECTED,
			DetectedTsNs: now.UnixNano(),
			Detail:       "expected " + strconv.FormatUint(uint64(bm.Expected), 10) + " computed " + strconv.FormatUint(uint64(bm.Computed), 10),
		})
	}
	p.log.Warn("book checksum mismatch; resubscribing", "product", bm.Product, "expected", bm.Expected, "computed", bm.Computed)
	for _, m := range bm.Messages {
		p.send(m)
	}
}

func (p *Pipeline) resolveResync(product string, rs resync, status pb.GapStatus, detail string) {
	delete(p.resyncs, product)
	now := p.now()
	st := statusLabel(status)
	p.m.GapsResolved.WithLabelValues(p.vname, product, "checksum_mismatch", st).Inc()
	p.m.GapRecovery.WithLabelValues(p.vname, "checksum_mismatch", st).Observe(now.Sub(rs.at).Seconds())
	p.sink.Gap(&pb.GapEvent{
		GapId:        rs.id,
		Venue:        p.feed.Venue(),
		ProductId:    product,
		Kind:         pb.GapKind_GAP_KIND_CHECKSUM_MISMATCH,
		Status:       status,
		DetectedTsNs: rs.at.UnixNano(),
		ResolvedTsNs: now.UnixNano(),
		Detail:       detail,
	})
}

func sourceLabel(s pb.Source) string {
	switch s {
	case pb.Source_SOURCE_WS:
		return "ws"
	case pb.Source_SOURCE_REST:
		return "rest"
	case pb.Source_SOURCE_REPLAY:
		return "replay"
	}
	return "unknown"
}

func kindLabel(k pb.GapKind) string {
	switch k {
	case pb.GapKind_GAP_KIND_TRADE_ID_JUMP:
		return "trade_id_jump"
	case pb.GapKind_GAP_KIND_HEARTBEAT_AHEAD:
		return "heartbeat_ahead"
	case pb.GapKind_GAP_KIND_CHECKSUM_MISMATCH:
		return "checksum_mismatch"
	}
	return "unknown"
}

func statusLabel(s pb.GapStatus) string {
	switch s {
	case pb.GapStatus_GAP_STATUS_RECOVERED:
		return "recovered"
	case pb.GapStatus_GAP_STATUS_UNRECOVERABLE:
		return "unrecoverable"
	case pb.GapStatus_GAP_STATUS_ABSENT_AT_SOURCE:
		return "absent_at_source"
	case pb.GapStatus_GAP_STATUS_DETECTED:
		return "detected"
	}
	return "unknown"
}

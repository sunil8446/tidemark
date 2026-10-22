// Command audit reads a trades topic from the beginning and reports, per
// venue and product, how complete the stream is: ids expected between the
// first and last trade_id seen, ids present, ids missing, and duplicates.
// It checks Kafka itself, independently of the ingestor's own counters.
//
//	audit --since 2h            # only trades with event time in the last 2h
//	audit --format markdown     # table for bench/gaps.md
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sr"
	"google.golang.org/protobuf/proto"

	pb "tidemark/gen/go/tidemark/v1"
	"tidemark/ingest/venue"
)

type stats struct {
	ids        map[uint64]int
	ws, rest   int
	minID      uint64
	maxID      uint64
	firstEvent time.Time
	lastEvent  time.Time
}

func main() {
	var (
		brokers = flag.String("brokers", "localhost:19092", "comma-separated Kafka brokers")
		topic   = flag.String("topic", "trades.v1", "trades topic")
		since   = flag.Duration("since", 0, "only count trades with event time within this window (0 = all)")
		settle  = flag.Duration("settle", 30*time.Second, "ignore the most recent trades (backfill may still be in flight)")
		format  = flag.String("format", "text", "text or markdown")
	)
	flag.Parse()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	cl, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(*brokers, ",")...))
	if err != nil {
		fail(err)
	}
	defer cl.Close()
	// Snapshot the end offsets so the scan terminates.
	ends, err := kadm.NewClient(cl).ListEndOffsets(ctx, *topic)
	if err != nil {
		fail(err)
	}
	target := map[int32]int64{}
	assign := map[string]map[int32]kgo.Offset{*topic: {}}
	ends.Each(func(o kadm.ListedOffset) {
		if o.Offset > 0 {
			target[o.Partition] = o.Offset
			assign[*topic][o.Partition] = kgo.NewOffset().AtStart()
		}
	})
	cl.AddConsumePartitions(assign)

	now := time.Now()
	lo := time.Time{}
	if *since > 0 {
		lo = now.Add(-*since)
	}
	hi := now.Add(-*settle)
	var h sr.ConfluentHeader
	all := map[string]*stats{}
	var records int
	for len(target) > 0 {
		fs := cl.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			fail(err)
		}
		fs.EachError(func(t string, p int32, err error) { fail(fmt.Errorf("%s/%d: %w", t, p, err)) })
		fs.EachRecord(func(r *kgo.Record) {
			if r.Offset+1 >= target[r.Partition] {
				delete(target, r.Partition)
			}
			records++
			_, rest, err := h.DecodeID(r.Value)
			if err != nil {
				return
			}
			_, body, err := h.DecodeIndex(rest, 8)
			if err != nil {
				return
			}
			var t pb.Trade
			if proto.Unmarshal(body, &t) != nil {
				return
			}
			ev := time.Unix(0, t.EventTsNs)
			if ev.Before(lo) || ev.After(hi) {
				return
			}
			k := venue.Key(t.Venue, t.ProductId)
			s, ok := all[k]
			if !ok {
				s = &stats{ids: map[uint64]int{}, minID: t.TradeId, maxID: t.TradeId, firstEvent: ev, lastEvent: ev}
				all[k] = s
			}
			s.ids[t.TradeId]++
			s.minID, s.maxID = min(s.minID, t.TradeId), max(s.maxID, t.TradeId)
			if ev.Before(s.firstEvent) {
				s.firstEvent = ev
			}
			if ev.After(s.lastEvent) {
				s.lastEvent = ev
			}
			switch t.Source {
			case pb.Source_SOURCE_REST:
				s.rest++
			default:
				s.ws++
			}
		})
	}

	keys := make([]string, 0, len(all))
	for k := range all {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	var totExp, totPresent, totDup uint64
	md := *format == "markdown"
	if md {
		fmt.Printf("Audit of `%s` at %s (records scanned: %d; window: %s; settle: %s)\n\n", *topic, now.UTC().Format(time.RFC3339), records, window(*since), *settle)
		fmt.Println("| venue:product | first event (UTC) | last event (UTC) | expected ids | present | missing | duplicates | via REST | completeness |")
		fmt.Println("|---|---|---|---:|---:|---:|---:|---:|---:|")
	}
	for _, k := range keys {
		s := all[k]
		exp := s.maxID - s.minID + 1
		present := uint64(len(s.ids))
		var dup uint64
		for _, n := range s.ids {
			dup += uint64(n - 1)
		}
		totExp += exp
		totPresent += present
		totDup += dup
		pct := 100 * float64(present) / float64(exp)
		if md {
			fmt.Printf("| %s | %s | %s | %d | %d | %d | %d | %d | %.4f%% |\n", k, s.firstEvent.UTC().Format("01-02 15:04:05"), s.lastEvent.UTC().Format("01-02 15:04:05"), exp, present, exp-present, dup, s.rest, pct)
		} else {
			fmt.Printf("%-18s ids %d..%d expected=%d present=%d missing=%d duplicates=%d rest=%d completeness=%.4f%%\n", k, s.minID, s.maxID, exp, present, exp-present, dup, s.rest, pct)
		}
	}
	if totExp > 0 {
		pct := 100 * float64(totPresent) / float64(totExp)
		if md {
			fmt.Printf("| **total** | | | %d | %d | %d | %d | | **%.4f%%** |\n", totExp, totPresent, totExp-totPresent, totDup, pct)
		} else {
			fmt.Printf("TOTAL expected=%d present=%d missing=%d duplicates=%d completeness=%.4f%%\n", totExp, totPresent, totExp-totPresent, totDup, pct)
		}
	}
}

func window(d time.Duration) string {
	if d == 0 {
		return "all"
	}
	return d.String()
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "audit:", err)
	os.Exit(1)
}

// Package resume recovers the gap tracker's position after a restart by
// reading the tail of the trades topic. Without it, trades that happened
// while the ingestor was down are never detected as missing, and a venue's
// subscription snapshot is produced again as duplicates.
package resume

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sr"
	"google.golang.org/protobuf/proto"

	pb "tidemark/gen/go/tidemark/v1"
	"tidemark/ingest/venue"
)

// Position is the highest trade id seen for one venue:product key.
type Position struct {
	TradeID uint64
	EventTs time.Time
}

// LastSeen scans the last `tail` records of every partition of topic and
// returns the highest trade id per key. Backfilled trades can land after
// newer live ones, so it takes the max rather than the last record.
func LastSeen(ctx context.Context, brokers []string, topic string, tail int64) (map[string]Position, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	ends, err := adm.ListEndOffsets(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("end offsets: %w", err)
	}
	starts, err := adm.ListStartOffsets(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("start offsets: %w", err)
	}
	target := map[int32]int64{}
	assign := map[string]map[int32]kgo.Offset{topic: {}}
	ends.Each(func(o kadm.ListedOffset) {
		start, _ := starts.Lookup(topic, o.Partition)
		from := max(o.Offset-tail, start.Offset)
		if o.Offset > from {
			target[o.Partition] = o.Offset
			assign[topic][o.Partition] = kgo.NewOffset().At(from)
		}
	})
	out := map[string]Position{}
	if len(target) == 0 {
		return out, nil
	}
	cl.AddConsumePartitions(assign)
	var h sr.ConfluentHeader
	for len(target) > 0 {
		fs := cl.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var ferr error
		fs.EachError(func(_ string, p int32, err error) { ferr = fmt.Errorf("partition %d: %w", p, err) })
		if ferr != nil {
			return nil, ferr
		}
		fs.EachRecord(func(r *kgo.Record) {
			if r.Offset+1 >= target[r.Partition] {
				delete(target, r.Partition)
			}
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
			k := venue.Key(t.Venue, t.ProductId)
			if t.TradeId > out[k].TradeID {
				out[k] = Position{TradeID: t.TradeId, EventTs: time.Unix(0, t.EventTsNs)}
			}
		})
	}
	return out, nil
}

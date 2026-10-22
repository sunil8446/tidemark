//go:build integration

// Integration tests start a throwaway Redpanda container (Docker required):
//
//	go test -tags integration ./ingest/sink/
package sink

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sr"
	"google.golang.org/protobuf/proto"

	pb "tidemark/gen/go/tidemark/v1"
	"tidemark/ingest/metrics"
	"tidemark/schemas"
)

func startRedpanda(t *testing.T) (broker, registry string) {
	t.Helper()
	ctx := context.Background()
	c, err := redpanda.Run(ctx, "docker.redpanda.com/redpandadata/redpanda:v25.2.4")
	if err != nil {
		t.Fatalf("start redpanda: %v", err)
	}
	t.Cleanup(func() { c.Terminate(context.Background()) })
	if broker, err = c.KafkaSeedBroker(ctx); err != nil {
		t.Fatal(err)
	}
	if registry, err = c.SchemaRegistryAddress(ctx); err != nil {
		t.Fatal(err)
	}
	return broker, registry
}

func TestKafkaRoundTripAndSchemaRules(t *testing.T) {
	broker, registry := startRedpanda(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	topics := DefaultTopics(".it")
	admin, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kadm.NewClient(admin).CreateTopics(ctx, 3, 1, nil, topics.Trades, topics.Book, topics.Gaps); err != nil {
		t.Fatal(err)
	}
	admin.Close()
	k, err := NewKafka(ctx, KafkaConfig{Brokers: []string{broker}, RegistryURL: registry, Topics: topics, ClientID: "it"}, metrics.New())
	if err != nil {
		t.Fatal(err)
	}
	const n = 500
	for i := uint64(1); i <= n; i++ {
		k.Trade(&pb.Trade{Venue: pb.Venue_VENUE_KRAKEN, ProductId: "BTC-USD", TradeId: i, Price: "1.0", Size: "2.0"})
	}
	k.Book(&pb.BookUpdate{Venue: pb.Venue_VENUE_KRAKEN, ProductId: "BTC-USD", IsSnapshot: true, ChecksumOk: true})
	k.Gap(&pb.GapEvent{GapId: "g1", Venue: pb.Venue_VENUE_KRAKEN, ProductId: "BTC-USD", Status: pb.GapStatus_GAP_STATUS_RECOVERED})
	if err := k.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	k.Close()

	// Consume everything back and decode the Confluent framing.
	cl, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.ConsumeTopics(topics.Trades, topics.Book, topics.Gaps),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	var h sr.ConfluentHeader
	var trades []uint64
	var books, gapsSeen int
	for len(trades) < n || books < 1 || gapsSeen < 1 {
		fs := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("timed out: trades=%d books=%d gaps=%d", len(trades), books, gapsSeen)
		}
		fs.EachRecord(func(r *kgo.Record) {
			_, rest, err := h.DecodeID(r.Value)
			if err != nil {
				t.Fatal(err)
			}
			idx, body, err := h.DecodeIndex(rest, 8)
			if err != nil {
				t.Fatal(err)
			}
			switch r.Topic {
			case topics.Trades:
				if idx[0] != schemas.IndexTrade[0] || string(r.Key) != "kraken:BTC-USD" {
					t.Fatalf("trade index %v key %s", idx, r.Key)
				}
				var tr pb.Trade
				if err := proto.Unmarshal(body, &tr); err != nil {
					t.Fatal(err)
				}
				trades = append(trades, tr.TradeId)
			case topics.Book:
				if idx[0] != schemas.IndexBookUpdate[0] {
					t.Fatalf("book index %v", idx)
				}
				books++
			case topics.Gaps:
				var g pb.GapEvent
				if idx[0] != schemas.IndexGapEvent[0] || proto.Unmarshal(body, &g) != nil || g.GapId != "g1" {
					t.Fatalf("gap index %v", idx)
				}
				gapsSeen++
			}
		})
	}
	// One key -> one partition -> order preserved.
	for i, id := range trades {
		if id != uint64(i+1) {
			t.Fatalf("trade %d out of order: got id %d", i, id)
		}
	}

	// BACKWARD compatibility: adding a field passes, changing a field's type is rejected.
	rc, _ := sr.NewClient(sr.URLs(registry))
	subject := topics.Trades + "-value"
	added := strings.Replace(schemas.MarketProto, "  string ingest_id = 10;", "  string ingest_id = 10;\n  string venue_seq = 99;", 1)
	if _, err := rc.CreateSchema(ctx, subject, sr.Schema{Schema: added, Type: sr.TypeProtobuf}); err != nil {
		t.Fatalf("additive change rejected: %v", err)
	}
	broken := strings.Replace(schemas.MarketProto, "  uint64 trade_id = 3;", "  string trade_id = 3;", 1)
	if _, err := rc.CreateSchema(ctx, subject, sr.Schema{Schema: broken, Type: sr.TypeProtobuf}); err == nil {
		t.Fatal("type change on trade_id was accepted; expected BACKWARD violation")
	}
}

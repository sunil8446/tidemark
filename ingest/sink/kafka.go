package sink

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sr"
	"google.golang.org/protobuf/proto"

	pb "tidemark/gen/go/tidemark/v1"
	"tidemark/ingest/metrics"
	"tidemark/ingest/venue"
	"tidemark/schemas"
)

type Topics struct {
	Trades, Book, Gaps, Alerts, Labels string
}

func DefaultTopics(suffix string) Topics {
	return Topics{
		Trades: "trades.v1" + suffix, Book: "book.v1" + suffix, Gaps: "gaps.v1" + suffix,
		Alerts: "alerts.v1" + suffix, Labels: "labels.v1" + suffix,
	}
}

// Kafka produces Confluent-framed Protobuf records. The client is idempotent
// (franz-go's default) with acks=all, so broker retries cannot duplicate or
// reorder records within a partition for the life of this producer.
type Kafka struct {
	cl           *kgo.Client
	topics       Topics
	ids          map[string]int // topic -> schema id
	m            *metrics.Metrics
	header       sr.ConfluentHeader
	trackLatency bool
}

type KafkaConfig struct {
	Brokers     []string
	RegistryURL string
	Topics      Topics
	ClientID    string
	// TrackLatency observes exchange-to-ack latency for live trades.
	TrackLatency bool
}

func NewKafka(ctx context.Context, cfg KafkaConfig, m *metrics.Metrics) (*Kafka, error) {
	ids, err := RegisterSchemas(ctx, cfg.RegistryURL, cfg.Topics)
	if err != nil {
		return nil, err
	}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(cfg.ClientID),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchCompression(kgo.ZstdCompression(), kgo.Lz4Compression(), kgo.NoCompression()),
		kgo.ProducerLinger(5*time.Millisecond),
		kgo.MaxBufferedRecords(200_000),
		kgo.RecordDeliveryTimeout(2*time.Minute),
	)
	if err != nil {
		return nil, err
	}
	pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := cl.Ping(pctx); err != nil {
		cl.Close()
		return nil, fmt.Errorf("kafka ping: %w", err)
	}
	return &Kafka{cl: cl, topics: cfg.Topics, ids: ids, m: m, trackLatency: cfg.TrackLatency}, nil
}

// RegisterSchemas registers market.proto under each topic's value subject
// with BACKWARD compatibility and returns the schema id per topic.
func RegisterSchemas(ctx context.Context, url string, t Topics) (map[string]int, error) {
	cl, err := sr.NewClient(sr.URLs(url))
	if err != nil {
		return nil, err
	}
	ids := map[string]int{}
	for _, topic := range []string{t.Trades, t.Book, t.Gaps, t.Alerts, t.Labels} {
		subject := topic + "-value"
		for _, res := range cl.SetCompatibility(ctx, sr.SetCompatibility{Level: sr.CompatBackward}, subject) {
			if res.Err != nil {
				return nil, fmt.Errorf("set compatibility %s: %w", subject, res.Err)
			}
		}
		ss, err := cl.CreateSchema(ctx, subject, sr.Schema{Schema: schemas.MarketProto, Type: sr.TypeProtobuf})
		if err != nil {
			return nil, fmt.Errorf("register %s: %w", subject, err)
		}
		ids[topic] = ss.ID
		slog.Info("schema registered", "subject", subject, "id", ss.ID, "version", ss.Version)
	}
	return ids, nil
}

func (k *Kafka) produce(topic, key string, index []int, msg proto.Message, onAck func()) {
	b, err := k.header.AppendEncode(nil, k.ids[topic], index)
	if err == nil {
		b, err = proto.MarshalOptions{}.MarshalAppend(b, msg)
	}
	if err != nil {
		k.m.ProduceErrors.WithLabelValues(topic).Inc()
		slog.Error("encode failed", "topic", topic, "err", err)
		return
	}
	rec := &kgo.Record{Topic: topic, Key: []byte(key), Value: b}
	k.cl.Produce(context.Background(), rec, func(_ *kgo.Record, err error) {
		if err != nil {
			k.m.ProduceErrors.WithLabelValues(topic).Inc()
			slog.Error("produce failed", "topic", topic, "err", err)
			if t, ok := msg.(*pb.Trade); ok {
				// The tracker already counted this id as seen, so no gap will be reported for it.
				// Count it: the run's completeness must not look perfect while trades are lost.
				k.m.TradesLost.WithLabelValues(venue.Name(t.Venue), t.ProductId).Inc()
			}
			return
		}
		k.m.Produced.WithLabelValues(topic).Inc()
		if onAck != nil {
			onAck()
		}
	})
}

func (k *Kafka) Trade(t *pb.Trade) {
	var onAck func()
	if k.trackLatency && t.Source == pb.Source_SOURCE_WS && !t.FromSnapshot && t.EventTsNs > 0 {
		obs := k.m.ExchangeToKafka.WithLabelValues(venue.Name(t.Venue))
		onAck = func() { obs.Observe(time.Since(time.Unix(0, t.EventTsNs)).Seconds()) }
	}
	k.produce(k.topics.Trades, venue.Key(t.Venue, t.ProductId), schemas.IndexTrade, t, onAck)
}

func (k *Kafka) Book(b *pb.BookUpdate) {
	k.produce(k.topics.Book, venue.Key(b.Venue, b.ProductId), schemas.IndexBookUpdate, b, nil)
}

func (k *Kafka) Gap(g *pb.GapEvent) {
	k.produce(k.topics.Gaps, venue.Key(g.Venue, g.ProductId), schemas.IndexGapEvent, g, nil)
}

// Label records ground truth for one injected pattern (replay --inject).
func (k *Kafka) Label(l *pb.InjectionLabel) {
	k.produce(k.topics.Labels, venue.Key(l.Venue, l.ProductId), schemas.IndexLabel, l, nil)
}

func (k *Kafka) Flush(ctx context.Context) error { return k.cl.Flush(ctx) }
func (k *Kafka) Close()                          { k.cl.Close() }

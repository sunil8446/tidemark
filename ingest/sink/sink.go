// Package sink is where normalised events go: Kafka in production, memory in tests.
package sink

import (
	"context"
	"sync"

	pb "tidemark/gen/go/tidemark/v1"
)

type Sink interface {
	Trade(*pb.Trade)
	Book(*pb.BookUpdate)
	Gap(*pb.GapEvent)
	Flush(ctx context.Context) error
	Close()
}

// Memory collects everything; safe for concurrent use.
type Memory struct {
	mu     sync.Mutex
	Trades []*pb.Trade
	Books  []*pb.BookUpdate
	Gaps   []*pb.GapEvent
}

func (m *Memory) Trade(t *pb.Trade)           { m.mu.Lock(); m.Trades = append(m.Trades, t); m.mu.Unlock() }
func (m *Memory) Book(b *pb.BookUpdate)       { m.mu.Lock(); m.Books = append(m.Books, b); m.mu.Unlock() }
func (m *Memory) Gap(g *pb.GapEvent)          { m.mu.Lock(); m.Gaps = append(m.Gaps, g); m.mu.Unlock() }
func (m *Memory) Flush(context.Context) error { return nil }
func (m *Memory) Close()                      {}

// Snapshot returns copies of the collected slices.
func (m *Memory) Snapshot() (trades []*pb.Trade, books []*pb.BookUpdate, gaps []*pb.GapEvent) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]*pb.Trade(nil), m.Trades...), append([]*pb.BookUpdate(nil), m.Books...), append([]*pb.GapEvent(nil), m.Gaps...)
}

// Package venue defines what the pipeline needs from an exchange integration:
// a feed that turns raw WebSocket frames into normalised events, and a
// backfiller that fetches missing trades over REST.
package venue

import (
	"context"
	"strings"
	"time"

	pb "tidemark/gen/go/tidemark/v1"
)

// Heartbeat reports the venue's latest trade id for a product, so silent
// drops at the tail of the stream can be detected.
type Heartbeat struct {
	Product     string
	LastTradeID uint64
}

// BookMismatch means the local book no longer matches the venue checksum.
// Messages are the frames to send to get a fresh snapshot.
type BookMismatch struct {
	Product  string
	Expected uint32
	Computed uint32
	Messages [][]byte
}

// Event is one normalised item parsed from a frame. Exactly one field is set.
type Event struct {
	Trade        *pb.Trade
	Book         *pb.BookUpdate
	Heartbeat    *Heartbeat
	BookMismatch *BookMismatch
	BookResynced string // product whose fresh snapshot arrived after a mismatch
}

// Feed is a venue's WebSocket protocol. Parse is called from a single
// goroutine, so implementations may keep unsynchronised per-connection state.
type Feed interface {
	Venue() pb.Venue
	URL() string
	// SubscribeMessages returns the frames to send after every (re)connect.
	SubscribeMessages() [][]byte
	// Reset drops per-connection state, such as order books, before a new session.
	Reset()
	Parse(frame []byte, recv time.Time) ([]Event, error)
}

// Backfiller fetches trades for a product with first <= trade_id <= last.
// sourceMax is the highest trade_id the source returned or currently has,
// which lets the caller tell REST lag apart from ids that never existed.
type Backfiller interface {
	Fetch(ctx context.Context, product string, first, last uint64, after time.Time) (trades []*pb.Trade, sourceMax uint64, err error)
}

// Key is the Kafka record key: ordering holds per venue and product.
func Key(v pb.Venue, product string) string {
	return Name(v) + ":" + product
}

// Name is the short lowercase venue name used in keys and metric labels.
func Name(v pb.Venue) string {
	return strings.ToLower(strings.TrimPrefix(v.String(), "VENUE_"))
}

// Package coinbase implements the Coinbase Exchange matches and heartbeat
// channels, plus REST backfill. See docs/feeds.md.
package coinbase

import (
	"encoding/json"
	"fmt"
	"time"

	pb "tidemark/gen/go/tidemark/v1"
	"tidemark/ingest/venue"
)

const DefaultURL = "wss://ws-feed.exchange.coinbase.com"

type Feed struct {
	url      string
	products []string
}

func NewFeed(url string, products []string) *Feed {
	if url == "" {
		url = DefaultURL
	}
	return &Feed{url: url, products: products}
}

func (f *Feed) Venue() pb.Venue { return pb.Venue_VENUE_COINBASE }
func (f *Feed) URL() string     { return f.url }
func (f *Feed) Reset()          {}

func (f *Feed) SubscribeMessages() [][]byte {
	b, _ := json.Marshal(map[string]any{
		"type":        "subscribe",
		"product_ids": f.products,
		"channels":    []string{"matches", "heartbeat"},
	})
	return [][]byte{b}
}

type message struct {
	Type        string    `json:"type"`
	TradeID     uint64    `json:"trade_id"`
	LastTradeID uint64    `json:"last_trade_id"`
	ProductID   string    `json:"product_id"`
	Side        string    `json:"side"`
	Size        string    `json:"size"`
	Price       string    `json:"price"`
	Time        time.Time `json:"time"`
	Message     string    `json:"message"`
	Reason      string    `json:"reason"`
}

func (f *Feed) Parse(frame []byte, recv time.Time) ([]venue.Event, error) {
	var m message
	if err := json.Unmarshal(frame, &m); err != nil {
		return nil, fmt.Errorf("coinbase: decode: %w", err)
	}
	switch m.Type {
	case "match", "last_match":
		return []venue.Event{{Trade: &pb.Trade{
			Venue:        pb.Venue_VENUE_COINBASE,
			ProductId:    m.ProductID,
			TradeId:      m.TradeID,
			Price:        m.Price,
			Size:         m.Size,
			TakerSide:    takerSide(m.Side),
			EventTsNs:    m.Time.UnixNano(),
			RecvTsNs:     recv.UnixNano(),
			Source:       pb.Source_SOURCE_WS,
			FromSnapshot: m.Type == "last_match",
		}}}, nil
	case "heartbeat":
		return []venue.Event{{Heartbeat: &venue.Heartbeat{Product: m.ProductID, LastTradeID: m.LastTradeID}}}, nil
	case "subscriptions":
		return nil, nil
	case "error":
		return nil, fmt.Errorf("coinbase: error message: %s %s", m.Message, m.Reason)
	default:
		return nil, nil
	}
}

// takerSide converts Coinbase's maker side to the taker side.
func takerSide(maker string) pb.Side {
	switch maker {
	case "buy":
		return pb.Side_SIDE_SELL
	case "sell":
		return pb.Side_SIDE_BUY
	}
	return pb.Side_SIDE_UNSPECIFIED
}

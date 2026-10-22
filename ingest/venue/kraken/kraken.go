// Package kraken implements the Kraken WebSocket v2 trade, book and
// instrument channels, plus REST backfill. See docs/feeds.md.
package kraken

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	pb "tidemark/gen/go/tidemark/v1"
	"tidemark/ingest/venue"
)

const DefaultURL = "wss://ws.kraken.com/v2"

type precision struct{ price, qty int }

type Feed struct {
	url       string
	symbols   []string // Kraken form, e.g. "BTC/USD"
	depth     int      // 0 disables the book channel
	precision map[string]precision
	books     map[string]*Book
	resyncing map[string]bool
}

func NewFeed(url string, symbols []string, bookDepth int) *Feed {
	if url == "" {
		url = DefaultURL
	}
	return &Feed{url: url, symbols: symbols, depth: bookDepth, precision: map[string]precision{}}
}

func (f *Feed) Venue() pb.Venue { return pb.Venue_VENUE_KRAKEN }
func (f *Feed) URL() string     { return f.url }

func (f *Feed) Reset() {
	f.books = map[string]*Book{}
	f.resyncing = map[string]bool{}
}

func subscribe(method string, params map[string]any) []byte {
	b, _ := json.Marshal(map[string]any{"method": method, "params": params})
	return b
}

func (f *Feed) SubscribeMessages() [][]byte {
	msgs := [][]byte{
		subscribe("subscribe", map[string]any{"channel": "instrument", "snapshot": true}),
		// snapshot=true replays the last 50 trades, which also fills short gaps after a reconnect.
		subscribe("subscribe", map[string]any{"channel": "trade", "symbol": f.symbols, "snapshot": true}),
	}
	if f.depth > 0 {
		msgs = append(msgs, subscribe("subscribe", map[string]any{"channel": "book", "symbol": f.symbols, "depth": f.depth}))
	}
	return msgs
}

// Product converts "BTC/USD" to the normalised "BTC-USD".
func Product(symbol string) string { return strings.ReplaceAll(symbol, "/", "-") }

// Symbol converts "BTC-USD" to Kraken's "BTC/USD".
func Symbol(product string) string { return strings.ReplaceAll(product, "-", "/") }

type envelope struct {
	Channel string          `json:"channel"`
	Type    string          `json:"type"`
	Data    json.RawMessage `json:"data"`
	Method  string          `json:"method"`
	Success *bool           `json:"success"`
	Error   string          `json:"error"`
}

type wsTrade struct {
	Symbol    string      `json:"symbol"`
	Side      string      `json:"side"`
	Price     json.Number `json:"price"`
	Qty       json.Number `json:"qty"`
	TradeID   uint64      `json:"trade_id"`
	Timestamp time.Time   `json:"timestamp"`
}

type wsLevel struct {
	Price json.Number `json:"price"`
	Qty   json.Number `json:"qty"`
}

type wsBook struct {
	Symbol    string    `json:"symbol"`
	Bids      []wsLevel `json:"bids"`
	Asks      []wsLevel `json:"asks"`
	Checksum  *uint32   `json:"checksum"`
	Timestamp time.Time `json:"timestamp"`
}

type wsInstrument struct {
	Pairs []struct {
		Symbol         string `json:"symbol"`
		PricePrecision int    `json:"price_precision"`
		QtyPrecision   int    `json:"qty_precision"`
	} `json:"pairs"`
}

func decode(raw []byte, v any) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	return d.Decode(v)
}

func (f *Feed) Parse(frame []byte, recv time.Time) ([]venue.Event, error) {
	if f.books == nil {
		f.Reset()
	}
	var env envelope
	if err := decode(frame, &env); err != nil {
		return nil, fmt.Errorf("kraken: decode: %w", err)
	}
	if env.Method != "" {
		if env.Success != nil && !*env.Success {
			return nil, fmt.Errorf("kraken: %s failed: %s", env.Method, env.Error)
		}
		return nil, nil
	}
	switch env.Channel {
	case "trade":
		var rows []wsTrade
		if err := decode(env.Data, &rows); err != nil {
			return nil, fmt.Errorf("kraken: trade: %w", err)
		}
		out := make([]venue.Event, 0, len(rows))
		for _, r := range rows {
			out = append(out, venue.Event{Trade: &pb.Trade{
				Venue:        pb.Venue_VENUE_KRAKEN,
				ProductId:    Product(r.Symbol),
				TradeId:      r.TradeID,
				Price:        r.Price.String(),
				Size:         r.Qty.String(),
				TakerSide:    side(r.Side),
				EventTsNs:    r.Timestamp.UnixNano(),
				RecvTsNs:     recv.UnixNano(),
				Source:       pb.Source_SOURCE_WS,
				FromSnapshot: env.Type == "snapshot",
			}})
		}
		return out, nil
	case "book":
		var rows []wsBook
		if err := decode(env.Data, &rows); err != nil {
			return nil, fmt.Errorf("kraken: book: %w", err)
		}
		var out []venue.Event
		for _, r := range rows {
			evs, err := f.applyBook(r, env.Type == "snapshot", recv)
			if err != nil {
				return out, err
			}
			out = append(out, evs...)
		}
		return out, nil
	case "instrument":
		var inst wsInstrument
		if err := decode(env.Data, &inst); err != nil {
			return nil, fmt.Errorf("kraken: instrument: %w", err)
		}
		for _, p := range inst.Pairs {
			f.precision[p.Symbol] = precision{price: p.PricePrecision, qty: p.QtyPrecision}
		}
		return nil, nil
	default: // status, heartbeat
		return nil, nil
	}
}

func (f *Feed) applyBook(r wsBook, snapshot bool, recv time.Time) ([]venue.Event, error) {
	product := Product(r.Symbol)
	var out []venue.Event
	if snapshot {
		prec, ok := f.precision[r.Symbol]
		if !ok {
			prec = inferPrecision(r)
		}
		f.books[r.Symbol] = NewBook(prec.price, prec.qty, f.depth)
		if f.resyncing[r.Symbol] {
			delete(f.resyncing, r.Symbol)
			out = append(out, venue.Event{BookResynced: product})
		}
	} else if f.resyncing[r.Symbol] {
		return nil, nil // waiting for the fresh snapshot
	}
	b, ok := f.books[r.Symbol]
	if !ok {
		return nil, nil // update before any snapshot
	}
	if err := b.Apply(levels(r.Bids), true); err != nil {
		return out, f.mismatch(r.Symbol, 0, 0, &out, err)
	}
	if err := b.Apply(levels(r.Asks), false); err != nil {
		return out, f.mismatch(r.Symbol, 0, 0, &out, err)
	}
	b.Truncate()

	upd := &pb.BookUpdate{
		Venue:      pb.Venue_VENUE_KRAKEN,
		ProductId:  product,
		IsSnapshot: snapshot,
		Bids:       pbLevels(r.Bids),
		Asks:       pbLevels(r.Asks),
		EventTsNs:  r.Timestamp.UnixNano(),
		RecvTsNs:   recv.UnixNano(),
		Source:     pb.Source_SOURCE_WS,
	}
	if r.Checksum != nil {
		upd.Checksum = *r.Checksum
		computed := b.Checksum()
		upd.ChecksumOk = computed == *r.Checksum
		if !upd.ChecksumOk {
			out = append(out, venue.Event{Book: upd})
			f.mismatch(r.Symbol, *r.Checksum, computed, &out, nil)
			return out, nil
		}
	}
	out = append(out, venue.Event{Book: upd})
	return out, nil
}

// mismatch drops the local book and asks for a fresh snapshot by
// unsubscribing and resubscribing that symbol's book.
func (f *Feed) mismatch(symbol string, expected, computed uint32, out *[]venue.Event, cause error) error {
	delete(f.books, symbol)
	f.resyncing[symbol] = true
	*out = append(*out, venue.Event{BookMismatch: &venue.BookMismatch{
		Product:  Product(symbol),
		Expected: expected,
		Computed: computed,
		Messages: [][]byte{
			subscribe("unsubscribe", map[string]any{"channel": "book", "symbol": []string{symbol}, "depth": f.depth}),
			subscribe("subscribe", map[string]any{"channel": "book", "symbol": []string{symbol}, "depth": f.depth}),
		},
	}})
	if cause != nil {
		return fmt.Errorf("kraken: book %s: %w", symbol, cause)
	}
	return nil
}

func inferPrecision(r wsBook) precision {
	var p precision
	for _, l := range slices.Concat(r.Bids, r.Asks) {
		p.price = max(p.price, decimals(l.Price.String()))
		p.qty = max(p.qty, decimals(l.Qty.String()))
	}
	return p
}

func levels(ws []wsLevel) []Level {
	out := make([]Level, len(ws))
	for i, l := range ws {
		out[i] = Level{Price: l.Price.String(), Qty: l.Qty.String()}
	}
	return out
}

func pbLevels(ws []wsLevel) []*pb.BookLevel {
	out := make([]*pb.BookLevel, len(ws))
	for i, l := range ws {
		out[i] = &pb.BookLevel{Price: l.Price.String(), Qty: l.Qty.String()}
	}
	return out
}

// side maps Kraken's taker side ("buy"/"sell", or "b"/"s" over REST).
func side(s string) pb.Side {
	switch s {
	case "buy", "b":
		return pb.Side_SIDE_BUY
	case "sell", "s":
		return pb.Side_SIDE_SELL
	}
	return pb.Side_SIDE_UNSPECIFIED
}

package coinbase

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	pb "tidemark/gen/go/tidemark/v1"
)

const DefaultRESTURL = "https://api.exchange.coinbase.com"

// Backfiller pages GET /products/{id}/trades. Pages are newest first and the
// `after` cursor returns trades with a smaller trade_id, so a range is fetched
// by starting at last+1 and walking back until trade_id < first.
type Backfiller struct {
	BaseURL string
	Client  *http.Client
	limiter *time.Ticker // public REST limit is 10 req/s; stay well under it
}

func NewBackfiller(baseURL string) *Backfiller {
	if baseURL == "" {
		baseURL = DefaultRESTURL
	}
	return &Backfiller{
		BaseURL: baseURL,
		Client:  &http.Client{Timeout: 10 * time.Second},
		limiter: time.NewTicker(200 * time.Millisecond),
	}
}

type restTrade struct {
	TradeID uint64    `json:"trade_id"`
	Side    string    `json:"side"`
	Size    string    `json:"size"`
	Price   string    `json:"price"`
	Time    time.Time `json:"time"`
}

// maxPages bounds one Fetch call (1000 trades per page).
const maxPages = 200

func (b *Backfiller) Fetch(ctx context.Context, product string, first, last uint64, _ time.Time) ([]*pb.Trade, uint64, error) {
	latest, _, err := b.page(ctx, product, "limit=1")
	if err != nil {
		return nil, 0, err
	}
	var sourceMax uint64
	if len(latest) > 0 {
		sourceMax = latest[0].TradeID
	}

	var out []*pb.Trade
	cursor := strconv.FormatUint(last+1, 10)
	for range maxPages {
		rows, next, err := b.page(ctx, product, "limit=1000&after="+cursor)
		if err != nil {
			return out, sourceMax, err
		}
		if len(rows) == 0 {
			break
		}
		done := false
		for _, r := range rows {
			if r.TradeID > sourceMax {
				sourceMax = r.TradeID
			}
			if r.TradeID < first {
				done = true
				continue
			}
			if r.TradeID <= last {
				out = append(out, &pb.Trade{
					Venue:     pb.Venue_VENUE_COINBASE,
					ProductId: product,
					TradeId:   r.TradeID,
					Price:     r.Price,
					Size:      r.Size,
					TakerSide: takerSide(r.Side),
					EventTsNs: r.Time.UnixNano(),
					Source:    pb.Source_SOURCE_REST,
				})
			}
		}
		if done || next == "" || next == cursor {
			break
		}
		cursor = next
	}
	return out, sourceMax, nil
}

func (b *Backfiller) page(ctx context.Context, product, query string) ([]restTrade, string, error) {
	select {
	case <-ctx.Done():
		return nil, "", ctx.Err()
	case <-b.limiter.C:
	}
	url := fmt.Sprintf("%s/products/%s/trades?%s", b.BaseURL, product, query)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, "", err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := b.Client.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("coinbase rest: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, "", fmt.Errorf("coinbase rest: %s %s", url, resp.Status)
	}
	var rows []restTrade
	if err := json.NewDecoder(resp.Body).Decode(&rows); err != nil {
		return nil, "", fmt.Errorf("coinbase rest: decode: %w", err)
	}
	return rows, resp.Header.Get("CB-AFTER"), nil
}
